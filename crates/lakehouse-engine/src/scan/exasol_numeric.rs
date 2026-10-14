//! Exasol's numeric typing ahead of DataFusion's `TypeCoercion` (#475): DOUBLE wins over DECIMAL,
//! and exact `+`, `-`, `*` and `SUM` overflow at Exasol's 128 bits instead of wrapping.

use std::any::Any;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, Int64Array};
use arrow::compute::cast;
use arrow::compute::kernels::arity::{binary, try_binary, unary};
use arrow::datatypes::{DataType, Decimal128Type, Int64Type, UInt64Type};
use arrow::error::ArrowError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::expr::{Case, InList, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    Between, BinaryExpr, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

const MAX_PRECISION: u16 = 38;
const EXASOL_MAX_SCALE: i8 = 36;
/// `SUM` adds with wrapping, also for `Decimal128`; its `Decimal128(p + 10)` total cannot wrap up
/// to this argument precision.
const DECIMAL128_SUM_MAX_PRECISION: u8 = 28;

/// Recognised by type through [`find_numeric_overflow`] and [`session_numeric_overflow`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NumericOverflow(pub(crate) String);

impl fmt::Display for NumericOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "data exception - numeric value out of range: {}", self.0)
    }
}

impl Error for NumericOverflow {}

pub(crate) fn find_numeric_overflow(error: &DataFusionError) -> Option<&NumericOverflow> {
    let mut level: &(dyn Error + 'static) = error;
    loop {
        if let Some(found) = level.downcast_ref::<NumericOverflow>() {
            return Some(found);
        }
        level = level.source()?;
    }
}

/// The Parquet row filter flattens a predicate's error to text, so the kernels also record it.
pub(crate) fn session_numeric_overflow(session: &SessionContext) -> Option<NumericOverflow> {
    let registered = session.state().udf(ExactOp::Mul.name()).ok()?;
    let implementation: &dyn Any = registered.inner().as_ref();
    implementation
        .downcast_ref::<ExactArithmeticUdf>()?
        .raised
        .get()
        .cloned()
}

#[derive(Debug)]
pub(crate) struct ExasolNumericTyping {
    widen: Arc<ScalarUDF>,
    add: Arc<ScalarUDF>,
    sub: Arc<ScalarUDF>,
    mul: Arc<ScalarUDF>,
}

impl ExasolNumericTyping {
    pub(crate) fn new() -> Self {
        let raised = Arc::new(OnceLock::new());
        let udf = |op: ExactOp| {
            Arc::new(ScalarUDF::new_from_impl(ExactArithmeticUdf {
                op,
                signature: Signature::any(op.arity(), Volatility::Immutable),
                raised: raised.clone(),
            }))
        };
        Self {
            widen: udf(ExactOp::Widen),
            add: udf(ExactOp::Add),
            sub: udf(ExactOp::Sub),
            mul: udf(ExactOp::Mul),
        }
    }

    pub(crate) fn register_functions(&self, ctx: &SessionContext) {
        for udf in [&self.widen, &self.add, &self.sub, &self.mul] {
            ctx.register_udf(udf.as_ref().clone());
        }
    }

    fn call(&self, udf: &Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
        let args = args.into_iter().map(tighten_literal).collect();
        Expr::ScalarFunction(ScalarFunction::new_udf(udf.clone(), args))
    }

    fn rewrite_plan(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut schema = merge_schema(&plan.inputs());
        if let LogicalPlan::TableScan(scan) = &plan {
            schema.merge(&DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &scan.source.schema(),
            )?);
        }
        let preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = preserver.save(&expr);
            expr.transform_up(|e| self.rewrite_expr(e, &schema))
                .map(|t| t.update_data(|e| name.restore(e)))
        })?
        .map_data(|plan| plan.recompute_schema())
    }

    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        match expr {
            Expr::BinaryExpr(binary) => self.rewrite_binary(binary, schema),
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => unify_float(std::iter::once(*expr).chain(list).collect(), schema)?.map_data(
                |mut all| {
                    let list = all.split_off(1);
                    Ok(Expr::InList(InList::new(
                        Box::new(all.remove(0)),
                        list,
                        negated,
                    )))
                },
            ),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => unify_float(vec![*expr, *low, *high], schema)?.map_data(|all| {
                let [expr, low, high] = <[Expr; 3]>::try_from(all).expect("three operands");
                Ok(Expr::Between(Between::new(
                    Box::new(expr),
                    negated,
                    Box::new(low),
                    Box::new(high),
                )))
            }),
            Expr::Case(case) => rewrite_case(case, schema),
            Expr::ScalarFunction(ScalarFunction { func, args })
                if matches!(
                    func.name(),
                    "greatest" | "least" | "coalesce" | "nullif" | "nvl"
                ) =>
            {
                unify_float(args, schema)?
                    .map_data(|args| Ok(Expr::ScalarFunction(ScalarFunction::new_udf(func, args))))
            }
            Expr::Negative(inner) if inner.get_type(schema)?.is_integer() => Ok(Transformed::yes(
                Expr::Negative(Box::new(self.call(&self.widen, vec![*inner]))),
            )),
            Expr::AggregateFunction(mut agg)
                if agg.func.name() == "sum" && agg.params.args.len() == 1 =>
            {
                let arg = &mut agg.params.args[0];
                *arg = match arg.get_type(schema)? {
                    DataType::Decimal128(p, s) if p > DECIMAL128_SUM_MAX_PRECISION => {
                        arg.clone().cast_to(&DataType::Decimal256(p, s), schema)?
                    }
                    t if t.is_integer() => self.call(&self.widen, vec![arg.clone()]),
                    _ => return Ok(Transformed::no(Expr::AggregateFunction(agg))),
                };
                Ok(Transformed::yes(Expr::AggregateFunction(agg)))
            }
            other => Ok(Transformed::no(other)),
        }
    }

    fn rewrite_binary(
        &self,
        BinaryExpr { left, op, right }: BinaryExpr,
        schema: &DFSchema,
    ) -> Result<Transformed<Expr>> {
        let rebuild = |left, right| Expr::BinaryExpr(BinaryExpr::new(left, op, right));
        if matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
        ) {
            return unify_float(vec![*left, *right], schema)?.map_data(|mut both| {
                let right = both.pop().expect("two operands");
                Ok(rebuild(Box::new(both.remove(0)), Box::new(right)))
            });
        }
        let udf = match op {
            Operator::Plus => &self.add,
            Operator::Minus => &self.sub,
            Operator::Multiply => &self.mul,
            _ => return Ok(Transformed::no(rebuild(left, right))),
        };
        if is_exact(&left.get_type(schema)?) && is_exact(&right.get_type(schema)?) {
            return Ok(Transformed::yes(self.call(udf, vec![*left, *right])));
        }
        Ok(Transformed::no(rebuild(left, right)))
    }
}

impl AnalyzerRule for ExasolNumericTyping {
    fn name(&self) -> &str {
        "exasol_numeric_typing"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|p| self.rewrite_plan(p))?
            .data)
    }
}

/// The operand with its `WHEN` values, and the results, unify separately.
fn rewrite_case(
    Case {
        expr,
        when_then_expr,
        else_expr,
    }: Case,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let has_operand = expr.is_some();
    let has_else = else_expr.is_some();
    let (whens, thens): (Vec<Expr>, Vec<Expr>) =
        when_then_expr.into_iter().map(|(w, t)| (*w, *t)).unzip();
    let keys = unify_float(expr.map(|e| *e).into_iter().chain(whens).collect(), schema)?;
    let results = unify_float(
        thens.into_iter().chain(else_expr.map(|e| *e)).collect(),
        schema,
    )?;
    let transformed = keys.transformed || results.transformed;
    let (mut keys, mut results) = (keys.data, results.data);
    let else_expr = if has_else { results.pop() } else { None };
    let operand = has_operand.then(|| keys.remove(0));
    let when_then = keys
        .into_iter()
        .zip(results)
        .map(|(w, t)| (Box::new(w), Box::new(t)))
        .collect();
    Ok(Transformed::new_transformed(
        Expr::Case(Case::new(
            operand.map(Box::new),
            when_then,
            else_expr.map(Box::new),
        )),
        transformed,
    ))
}

/// Each operand is typed on its own: before coercion, a `CASE` reports only its first result type.
fn unify_float(exprs: Vec<Expr>, schema: &DFSchema) -> Result<Transformed<Vec<Expr>>> {
    let types = exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let is_decimal =
        |t: &DataType| matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..));
    if !(types.iter().any(DataType::is_floating) && types.iter().any(is_decimal)) {
        return Ok(Transformed::no(exprs));
    }
    let unified = exprs
        .into_iter()
        .zip(&types)
        .map(|(e, t)| match is_decimal(t) {
            true => e.cast_to(&DataType::Float64, schema),
            false => Ok(e),
        })
        .collect::<Result<_>>()?;
    Ok(Transformed::yes(unified))
}

fn is_exact(t: &DataType) -> bool {
    t.is_integer() || matches!(t, DataType::Decimal128(..))
}

/// Exasol types an integer literal by its digits (`L * 100` is `DECIMAL(22,0)`), which keeps
/// results on the unchecked path and in a `Decimal128` `SUM`.
fn tighten_literal(expr: Expr) -> Expr {
    let Expr::Literal(value, _) = &expr else {
        return expr;
    };
    if !value.data_type().is_integer() {
        return expr;
    }
    let Ok(ScalarValue::Decimal128(Some(v), ..)) = value.cast_to(&DataType::Decimal128(38, 0))
    else {
        return expr;
    };
    let digits = v.unsigned_abs().checked_ilog10().map_or(1, |d| d + 1) as u8;
    Expr::Literal(ScalarValue::Decimal128(Some(v), digits, 0), None)
}

fn integer_precision(t: &DataType) -> Option<u8> {
    Some(match t {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 => 19,
        DataType::UInt64 => 20,
        _ => return None,
    })
}

fn exact_ps(t: &DataType) -> Result<(u8, i8)> {
    match (t, integer_precision(t)) {
        (DataType::Decimal128(p, s), _) => Ok((*p, *s)),
        (_, Some(p)) => Ok((p, 0)),
        _ => plan_err!("exact arithmetic over {t}"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ExactOp {
    Widen,
    Add,
    Sub,
    Mul,
}

impl ExactOp {
    fn name(self) -> &'static str {
        match self {
            Self::Widen => "exa_widen",
            Self::Add => "exa_add",
            Self::Sub => "exa_sub",
            Self::Mul => "exa_mul",
        }
    }

    fn arity(self) -> usize {
        if self == Self::Widen { 1 } else { 2 }
    }

    /// Exasol's result type, before the 38-digit cap.
    fn result_type(self, (pa, sa): (u8, i8), (pb, sb): (u8, i8)) -> (u16, i8) {
        match self {
            Self::Widen => (pa.into(), sa),
            Self::Mul => (u16::from(pa) + u16::from(pb), sa + sb),
            Self::Add | Self::Sub => {
                let s = sa.max(sb);
                let integer_digits =
                    (i16::from(pa) - i16::from(sa)).max(i16::from(pb) - i16::from(sb));
                ((integer_digits + i16::from(s) + 1) as u16, s)
            }
        }
    }

    fn apply_wrapping(self, x: i128, y: i128, fx: i128, fy: i128) -> i128 {
        match self {
            Self::Add => x.wrapping_mul(fx).wrapping_add(y.wrapping_mul(fy)),
            Self::Sub => x.wrapping_mul(fx).wrapping_sub(y.wrapping_mul(fy)),
            Self::Mul | Self::Widen => x.wrapping_mul(y),
        }
    }

    fn apply_checked(self, x: i128, y: i128, fx: i128, fy: i128) -> Option<i128> {
        match self {
            Self::Add => x.checked_mul(fx)?.checked_add(y.checked_mul(fy)?),
            Self::Sub => x.checked_mul(fx)?.checked_sub(y.checked_mul(fy)?),
            Self::Mul | Self::Widen => x.checked_mul(y),
        }
    }
}

#[derive(Debug)]
struct ExactArithmeticUdf {
    op: ExactOp,
    signature: Signature,
    raised: Arc<OnceLock<NumericOverflow>>,
}

impl PartialEq for ExactArithmeticUdf {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
    }
}

impl Eq for ExactArithmeticUdf {}

impl Hash for ExactArithmeticUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.hash(state);
    }
}

impl ExactArithmeticUdf {
    fn combine(&self, args: &ScalarFunctionArgs) -> Result<Decimal128Array> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("{} takes two arguments", self.op.name());
        };
        let a = exact_ps(args.arg_fields[0].data_type())?;
        let b = exact_ps(args.arg_fields[1].data_type())?;
        let (fa, fb) = match self.op {
            ExactOp::Add | ExactOp::Sub => {
                let s = a.1.max(b.1);
                (10i128.pow((s - a.1) as u32), 10i128.pow((s - b.1) as u32))
            }
            _ => (1, 1),
        };
        let op = self.op;
        let rows = args.number_rows;
        if op.result_type(a, b).0 <= MAX_PRECISION {
            let f = move |x: i128, y: i128| op.apply_wrapping(x, y, fa, fb);
            return Ok(
                match (
                    int64_array(left)?,
                    scalar_i128(left),
                    int64_array(right)?,
                    scalar_i128(right),
                ) {
                    (Some(x), _, Some(y), _) => binary(&x, &y, |x, y| f(x.into(), y.into()))?,
                    (Some(x), _, _, Some(c)) => unary(&x, |x| f(x.into(), c)),
                    (_, Some(c), Some(y), _) => unary(&y, |y| f(c, y.into())),
                    _ => binary(&as_i128(left, rows)?, &as_i128(right, rows)?, f)?,
                },
            );
        }
        try_binary(&as_i128(left, rows)?, &as_i128(right, rows)?, |x, y| {
            op.apply_checked(x, y, fa, fb)
                .ok_or(ArrowError::ArithmeticOverflow(format!(
                    "{} exceeds the 128-bit internal number representation",
                    op.name()
                )))
        })
        .map_err(|e| match e {
            ArrowError::ArithmeticOverflow(detail) => {
                let overflow = NumericOverflow(detail);
                let _ = self.raised.set(overflow.clone());
                DataFusionError::External(Box::new(overflow))
            }
            other => other.into(),
        })
    }
}

impl ScalarUDFImpl for ExactArithmeticUdf {
    fn name(&self) -> &str {
        self.op.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let a = exact_ps(&args[0])?;
        let b = match args.get(1) {
            Some(t) => exact_ps(t)?,
            None => a,
        };
        let (p, s) = self.op.result_type(a, b);
        if s > EXASOL_MAX_SCALE {
            return plan_err!("data exception - An exact numeric operation needs a too big scale");
        }
        Ok(DataType::Decimal128(p.min(MAX_PRECISION) as u8, s))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::Decimal128(p, s) = *args.return_field.data_type() else {
            return exec_err!("{} must return Decimal128", self.op.name());
        };
        let values = match self.op {
            ExactOp::Widen => as_i128(&args.args[0], args.number_rows)?,
            _ => self.combine(&args)?,
        };
        Ok(ColumnarValue::Array(Arc::new(
            values.with_precision_and_scale(p, s)?,
        )))
    }
}

fn int64_array(value: &ColumnarValue) -> Result<Option<Int64Array>> {
    Ok(match value {
        ColumnarValue::Array(a)
            if matches!(
                a.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) =>
        {
            Some(
                cast(a, &DataType::Int64)?
                    .as_primitive::<Int64Type>()
                    .clone(),
            )
        }
        _ => None,
    })
}

fn scalar_i128(value: &ColumnarValue) -> Option<i128> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Decimal128(Some(v), ..)) => Some(*v),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Some((*v).into()),
        _ => None,
    }
}

fn as_i128(value: &ColumnarValue, rows: usize) -> Result<Decimal128Array> {
    let array: ArrayRef = value.to_array(rows)?;
    Ok(match array.data_type() {
        DataType::Decimal128(..) => array.as_primitive::<Decimal128Type>().clone(),
        DataType::UInt64 => unary(array.as_primitive::<UInt64Type>(), i128::from),
        _ => unary(
            cast(&array, &DataType::Int64)?.as_primitive::<Int64Type>(),
            i128::from,
        ),
    })
}

#[cfg(test)]
#[path = "exasol_numeric_tests.rs"]
mod tests;
