//! The one table of translated scalar functions: how each renders for Exasol and for the scan,
//! how the scan's typed pass types it, and how many arguments a call carries. The scan declines a
//! name absent here. The aggregates and their argument counts are listed here too.

use std::ops::RangeInclusive;

use crate::CHECKED_INT_DIV_FN;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExasolForm {
    /// `<NAME>(<rendered args>)` with no arity check: Exasol emitted the call, so
    /// re-emitting it verbatim cannot be wrong.
    VerbatimCall,
    /// The Exasol form has its own arm: it is not a plain call (operator, `CASE`, CAST), or it
    /// is arity-checked (`MOD`).
    Shaped,
}

/// Which arguments of a function Exasol converts to text (#210).
#[derive(Clone, Copy, Debug)]
pub(crate) enum TextArgs {
    Never,
    All,
    First,
    FirstTwo,
    /// The subject, and the fill of the three-argument form.
    Pad,
}

/// A length, count, start, or code argument, which Exasol rounds to an integer (#604).
#[derive(Clone, Copy, Debug)]
pub(crate) struct IntegerArg {
    pub(crate) position: usize,
    /// Exasol returns NULL for a negative value, which DataFusion does not.
    pub(crate) negative_is_null: bool,
}

/// How a function's result type follows from its arguments' types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultRule {
    Text,
    Integer,
    /// `DECIMAL(precision,0)`, the type Exasol 8 and 2025 give a date part.
    FixedInteger(u8),
    /// A number of unproven scale.
    Number,
    /// An integer over integers, a number over numbers.
    Arithmetic,
    /// A number over numbers, never an integer: `SQRT(2)` is not one.
    NumberOverNumbers,
    /// An integer, and the scan computes the call only over integer operands.
    IntegerOverIntegers,
    FirstArgument,
    /// Exasol brings every argument to one type, as for CASE branches.
    Unified,
    /// `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, `ADD_MINUTES`: over a TIMESTAMP the result keeps
    /// its precision, over a DATE it is as given.
    DateAddition(DateResult),
    /// `DATE_TRUNC`: the type of the truncated DATE or TIMESTAMP.
    Truncated,
    /// `TO_DATE`: a DATE in every Exasol version.
    Date,
    /// A TIMESTAMP whose precision differs by Exasol version.
    Timestamp,
    Other,
}

/// What a date addition returns over a DATE argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DateResult {
    /// `ADD_DAYS`, `ADD_WEEKS`.
    Date,
    /// `ADD_HOURS`, `ADD_MINUTES`: Exasol returns `TIMESTAMP(0)`.
    Timestamp,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarFunction {
    pub(crate) name: &'static str,
    pub(crate) exasol: ExasolForm,
    pub(crate) text_args: TextArgs,
    /// Text arguments that are parameters, not values: a format model or a unit.
    pub(crate) parameters: &'static [usize],
    pub(crate) result: ResultRule,
    /// The DataFusion function of a same-arity call the scan's generic arm emits.
    pub(crate) datafusion_call: Option<&'static str>,
    /// The fewest and the most arguments an Exasol call of the function carries.
    min_arguments: usize,
    max_arguments: usize,
}

impl ScalarFunction {
    const fn new(name: &'static str, exasol: ExasolForm, arguments: Arguments) -> Self {
        Self {
            name,
            exasol,
            text_args: TextArgs::Never,
            parameters: &[],
            result: ResultRule::Other,
            datafusion_call: None,
            min_arguments: *arguments.start(),
            max_arguments: *arguments.end(),
        }
    }

    const fn shaped(name: &'static str, arguments: Arguments) -> Self {
        Self::new(name, ExasolForm::Shaped, arguments)
    }

    const fn verbatim(name: &'static str, arguments: Arguments) -> Self {
        Self::new(name, ExasolForm::VerbatimCall, arguments)
    }

    const fn typed(self, text_args: TextArgs, result: ResultRule) -> Self {
        Self {
            text_args,
            result,
            ..self
        }
    }

    const fn parameters(self, parameters: &'static [usize]) -> Self {
        Self { parameters, ..self }
    }

    const fn datafusion_call(self, datafusion_name: &'static str) -> Self {
        Self {
            datafusion_call: Some(datafusion_name),
            ..self
        }
    }
}

/// The argument counts of an Exasol call.
pub(crate) type Arguments = RangeInclusive<usize>;

/// The upper bound of a function that takes any number of arguments.
pub(crate) const MANY: usize = usize::MAX;

/// A `function_scalar` named `CAST` parses as a CAST node, which checks its own argument.
const ANY: Arguments = 0..=MANY;

/// The clock functions parse, but are not declared in [`SCALAR_FUNCTIONS`]: the adapter folds them
/// into literals before any translator sees them. Exasol 8 sends a TIMESTAMP clock without the
/// precision argument that Exasol 2025 always sends.
const CLOCK_FUNCTIONS: &[(&str, Arguments)] = &[
    ("CURRENT_DATE", 0..=0),
    ("SYSDATE", 0..=0),
    ("CURRENT_TIMESTAMP", 0..=1),
    ("LOCALTIMESTAMP", 0..=1),
    ("SYSTIMESTAMP", 0..=1),
];

/// The aggregates the adapter advertises. Exasol sends `COUNT(*)` with no arguments and refuses a
/// `COUNT` of two.
const AGGREGATE_FUNCTIONS: &[(&str, Arguments)] = &[
    ("COUNT", 0..=1),
    ("SUM", 1..=1),
    ("MIN", 1..=1),
    ("MAX", 1..=1),
    ("AVG", 1..=1),
    ("STDDEV", 1..=1),
    ("STDDEV_POP", 1..=1),
    ("STDDEV_SAMP", 1..=1),
    ("VARIANCE", 1..=1),
    ("VAR_POP", 1..=1),
    ("VAR_SAMP", 1..=1),
];

/// How the typed pass treats a name the table does not hold, which the renderer declines anyway.
pub(crate) const UNLISTED: ScalarFunction = ScalarFunction::shaped("", ANY);

pub(crate) const SCALAR_FUNCTIONS: &[ScalarFunction] = {
    use ResultRule as R;
    use ScalarFunction as F;
    use TextArgs as T;
    &[
        F::shaped("ADD", 2..=2).typed(T::Never, R::Arithmetic),
        F::shaped("SUB", 2..=2).typed(T::Never, R::Arithmetic),
        F::shaped("MULT", 2..=2).typed(T::Never, R::Arithmetic),
        F::shaped("FLOAT_DIV", 2..=2).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("DIV", 2..=2)
            .typed(T::Never, R::IntegerOverIntegers)
            .datafusion_call(CHECKED_INT_DIV_FN),
        F::shaped("NEG", 1..=1).typed(T::Never, R::Arithmetic),
        F::shaped("CAST", ANY),
        // MOD: Exasol requires `MOD(a, b)`, DataFusion the `%` operator.
        F::shaped("MOD", 2..=2).typed(T::Never, R::Arithmetic),
        // CONCAT: Exasol's `||` treats NULL as '' and yields NULL only when all-empty, as
        // DataFusion's `concat` does once the typed pass reads its empty result as NULL.
        F::shaped("CONCAT", 1..=MANY).typed(T::All, R::Text),
        F::shaped("CASE", 2..=MANY),
        // VerbatimCall: applied to every remaining name uniformly, not only where the
        // DataFusion form fails on Exasol, as `signum` does.
        F::verbatim("ABS", 1..=1).typed(T::Never, R::Arithmetic),
        F::verbatim("FLOOR", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("CEIL", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("SQRT", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("EXP", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("LN", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("SIGN", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("DEGREES", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("RADIANS", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("SIN", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("COS", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("TAN", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("ASIN", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("ACOS", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("ATAN", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("SINH", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("COSH", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("TANH", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("COT", 1..=1).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("ROUND", 1..=2).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("TRUNC", 1..=2).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("LOG", 2..=2).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("POWER", 2..=2).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("ATAN2", 2..=2).typed(T::Never, R::NumberOverNumbers),
        F::verbatim("LOWER", 1..=1)
            .typed(T::First, R::Text)
            .datafusion_call("lower"),
        F::verbatim("UPPER", 1..=1)
            .typed(T::First, R::Text)
            .datafusion_call("upper"),
        F::verbatim("SUBSTR", 2..=3)
            .typed(T::First, R::Text)
            .datafusion_call("substr"),
        F::verbatim("TRIM", 1..=2)
            .typed(T::All, R::Text)
            .datafusion_call("trim"),
        F::verbatim("LTRIM", 1..=2)
            .typed(T::All, R::Text)
            .datafusion_call("ltrim"),
        F::verbatim("RTRIM", 1..=2)
            .typed(T::All, R::Text)
            .datafusion_call("rtrim"),
        F::verbatim("REPLACE", 2..=3).typed(T::All, R::Text),
        F::verbatim("REPEAT", 2..=2)
            .typed(T::First, R::Text)
            .datafusion_call("repeat"),
        F::verbatim("REVERSE", 1..=1)
            .typed(T::First, R::Text)
            .datafusion_call("reverse"),
        F::verbatim("LPAD", 2..=3)
            .typed(T::Pad, R::Text)
            .datafusion_call("lpad"),
        F::verbatim("RPAD", 2..=3)
            .typed(T::Pad, R::Text)
            .datafusion_call("rpad"),
        F::verbatim("ASCII", 1..=1)
            .typed(T::First, R::Integer)
            .datafusion_call("ascii"),
        F::verbatim("CHR", 1..=1)
            .typed(T::Never, R::Text)
            .datafusion_call("chr"),
        F::verbatim("INITCAP", 1..=1)
            .typed(T::First, R::Text)
            .datafusion_call("initcap"),
        F::verbatim("LEFT", 2..=2)
            .typed(T::First, R::Text)
            .datafusion_call("left"),
        F::verbatim("RIGHT", 2..=2)
            .typed(T::First, R::Text)
            .datafusion_call("right"),
        F::verbatim("TRANSLATE", 3..=3)
            .typed(T::All, R::Text)
            .datafusion_call("translate"),
        F::verbatim("LENGTH", 1..=1)
            .typed(T::First, R::Integer)
            .datafusion_call("character_length"),
        F::verbatim("OCTET_LENGTH", 1..=1)
            .typed(T::First, R::Integer)
            .datafusion_call("octet_length"),
        F::verbatim("UNICODE", 1..=1)
            .typed(T::First, R::Integer)
            .datafusion_call("ascii"),
        F::verbatim("UNICODECHR", 1..=1)
            .typed(T::Never, R::Text)
            .datafusion_call("chr"),
        // Exasol's INSTR/LOCATE accept the optional start argument natively.
        F::verbatim("INSTR", 2..=4).typed(T::FirstTwo, R::Integer),
        F::verbatim("LOCATE", 2..=3).typed(T::FirstTwo, R::Integer),
        F::verbatim("GREATEST", 1..=MANY).typed(T::Never, R::Unified),
        F::verbatim("LEAST", 1..=MANY).typed(T::Never, R::Unified),
        F::verbatim("NULLIF", 2..=2).typed(T::Never, R::Unified),
        F::verbatim("NULLIFZERO", 1..=1).typed(T::Never, R::FirstArgument),
        F::verbatim("ZEROIFNULL", 1..=1).typed(T::Never, R::FirstArgument),
        // DataFusion renders these as `date_part`, which Exasol lacks.
        F::verbatim("YEAR", 1..=1).typed(T::Never, R::FixedInteger(4)),
        F::verbatim("MONTH", 1..=1).typed(T::Never, R::FixedInteger(2)),
        F::verbatim("DAY", 1..=1).typed(T::Never, R::FixedInteger(2)),
        F::verbatim("HOUR", 1..=1).typed(T::Never, R::FixedInteger(2)),
        F::verbatim("MINUTE", 1..=1).typed(T::Never, R::FixedInteger(2)),
        F::verbatim("SECOND", 1..=2).typed(T::Never, R::Number),
        F::verbatim("WEEK", 1..=1).typed(T::Never, R::FixedInteger(2)),
        F::verbatim("DATE_TRUNC", 2..=2)
            .typed(T::Never, R::Truncated)
            .parameters(&[0]),
        F::verbatim("TO_DATE", 1..=2)
            .typed(T::First, R::Date)
            .parameters(&[1]),
        F::verbatim("TO_TIMESTAMP", 1..=2)
            .typed(T::First, R::Timestamp)
            .parameters(&[1]),
        // DataFusion emulates these via `date_part`, which Exasol lacks.
        F::verbatim("DAYS_BETWEEN", 2..=2).typed(T::Never, R::Number),
        F::verbatim("HOURS_BETWEEN", 2..=2).typed(T::Never, R::Number),
        F::verbatim("MINUTES_BETWEEN", 2..=2).typed(T::Never, R::Number),
        F::verbatim("SECONDS_BETWEEN", 2..=2).typed(T::Never, R::Number),
        F::verbatim("ADD_DAYS", 2..=2).typed(T::Never, R::DateAddition(DateResult::Date)),
        F::verbatim("ADD_WEEKS", 2..=2).typed(T::Never, R::DateAddition(DateResult::Date)),
        F::verbatim("ADD_HOURS", 2..=2).typed(T::Never, R::DateAddition(DateResult::Timestamp)),
        F::verbatim("ADD_MINUTES", 2..=2).typed(T::Never, R::DateAddition(DateResult::Timestamp)),
        // Not declared: the clock functions reach the translator only as the literals the
        // adapter folds them into, so a missed call is refused, never computed per shard.
    ]
};

impl ScalarFunction {
    pub(crate) fn integer_args(self) -> &'static [IntegerArg] {
        const fn arg(position: usize, negative_is_null: bool) -> IntegerArg {
            IntegerArg {
                position,
                negative_is_null,
            }
        }
        const LENGTH: &[IntegerArg] = &[arg(1, true)];
        const SUBSTR: &[IntegerArg] = &[arg(1, false), arg(2, true)];
        const COUNT: &[IntegerArg] = &[arg(1, false)];
        const CODE: &[IntegerArg] = &[arg(0, false)];
        match self.name {
            "LPAD" | "RPAD" | "LEFT" | "RIGHT" => LENGTH,
            "SUBSTR" => SUBSTR,
            "REPEAT" => COUNT,
            "CHR" | "UNICODECHR" => CODE,
            _ => &[],
        }
    }
}

pub(crate) fn scalar_function(name: &str) -> Option<ScalarFunction> {
    SCALAR_FUNCTIONS
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(name))
        .copied()
}

/// The argument counts a `function_scalar` call of `name` takes, `None` for a name the adapter
/// does not translate.
pub(crate) fn scalar_arguments(name: &str) -> Option<Arguments> {
    match scalar_function(name) {
        Some(function) => Some(function.min_arguments..=function.max_arguments),
        None => arguments_of(CLOCK_FUNCTIONS, name),
    }
}

/// The argument counts a `function_aggregate` call of `name` takes, `None` for a name the adapter
/// does not translate.
pub(crate) fn aggregate_arguments(name: &str) -> Option<Arguments> {
    arguments_of(AGGREGATE_FUNCTIONS, name)
}

fn arguments_of(table: &[(&str, Arguments)], name: &str) -> Option<Arguments> {
    table
        .iter()
        .find(|(declared, _)| declared.eq_ignore_ascii_case(name))
        .map(|(_, arguments)| arguments.clone())
}

#[cfg(test)]
#[path = "scalar_functions_tests.rs"]
mod tests;
