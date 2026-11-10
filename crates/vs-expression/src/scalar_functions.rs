//! The one table of translated scalar functions: how the renderer emits each in both dialects, and
//! how the scan's typed pass types it. A name absent here is declined in both dialects.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExasolForm {
    /// `<NAME>(<rendered args>)` with no arity check: Exasol emitted the call, so
    /// re-emitting it verbatim cannot be wrong.
    VerbatimCall,
    /// The per-name arm owns both dialects: the Exasol form is not a plain call
    /// (operator, `CASE`, per-dialect CAST target), or the DataFusion side is not
    /// (`MOD` renders as `%`).
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

/// How a function's result class follows from its arguments' classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultRule {
    Text,
    Integer,
    /// An integer when every argument is an integer or NULL.
    IntegerOverIntegers,
    FirstArgument,
    /// Exasol brings every argument to one type, as for CASE branches.
    Unified,
    Other,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarFunction {
    pub(crate) name: &'static str,
    pub(crate) exasol: ExasolForm,
    pub(crate) text_args: TextArgs,
    /// Text arguments that are parameters, not values: a format model or a unit.
    pub(crate) parameters: &'static [usize],
    pub(crate) result: ResultRule,
    /// The DataFusion function of a same-arity string call the renderer's generic arm emits.
    pub(crate) string_call: Option<&'static str>,
}

impl ScalarFunction {
    const fn new(name: &'static str, exasol: ExasolForm) -> Self {
        Self {
            name,
            exasol,
            text_args: TextArgs::Never,
            parameters: &[],
            result: ResultRule::Other,
            string_call: None,
        }
    }

    const fn shaped(name: &'static str) -> Self {
        Self::new(name, ExasolForm::Shaped)
    }

    const fn verbatim(name: &'static str) -> Self {
        Self::new(name, ExasolForm::VerbatimCall)
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

    const fn string_call(self, datafusion_name: &'static str) -> Self {
        Self {
            string_call: Some(datafusion_name),
            ..self
        }
    }
}

/// How the typed pass treats a name the table does not hold, which the renderer declines anyway.
pub(crate) const UNLISTED: ScalarFunction = ScalarFunction::shaped("");

pub(crate) const SCALAR_FUNCTIONS: &[ScalarFunction] = {
    use ResultRule as R;
    use ScalarFunction as F;
    use TextArgs as T;
    &[
        F::shaped("ADD").typed(T::Never, R::IntegerOverIntegers),
        F::shaped("SUB").typed(T::Never, R::IntegerOverIntegers),
        F::shaped("MULT").typed(T::Never, R::IntegerOverIntegers),
        F::shaped("FLOAT_DIV"),
        F::shaped("NEG").typed(T::Never, R::IntegerOverIntegers),
        F::shaped("CAST"),
        // MOD: Exasol requires `MOD(a, b)`, DataFusion the `%` operator.
        F::shaped("MOD").typed(T::Never, R::IntegerOverIntegers),
        // CONCAT: Exasol's `||` treats NULL as '' and yields NULL only when all-empty;
        // DataFusion reproduces that as `nullif(concat(...), '')`.
        F::shaped("CONCAT").typed(T::All, R::Text),
        F::shaped("CASE"),
        // VerbatimCall: applied to every remaining name uniformly, not only where the
        // DataFusion rendering fails on Exasol. DataFusion's `signum` does not exist in
        // Exasol, and the gate precedes the whole name match, because the math arm matches SIGN.
        F::verbatim("ABS").typed(T::Never, R::IntegerOverIntegers),
        F::verbatim("FLOOR"),
        F::verbatim("CEIL"),
        F::verbatim("SQRT"),
        F::verbatim("EXP"),
        F::verbatim("LN"),
        F::verbatim("SIGN"),
        F::verbatim("DEGREES"),
        F::verbatim("RADIANS"),
        F::verbatim("SIN"),
        F::verbatim("COS"),
        F::verbatim("TAN"),
        F::verbatim("ASIN"),
        F::verbatim("ACOS"),
        F::verbatim("ATAN"),
        F::verbatim("SINH"),
        F::verbatim("COSH"),
        F::verbatim("TANH"),
        F::verbatim("COT"),
        F::verbatim("ROUND"),
        F::verbatim("TRUNC"),
        F::verbatim("LOG"),
        F::verbatim("POWER"),
        F::verbatim("ATAN2"),
        F::verbatim("LOWER")
            .typed(T::First, R::Text)
            .string_call("lower"),
        F::verbatim("UPPER")
            .typed(T::First, R::Text)
            .string_call("upper"),
        F::verbatim("SUBSTR")
            .typed(T::First, R::Text)
            .string_call("substr"),
        F::verbatim("TRIM")
            .typed(T::All, R::Text)
            .string_call("trim"),
        F::verbatim("LTRIM")
            .typed(T::All, R::Text)
            .string_call("ltrim"),
        F::verbatim("RTRIM")
            .typed(T::All, R::Text)
            .string_call("rtrim"),
        F::verbatim("REPLACE")
            .typed(T::All, R::Text)
            .string_call("replace"),
        F::verbatim("REPEAT")
            .typed(T::First, R::Text)
            .string_call("repeat"),
        F::verbatim("REVERSE")
            .typed(T::First, R::Text)
            .string_call("reverse"),
        F::verbatim("LPAD")
            .typed(T::Pad, R::Text)
            .string_call("lpad"),
        F::verbatim("RPAD")
            .typed(T::Pad, R::Text)
            .string_call("rpad"),
        F::verbatim("ASCII")
            .typed(T::First, R::Integer)
            .string_call("ascii"),
        F::verbatim("CHR")
            .typed(T::Never, R::Text)
            .string_call("chr"),
        F::verbatim("INITCAP")
            .typed(T::First, R::Text)
            .string_call("initcap"),
        F::verbatim("LEFT")
            .typed(T::First, R::Text)
            .string_call("left"),
        F::verbatim("RIGHT")
            .typed(T::First, R::Text)
            .string_call("right"),
        F::verbatim("TRANSLATE")
            .typed(T::All, R::Text)
            .string_call("translate"),
        F::verbatim("LENGTH")
            .typed(T::First, R::Integer)
            .string_call("character_length"),
        F::verbatim("OCTET_LENGTH")
            .typed(T::First, R::Integer)
            .string_call("octet_length"),
        F::verbatim("UNICODE")
            .typed(T::First, R::Integer)
            .string_call("ascii"),
        F::verbatim("UNICODECHR")
            .typed(T::Never, R::Text)
            .string_call("chr"),
        // Exasol's INSTR/LOCATE accept the optional start argument natively.
        F::verbatim("INSTR").typed(T::FirstTwo, R::Integer),
        F::verbatim("LOCATE").typed(T::FirstTwo, R::Integer),
        F::verbatim("GREATEST").typed(T::Never, R::Unified),
        F::verbatim("LEAST").typed(T::Never, R::Unified),
        F::verbatim("NULLIF").typed(T::Never, R::Unified),
        F::verbatim("NULLIFZERO").typed(T::Never, R::FirstArgument),
        F::verbatim("ZEROIFNULL").typed(T::Never, R::FirstArgument),
        // DataFusion renders these as `date_part`, which Exasol lacks.
        F::verbatim("YEAR").typed(T::Never, R::Integer),
        F::verbatim("MONTH").typed(T::Never, R::Integer),
        F::verbatim("DAY").typed(T::Never, R::Integer),
        F::verbatim("HOUR").typed(T::Never, R::Integer),
        F::verbatim("MINUTE").typed(T::Never, R::Integer),
        F::verbatim("SECOND"),
        F::verbatim("WEEK").typed(T::Never, R::Integer),
        F::verbatim("DATE_TRUNC").parameters(&[0]),
        F::verbatim("TO_DATE")
            .typed(T::First, R::Other)
            .parameters(&[1]),
        F::verbatim("TO_TIMESTAMP")
            .typed(T::First, R::Other)
            .parameters(&[1]),
        // DataFusion emulates these via `date_part`, which Exasol lacks.
        F::verbatim("DAYS_BETWEEN"),
        F::verbatim("HOURS_BETWEEN"),
        F::verbatim("MINUTES_BETWEEN"),
        F::verbatim("SECONDS_BETWEEN"),
        // Not declared: CURRENT_DATE, SYSDATE, CURRENT_TIMESTAMP, SYSTIMESTAMP. The scan
        // UDF has no session time zone, statement anchor, or connect-back.
    ]
};

pub(crate) fn scalar_function(name: &str) -> Option<ScalarFunction> {
    SCALAR_FUNCTIONS
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(name))
        .copied()
}
