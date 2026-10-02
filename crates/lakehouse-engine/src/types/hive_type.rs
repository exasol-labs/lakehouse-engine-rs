use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};

/// Into the Spark type the one Spark-type classifier types (`vs-adapter/glue-hive-type-mapping`);
/// every nested member is nullable.
pub(crate) fn parse_hive_type(hive_type: &str) -> Result<DataType, String> {
    let mut cursor = Cursor {
        rest: hive_type,
        depth: 0,
    };
    let parsed = cursor.data_type().and_then(|data_type| {
        cursor.end()?;
        Ok(data_type)
    });
    parsed.map_err(|problem| format!("Hive type '{hive_type}' {problem}"))
}

/// Hive's default for a bare `decimal`.
const DEFAULT_DECIMAL: (u8, u8) = (10, 0);

/// Bounds the recursion, so a hostile type string refuses its column instead of
/// overflowing the adapter VM's stack; serde_json applies the same default limit.
const MAX_NESTING_DEPTH: usize = 128;

struct Cursor<'a> {
    rest: &'a str,
    depth: usize,
}

impl<'a> Cursor<'a> {
    fn data_type(&mut self) -> Result<DataType, String> {
        let word = self.word();
        match word.to_ascii_lowercase().as_str() {
            "tinyint" => Ok(DataType::BYTE),
            "smallint" => Ok(DataType::SHORT),
            "int" | "integer" => Ok(DataType::INTEGER),
            "bigint" => Ok(DataType::LONG),
            "float" => Ok(DataType::FLOAT),
            "double" => Ok(DataType::DOUBLE),
            "boolean" => Ok(DataType::BOOLEAN),
            "string" => Ok(DataType::STRING),
            "varchar" | "char" => self.length().map(|_| DataType::STRING),
            "date" => Ok(DataType::DATE),
            "timestamp" => Ok(DataType::TIMESTAMP_NTZ),
            "binary" => Ok(DataType::BINARY),
            "decimal" => self.decimal(),
            "array" => self.nested(Self::array),
            "map" => self.nested(Self::map),
            "struct" => self.nested(Self::struct_type),
            "" => Err(format!("is malformed: expected a type {}", self.position())),
            _ => Err(format!("names the unrecognized type '{word}'")),
        }
    }

    fn nested(
        &mut self,
        parse: impl FnOnce(&mut Self) -> Result<DataType, String>,
    ) -> Result<DataType, String> {
        if self.depth == MAX_NESTING_DEPTH {
            return Err(format!(
                "is malformed: it nests deeper than {MAX_NESTING_DEPTH} levels"
            ));
        }
        self.depth += 1;
        let parsed = parse(self);
        self.depth -= 1;
        parsed
    }

    fn length(&mut self) -> Result<u32, String> {
        self.expect('(')?;
        let length = self.number()?;
        self.expect(')')?;
        Ok(length)
    }

    fn decimal(&mut self) -> Result<DataType, String> {
        let (precision, scale) = if self.eat('(') {
            let precision = self.number()?;
            let scale = if self.eat(',') { self.number()? } else { 0 };
            self.expect(')')?;
            (precision, scale)
        } else {
            (DEFAULT_DECIMAL.0.into(), DEFAULT_DECIMAL.1.into())
        };
        let invalid =
            || format!("is malformed: decimal({precision},{scale}) is not a valid decimal");
        let precision = u8::try_from(precision).map_err(|_| invalid())?;
        let scale = u8::try_from(scale).map_err(|_| invalid())?;
        DataType::decimal(precision, scale).map_err(|_| invalid())
    }

    fn array(&mut self) -> Result<DataType, String> {
        self.expect('<')?;
        let element = self.data_type()?;
        self.expect('>')?;
        Ok(ArrayType::new(element, true).into())
    }

    fn map(&mut self) -> Result<DataType, String> {
        self.expect('<')?;
        let key = self.data_type()?;
        self.expect(',')?;
        let value = self.data_type()?;
        self.expect('>')?;
        Ok(MapType::new(key, value, true).into())
    }

    fn struct_type(&mut self) -> Result<DataType, String> {
        self.expect('<')?;
        let mut fields = Vec::new();
        loop {
            let name = self.member_name()?;
            self.expect(':')?;
            fields.push(StructField::nullable(name, self.data_type()?));
            if !self.eat(',') {
                break;
            }
        }
        self.expect('>')?;
        StructType::try_new(fields)
            .map(DataType::from)
            .map_err(|error| format!("is malformed: {error}"))
    }

    fn member_name(&mut self) -> Result<String, String> {
        let end = self
            .rest
            .find([':', ',', '<', '>'])
            .unwrap_or(self.rest.len());
        let (name, rest) = self.rest.split_at(end);
        let name = name.trim();
        if name.is_empty() {
            return Err(format!(
                "is malformed: expected a struct member name {}",
                self.position()
            ));
        }
        self.rest = rest;
        Ok(name.to_string())
    }

    fn word(&mut self) -> &'a str {
        self.rest = self.rest.trim_start();
        let end = self
            .rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(self.rest.len());
        let (word, rest) = self.rest.split_at(end);
        self.rest = rest;
        word
    }

    fn number(&mut self) -> Result<u32, String> {
        let word = self.word();
        word.parse()
            .map_err(|_| format!("is malformed: expected a number, found '{word}'"))
    }

    fn eat(&mut self, expected: char) -> bool {
        match self.rest.trim_start().strip_prefix(expected) {
            Some(rest) => {
                self.rest = rest;
                true
            }
            None => false,
        }
    }

    fn expect(&mut self, expected: char) -> Result<(), String> {
        if self.eat(expected) {
            Ok(())
        } else {
            Err(format!(
                "is malformed: expected '{expected}' {}",
                self.position()
            ))
        }
    }

    fn end(&mut self) -> Result<(), String> {
        if self.rest.trim().is_empty() {
            Ok(())
        } else {
            Err(format!("is malformed: unexpected '{}'", self.rest.trim()))
        }
    }

    fn position(&self) -> String {
        match self.rest.trim_start() {
            "" => "at the end".to_string(),
            rest => format!("before '{rest}'"),
        }
    }
}

#[cfg(test)]
#[path = "hive_type_tests.rs"]
mod tests;
