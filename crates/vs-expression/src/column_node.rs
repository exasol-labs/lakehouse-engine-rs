use serde_json::{Map, Value as Json};

const TABLE_NAME: &str = "tableName";
const TABLE_ALIAS: &str = "tableAlias";

/// A pushdown request's `column` node: the sole reader and writer of its keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnNode<'a> {
    /// As the request spells it; callers pick [`Self::upper_name`] or [`Self::ascii_upper_name`].
    pub name: &'a str,
    pub table_name: Option<&'a str>,
    pub table_alias: Option<&'a str>,
}

impl<'a> ColumnNode<'a> {
    /// `None` unless `node` is a `column` node with a name.
    pub fn parse(node: &'a Json) -> Option<Self> {
        Self::from_map(node.as_object()?)
    }

    fn from_map(map: &'a Map<String, Json>) -> Option<Self> {
        if map.get("type")?.as_str()? != "column" {
            return None;
        }
        let text = |key: &str| map.get(key).and_then(Json::as_str);
        Some(Self {
            name: text("name")?,
            table_name: text(TABLE_NAME),
            table_alias: text(TABLE_ALIAS),
        })
    }

    /// Full-Unicode uppercase (`ß` → `SS`), the fold of every declared column name.
    pub fn upper_name(&self) -> String {
        self.name.to_uppercase()
    }

    /// The join-leg collectors' fold. It agrees with [`Self::upper_name`] only because every
    /// declared name is already Unicode-uppercased; repair that premise, never unify the folds.
    pub fn ascii_upper_name(&self) -> String {
        self.name.to_ascii_uppercase()
    }

    pub fn to_json(name: &str, table_name: Option<&str>, table_alias: Option<&str>) -> Json {
        let mut node = serde_json::json!({"type": "column", "name": name});
        if let Some(table) = table_name {
            node[TABLE_NAME] = Json::String(table.to_string());
        }
        if let Some(alias) = table_alias {
            node[TABLE_ALIAS] = Json::String(alias.to_string());
        }
        node
    }
}

/// Visits every `column` node at any depth.
pub fn walk_column_nodes<'a>(tree: &'a Json, f: &mut impl FnMut(ColumnNode<'a>)) {
    match tree {
        Json::Object(map) => {
            if let Some(column) = ColumnNode::from_map(map) {
                f(column);
            }
            for value in map.values() {
                walk_column_nodes(value, f);
            }
        }
        Json::Array(items) => {
            for item in items {
                walk_column_nodes(item, f);
            }
        }
        _ => {}
    }
}

/// Copies `tree` without any `tableAlias` key, at every depth.
pub fn strip_table_alias(tree: &Json) -> Json {
    match tree {
        Json::Object(map) => Json::Object(
            map.iter()
                .filter(|(key, _)| key.as_str() != TABLE_ALIAS)
                .map(|(key, value)| (key.clone(), strip_table_alias(value)))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(strip_table_alias).collect()),
        other => other.clone(),
    }
}

/// Copies `tree`, setting each `column` node's `tableAlias` to `f`'s answer.
pub fn rewrite_table_aliases(tree: &Json, f: &mut impl FnMut(ColumnNode<'_>) -> String) -> Json {
    match tree {
        Json::Object(map) => {
            let mut out: Map<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), rewrite_table_aliases(value, f)))
                .collect();
            if let Some(column) = ColumnNode::from_map(map) {
                out.insert(TABLE_ALIAS.to_string(), Json::String(f(column)));
            }
            Json::Object(out)
        }
        Json::Array(items) => Json::Array(
            items
                .iter()
                .map(|item| rewrite_table_aliases(item, f))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Copies `tree`, replacing each `column` node with an unqualified one named by `f`'s answer.
pub fn rename_columns(tree: &Json, f: &mut impl FnMut(ColumnNode<'_>) -> String) -> Json {
    match tree {
        Json::Object(map) => match ColumnNode::from_map(map) {
            Some(column) => ColumnNode::to_json(&f(column), None, None),
            None => Json::Object(
                map.iter()
                    .map(|(key, value)| (key.clone(), rename_columns(value, f)))
                    .collect(),
            ),
        },
        Json::Array(items) => {
            Json::Array(items.iter().map(|item| rename_columns(item, f)).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
#[path = "column_node_tests.rs"]
mod tests;
