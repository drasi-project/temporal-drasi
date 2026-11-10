use crate::protocol::{ComponentRecipe, Error, PluginDescriptor, Result};
use serde_json::{json, Map, Number, Value};
use std::{cmp::Ordering, collections::BTreeSet};

fn unsupported() -> Error {
    Error::new(
        3,
        "plugin descriptor uses an unsupported configuration schema",
    )
}

fn invalid() -> Error {
    Error::new(
        1,
        "plugin configuration does not match the pinned descriptor",
    )
}

struct Budget(usize);

impl Budget {
    fn step(&mut self, depth: usize) -> Result<()> {
        if depth > 32 || self.0 == 0 {
            return Err(unsupported());
        }
        self.0 -= 1;
        Ok(())
    }
}

fn reference<'a>(document: &'a Value, value: &Value) -> Result<&'a Value> {
    let name = value
        .as_str()
        .and_then(|value| value.strip_prefix("#/components/schemas/"))
        .filter(|name| !name.is_empty() && !name.contains(['/', '~']))
        .ok_or_else(unsupported)?;
    document.get(name).ok_or_else(unsupported)
}

fn unsigned(value: &Value) -> Result<usize> {
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(unsupported)
}

fn integer(value: &Value) -> bool {
    value.as_i64().is_some() || value.as_u64().is_some()
}

fn compare(left: &Number, right: &Number) -> Ordering {
    match (left.as_i64(), right.as_i64()) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(left), None) if left < 0 => Ordering::Less,
        (None, Some(right)) if right < 0 => Ordering::Greater,
        _ => left
            .as_u64()
            .expect("validated nonnegative integer")
            .cmp(&right.as_u64().expect("validated nonnegative integer")),
    }
}

fn check(document: &Value, schema: &Value, depth: usize, budget: &mut Budget) -> Result<()> {
    budget.step(depth)?;
    if schema.is_boolean() {
        return Ok(());
    }
    let object = schema.as_object().ok_or_else(unsupported)?;
    for (key, value) in object {
        match key.as_str() {
            "$ref" => check(document, reference(document, value)?, depth + 1, budget)?,
            "type" => {
                let types = match value {
                    Value::String(_) => vec![value],
                    Value::Array(types) if !types.is_empty() => types.iter().collect(),
                    _ => return Err(unsupported()),
                };
                let mut unique = BTreeSet::new();
                for kind in types {
                    let kind = kind.as_str().ok_or_else(unsupported)?;
                    if !matches!(
                        kind,
                        "null" | "boolean" | "integer" | "number" | "string" | "array" | "object"
                    ) || !unique.insert(kind)
                    {
                        return Err(unsupported());
                    }
                }
            }
            "properties" => {
                for property in value.as_object().ok_or_else(unsupported)?.values() {
                    check(document, property, depth + 1, budget)?;
                }
            }
            "required" => {
                let mut names = BTreeSet::new();
                for name in value.as_array().ok_or_else(unsupported)? {
                    if !names.insert(name.as_str().ok_or_else(unsupported)?) {
                        return Err(unsupported());
                    }
                }
            }
            "additionalProperties" => {
                value.as_bool().ok_or_else(unsupported)?;
            }
            "items" => check(document, value, depth + 1, budget)?,
            "allOf" | "anyOf" | "oneOf" => {
                let branches = value.as_array().ok_or_else(unsupported)?;
                if branches.is_empty() || branches.len() > 64 {
                    return Err(unsupported());
                }
                for branch in branches {
                    check(document, branch, depth + 1, budget)?;
                }
            }
            "enum" => {
                if value.as_array().is_none_or(Vec::is_empty) {
                    return Err(unsupported());
                }
            }
            "minLength" | "maxLength" | "minItems" | "maxItems" | "minProperties"
            | "maxProperties" => {
                unsigned(value)?;
            }
            "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" => {
                if object.get("type").and_then(Value::as_str) != Some("integer") || !integer(value)
                {
                    return Err(unsupported());
                }
            }
            "const" | "title" | "description" | "default" | "examples" | "format"
            | "deprecated" | "readOnly" | "writeOnly" | "x-ui" => {}
            key if key.starts_with("x-ui:") => {}
            _ => return Err(unsupported()),
        }
    }
    Ok(())
}

fn kind_matches(kind: &str, value: &Value) -> bool {
    match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "integer" => integer(value),
        "number" => value.is_number(),
        "string" => value.is_string(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

fn matches(
    document: &Value,
    schema: &Value,
    value: &Value,
    depth: usize,
    budget: &mut Budget,
) -> Result<bool> {
    budget.step(depth)?;
    if let Some(allowed) = schema.as_bool() {
        return Ok(allowed);
    }
    let object = schema.as_object().ok_or_else(unsupported)?;
    for (key, constraint) in object {
        let accepted = match key.as_str() {
            "$ref" => matches(
                document,
                reference(document, constraint)?,
                value,
                depth + 1,
                budget,
            )?,
            "type" => match constraint {
                Value::String(kind) => kind_matches(kind, value),
                Value::Array(types) => types
                    .iter()
                    .any(|kind| kind_matches(kind.as_str().expect("checked type"), value)),
                _ => return Err(unsupported()),
            },
            "const" => constraint == value,
            "enum" => constraint.as_array().expect("checked enum").contains(value),
            "allOf" | "anyOf" | "oneOf" => {
                let branches = constraint.as_array().expect("checked branches");
                let mut count = 0;
                for branch in branches {
                    count += usize::from(matches(document, branch, value, depth + 1, budget)?);
                }
                match key.as_str() {
                    "allOf" => count == branches.len(),
                    "anyOf" => count != 0,
                    _ => count == 1,
                }
            }
            "properties" => {
                let mut accepted = true;
                if let Some(values) = value.as_object() {
                    for (field, schema) in constraint.as_object().expect("checked properties") {
                        if let Some(value) = values.get(field) {
                            accepted &= matches(document, schema, value, depth + 1, budget)?;
                        }
                    }
                }
                accepted
            }
            "required" => value.as_object().is_none_or(|values| {
                constraint
                    .as_array()
                    .expect("checked required")
                    .iter()
                    .all(|field| values.contains_key(field.as_str().expect("checked field")))
            }),
            "additionalProperties" => {
                constraint == &Value::Bool(true)
                    || value.as_object().is_none_or(|values| {
                        let properties = object.get("properties").and_then(Value::as_object);
                        values.keys().all(|field| {
                            properties.is_some_and(|properties| properties.contains_key(field))
                        })
                    })
            }
            "items" => {
                let mut accepted = true;
                if let Some(values) = value.as_array() {
                    for value in values {
                        accepted &= matches(document, constraint, value, depth + 1, budget)?;
                    }
                }
                accepted
            }
            "minLength" | "maxLength" | "minItems" | "maxItems" | "minProperties"
            | "maxProperties" => {
                let length = match key.as_str() {
                    "minLength" | "maxLength" => value.as_str().map(|value| value.chars().count()),
                    "minItems" | "maxItems" => value.as_array().map(Vec::len),
                    _ => value.as_object().map(Map::len),
                };
                length.is_none_or(|length| {
                    if key.starts_with("min") {
                        length >= unsigned(constraint).expect("checked bound")
                    } else {
                        length <= unsigned(constraint).expect("checked bound")
                    }
                })
            }
            "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" => {
                if integer(value) {
                    let order = compare(
                        value.as_number().expect("integer"),
                        constraint.as_number().expect("checked bound"),
                    );
                    match key.as_str() {
                        "minimum" => order != Ordering::Less,
                        "maximum" => order != Ordering::Greater,
                        "exclusiveMinimum" => order == Ordering::Greater,
                        _ => order == Ordering::Less,
                    }
                } else {
                    !value.is_number()
                }
            }
            _ => true,
        };
        if !accepted {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Validate explicit top-level fields against a bounded subset of SDK component schemas.
/// UI annotations and formats are descriptive, never credential authorization.
pub(super) fn validate(descriptor: &PluginDescriptor, recipe: &ComponentRecipe) -> Result<()> {
    let document = &descriptor.configuration_schema;
    if serde_json::to_vec(document)?.len() > 64 << 10 {
        return Err(unsupported());
    }
    let schema = document
        .get(&descriptor.schema_name)
        .ok_or_else(unsupported)?;
    let root = schema.as_object().ok_or_else(unsupported)?;
    if root.get("type").and_then(Value::as_str) != Some("object") {
        return Err(unsupported());
    }
    let properties = root
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(unsupported)?;
    check(document, schema, 0, &mut Budget(8192))?;
    let mut input = recipe
        .configuration
        .as_object()
        .ok_or_else(invalid)?
        .clone();
    for field in recipe.secrets.keys() {
        if input
            .insert(
                field.clone(),
                json!({"kind":"Secret","name":"schema-check"}),
            )
            .is_some()
        {
            return Err(invalid());
        }
    }
    if input.keys().any(|field| !properties.contains_key(field))
        || !matches(
            document,
            schema,
            &Value::Object(input),
            0,
            &mut Budget(8192),
        )?
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{PluginCategory, PluginComponentRef, SecretRef};
    use std::collections::BTreeMap;

    fn descriptor(schema: Value) -> PluginDescriptor {
        PluginDescriptor {
            plugin: "fixture".into(),
            component: "fixture".into(),
            category: PluginCategory::Source,
            config_version: "1".into(),
            schema_name: "Config".into(),
            configuration_schema: json!({"Config": schema}),
            supported: true,
        }
    }

    fn recipe(configuration: Value) -> ComponentRecipe {
        ComponentRecipe {
            plugin: PluginComponentRef {
                digest: "pin".into(),
                component: "fixture".into(),
            },
            configuration,
            secrets: BTreeMap::new(),
        }
    }

    #[test]
    fn supported_schema_checks_values_and_exact_integer_bounds() {
        let descriptor = descriptor(json!({
            "type":"object", "additionalProperties":false,
            "properties": {
                "count":{"type":"integer","minimum":-1,"maximum":18446744073709551614_u64},
                "text":{"type":"string","minLength":1,"maxLength":2},
                "rows":{"type":"array","maxItems":2,"items":{
                    "type":"object","properties":{"name":{"enum":["first","second"]}},
                    "required":["name"],"additionalProperties":false
                }}
            }, "required":["count","text"]
        }));
        for (name, value, accepted) in [
            (
                "upper bound",
                json!({"count":18446744073709551614_u64,"text":"ab"}),
                true,
            ),
            (
                "above bound",
                json!({"count":18446744073709551615_u64,"text":"ab"}),
                false,
            ),
            ("lower bound", json!({"count":-1,"text":"a"}), true),
            ("below bound", json!({"count":-2,"text":"a"}), false),
            ("fraction", json!({"count":0.5,"text":"a"}), false),
            (
                "Unicode length",
                json!({"count":0,"text":"\u{1f642}"}),
                true,
            ),
            ("long string", json!({"count":0,"text":"abc"}), false),
            (
                "unknown field",
                json!({"count":0,"text":"a","unknown":true}),
                false,
            ),
            ("missing field", json!({"count":0}), false),
            (
                "nested valid",
                json!({"count":0,"text":"a","rows":[{"name":"first"}]}),
                true,
            ),
            (
                "nested invalid",
                json!({"count":0,"text":"a","rows":[{"name":"third"}]}),
                false,
            ),
        ] {
            assert_eq!(
                validate(&descriptor, &recipe(value)).is_ok(),
                accepted,
                "{name}"
            );
        }
    }

    #[test]
    fn exact_references_and_secret_shapes_not_ui_hints_grant_fields() {
        let mut descriptor = descriptor(json!({
            "type":"object","properties":{"credential":{"$ref":"#/components/schemas/Secret"}},
            "required":["credential"]
        }));
        descriptor.configuration_schema["Secret"] = json!({
            "type":"object", "properties":{"kind":{"const":"Secret"},"name":{"type":"string"}},
            "required":["kind","name"], "additionalProperties":false
        });
        let mut recipe = recipe(json!({}));
        recipe.secrets.insert(
            "credential".into(),
            SecretRef {
                provider: "provider".into(),
                key: "key".into(),
                version: "version".into(),
            },
        );
        validate(&descriptor, &recipe).unwrap();
        let original = serde_json::to_vec(&recipe).unwrap();
        descriptor.configuration_schema["Secret"] = json!({"type":"string","x-ui":{"secret":true}});
        assert_eq!(validate(&descriptor, &recipe).unwrap_err().code, 1);
        assert_eq!(serde_json::to_vec(&recipe).unwrap(), original);
        recipe.secrets.clear();
        recipe.configuration = json!({"credential":"literal"});
        validate(&descriptor, &recipe).unwrap();
    }

    #[test]
    fn sdk_ui_extensions_are_descriptive_and_cannot_relax_constraints() {
        let descriptor = descriptor(json!({
            "type": "object",
            "properties": {"field": {
                "type": "integer", "minimum": 1, "maximum": 10,
                "x-ui:widget": "password",
                "x-ui:group": "Connection",
                "x-ui:order": 1,
                "x-ui:condition": {"field": "other", "value": "hidden"},
                "x-ui:secret": true
            }},
            "required": ["field"]
        }));
        validate(&descriptor, &recipe(json!({"field": 5}))).unwrap();
        for value in [json!({"field": 11}), json!({"field": "value"}), json!({})] {
            assert_eq!(validate(&descriptor, &recipe(value)).unwrap_err().code, 1);
        }
        let mut secret = recipe(json!({}));
        secret.secrets.insert(
            "field".into(),
            SecretRef {
                provider: "provider".into(),
                key: "key".into(),
                version: "version".into(),
            },
        );
        assert_eq!(validate(&descriptor, &secret).unwrap_err().code, 1);
    }

    #[test]
    fn unsupported_constraints_and_recursive_references_fail_closed() {
        for schema in [
            json!({"type":"string","pattern":"private-schema-text"}),
            json!({"$ref":"https://invalid.example/schema"}),
            json!({"$ref":"#/components/schemas/Config"}),
            json!({"type":"number","minimum":0}),
            json!({"type":"integer","minimum":0.1}),
            json!({"oneOf":[]}),
            json!({"type":"string","minLength":-1}),
        ] {
            let descriptor = descriptor(json!({"type":"object","properties":{"field":schema}}));
            let error = validate(&descriptor, &recipe(json!({}))).unwrap_err();
            assert_eq!(error.code, 3);
            assert!(!error.message.contains("private-schema-text"));
        }
        let mut budget = Budget(0);
        assert!(check(&json!({}), &Value::Bool(true), 0, &mut budget).is_err());
    }
}
