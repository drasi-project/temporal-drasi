use drasi_core::evaluation::{
    context::{QueryPartEvaluationContext, QueryVariables},
    variable_value::VariableValue,
};
use drasi_lib::channels::ResultDiff;
use serde_json::Value;

pub(super) fn diffs(contexts: &[QueryPartEvaluationContext]) -> Vec<ResultDiff> {
    contexts
        .iter()
        .filter_map(|context| match context {
            QueryPartEvaluationContext::Adding {
                after,
                row_signature,
            } => Some(ResultDiff::Add {
                data: variables(after),
                row_signature: *row_signature,
            }),
            QueryPartEvaluationContext::Removing {
                before,
                row_signature,
            } => Some(ResultDiff::Delete {
                data: variables(before),
                row_signature: *row_signature,
            }),
            QueryPartEvaluationContext::Updating {
                before,
                after,
                row_signature,
            } => {
                let after = variables(after);
                Some(ResultDiff::Update {
                    data: after.clone(),
                    before: variables(before),
                    after,
                    grouping_keys: None,
                    row_signature: *row_signature,
                })
            }
            QueryPartEvaluationContext::Aggregation {
                before,
                after,
                row_signature,
                ..
            } => Some(ResultDiff::Aggregation {
                before: before.as_ref().map(variables),
                after: variables(after),
                row_signature: *row_signature,
            }),
            QueryPartEvaluationContext::Noop => None,
        })
        .collect()
}

fn variables(variables: &QueryVariables) -> Value {
    Value::Object(
        variables
            .iter()
            .map(|(key, value)| (key.to_string(), value_json(value)))
            .collect(),
    )
}

// The SDK's reaction codec differs from VariableValue's Into<Value> implementation.
pub(super) fn value_json(value: &VariableValue) -> Value {
    match value {
        VariableValue::Null => Value::Null,
        VariableValue::Bool(value) => Value::Bool(*value),
        VariableValue::Float(value) => {
            let text = value.to_string();
            if value.is_f64() {
                text.parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::String(text))
            } else {
                Value::String(text)
            }
        }
        VariableValue::Integer(value) => {
            if let Some(value) = value.as_i64() {
                Value::Number(value.into())
            } else if let Some(value) = value.as_u64() {
                Value::Number(value.into())
            } else {
                Value::String(value.to_string())
            }
        }
        VariableValue::String(value) => Value::String(value.clone()),
        VariableValue::List(values) => Value::Array(values.iter().map(value_json).collect()),
        VariableValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_json(value)))
                .collect(),
        ),
        VariableValue::Date(value) => Value::String(value.to_string()),
        VariableValue::LocalTime(value) => Value::String(value.to_string()),
        VariableValue::ZonedTime(value) => Value::String(value.to_string()),
        VariableValue::LocalDateTime(value) => Value::String(value.to_string()),
        VariableValue::ZonedDateTime(value) => Value::String(value.datetime().to_rfc3339()),
        VariableValue::Duration(value) => Value::String(value.to_string()),
        _ => Value::String(format!("{value:?}")),
    }
}
