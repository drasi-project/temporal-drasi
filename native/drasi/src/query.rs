use crate::protocol::{Error, Language, Result};
use drasi_core::{evaluation::functions::FunctionRegistry, query::QueryBuilder};
use drasi_functions_cypher::CypherFunctionSet;
use drasi_functions_gql::GQLFunctionSet;
use drasi_query_cypher::CypherParser;
use drasi_query_gql::GQLParser;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{Semaphore, SemaphorePermit};

static COMPILERS: Semaphore = Semaphore::const_new(2);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    language: Language,
    text: String,
}

pub(crate) struct Compilation {
    request: Request,
    _permit: SemaphorePermit<'static>,
}

impl Request {
    pub(crate) fn prepare(self) -> Result<Compilation> {
        if self.text.trim().is_empty() || self.text.len() > 65_536 {
            return Err(Error::new(1, "query text requires 1..65536 bytes"));
        }
        let permit = COMPILERS
            .try_acquire()
            .map_err(|_| Error::new(3, "native compiler concurrency limit reached"))?;
        Ok(Compilation {
            request: self,
            _permit: permit,
        })
    }
}

impl Compilation {
    pub(crate) async fn validate(self) -> Result<Value> {
        let request = self.request;
        let functions = Arc::new(FunctionRegistry::new());
        let functions = match request.language {
            Language::Cypher => functions.with_cypher_function_set(),
            Language::Gql => functions.with_gql_function_set(),
        };
        let builder = match request.language {
            Language::Cypher => {
                QueryBuilder::new(request.text, Arc::new(CypherParser::new(functions.clone())))
            }
            Language::Gql => {
                QueryBuilder::new(request.text, Arc::new(GQLParser::new(functions.clone())))
            }
        };
        Ok(json!({
            "valid": builder.with_function_registry(functions).try_build().await.is_ok()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_admission_and_actual_language_compilation() {
        let request = || Request {
            language: Language::Cypher,
            text: "MATCH (n) RETURN n".into(),
        };
        let first = request().prepare().unwrap().validate();
        let second = request().prepare().unwrap().validate();
        assert_eq!(request().prepare().err().unwrap().code, 3);
        drop(first);
        let replacement = request().prepare().unwrap().validate();
        drop((second, replacement));

        for language in ["cypher", "gql"] {
            for (text, valid) in [
                ("MATCH (n:Record) RETURN n.id AS id", true),
                ("MATCH (n:Record) RETURN count(n) AS total", true),
                ("MATCH (", false),
                ("not a query", false),
            ] {
                let request = serde_json::from_value(json!({
                    "language": language, "text": text
                }))
                .unwrap();
                assert_eq!(
                    Request::prepare(request).unwrap().validate().await.unwrap(),
                    json!({"valid": valid}),
                    "{language}: {text}"
                );
            }
            let mut maximum = "MATCH (n) RETURN n".to_string();
            maximum.push_str(&" ".repeat(65_536 - maximum.len()));
            let request = serde_json::from_value(json!({
                "language": language, "text": maximum
            }))
            .unwrap();
            assert_eq!(
                Request::prepare(request).unwrap().validate().await.unwrap(),
                json!({"valid": true})
            );
        }
    }

    #[tokio::test]
    async fn compiler_rejects_empty_and_oversized_input() {
        for text in [" ".into(), "x".repeat(65_537)] {
            let error = Request {
                language: Language::Cypher,
                text,
            }
            .prepare()
            .err()
            .unwrap();
            assert_eq!(error.code, 1);
        }
    }
}
