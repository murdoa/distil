use crate::sql::{execute, schema, parsing::parse_statement};

use super::{parsing::parse, types::QueryResult};

pub fn parse_and_execute(sql_statement: String, input_data: &serde_json::Value) -> Result<Vec<Result<QueryResult, String>>, String> {
    let ast = parse(sql_statement)?;
    let parsed = ast.iter().map(|x| parse_statement((*x).clone())).collect::<Vec<_>>();

    let mut results : Vec<Result<QueryResult, String>> = Vec::new();

    for query in parsed {

        if query.is_err() {
            return Err(query.unwrap_err());
        }

        let mut query = query.unwrap();
        let res = execute::execute_query(&mut query, input_data);
        results.push(res);
    }

    Ok(results)
}