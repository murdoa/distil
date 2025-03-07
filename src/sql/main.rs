use crate::sql::{execute, parsing::parse_statement};

use super::parsing::parse;

pub fn parse_and_execute(sql_statement: String, input_data: &serde_json::Value) -> Result<Vec<Result<(Vec<(String, serde_json::Value)>, Option<serde_json::Value>), String>>, String> {

    let ast = parse(sql_statement)?;
    let parsed = ast.iter().map(|x| parse_statement((*x).clone())).collect::<Vec<_>>();

    let mut results : Vec<Result<(Vec::<(String, serde_json::Value)>, Option::<serde_json::Value>),String>> = Vec::new();

    for query in parsed {

        if query.is_err() {
            return Err(query.unwrap_err());
        }

        let mut query = query.unwrap();

        let res = execute::execute_query(&mut query, input_data);

        if res.is_err() {
            results.push(Err(res.unwrap_err()));
            continue;
        }

        results.push(query.get_results());
    }

    Ok(results)
}