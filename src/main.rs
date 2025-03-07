#![warn(clippy::all)]

use std::collections::HashMap;

use crate::sql::{schema::{self, SchemaNode}, types::QueryResult};

mod json_math;
mod sql;

fn main() {
    // let sql_statement = "SELECT
    // \tpayload.version AS version, 
    // \tpayload.meta.id AS id, 
    // \tversion + 5, 
    // \tpayload.data.payload AS \"abc\" 
    // FROM \"/topic\" 
    // WHERE (version-1) = 0".to_string();
    
    let sql_statement = "FOREACH
    \tpayload.data.payload AS \"item\"
    \tRETURN item + 1
    \tWHEN item > 3
    \tFROM \"/topic\"
    \tWHERE payload.version >= 1
    ".to_string();

    let input_data = serde_json::json!({
        "version": 1,
        "data": {
            "payload": [1,2,3,4,5, "a"]
        },
        "meta": {
            "id": 2
        }
    });

    let input_data_schema =
        SchemaNode::Object(Some(HashMap::from([
            ("version".to_string(), SchemaNode::Number),
            ("data".to_string(), SchemaNode::Object(Some(HashMap::from([
                ("payload".to_string(), SchemaNode::Array(None))
            ])))),
            ("meta".to_string(), SchemaNode::Object(Some(HashMap::from([
                ("id".to_string(), SchemaNode::Number)
            ])))),
            ("abcd".to_string(), SchemaNode::Number)
        ])));

    println!("Valid Input Data: {}", input_data_schema.validate_json(&input_data));

    let output_schema = vec![SchemaNode::Number];

    println!("INPUT SQL:\n{}\n", sql_statement);
    println!("INPUT DATA:\n{}\n", input_data.to_string());

    let result = sql::parse_and_execute(
        sql_statement,
        &input_data
    );

    if result.is_err() {
        println!("Error: {}", result.unwrap_err());
        return;
    }

    let result = result.unwrap();

    for res in result {
        if res.is_err() {
            println!("Error: {}", res.unwrap_err());
            continue;
        }

        let res = res.unwrap();

        sql::debug::print_query_result(&res);

        let valid = match res {
            QueryResult::Simple(simple) => vec![schema::validate_simple_query_result(&output_schema, &simple)],
            QueryResult::Nested(nested) => schema::validate_nested_query_result(&output_schema, &nested)
        };

        println!("Output Valid: {:?}", valid);
        
    }
}
