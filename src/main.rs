#![warn(clippy::all)]

use crate::sql::types::QueryResult;

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
            "payload": [1,2,3,4,5]
        },
        "meta": {
            "id": 2
        }
    });

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

        match res {
            QueryResult::Simple(simple) => {
                println!("SELECT");
                for (key, value) in simple.result {
                    println!("\t{}: {}", key, value);
                }
                if simple.cond.is_some() {
                    println!("WHERE");
                    println!("\t{}", simple.cond.unwrap());
                }
            }, 
            QueryResult::Nested(nested) => {
                println!("FOREACH RETURN");

                let max_column = nested.result.iter().map(|x| {
                    match x {
                        Ok(QueryResult::Simple(simple)) => simple.result.len(),
                        _ => 0
                    }
                }).max().unwrap_or(0);


                print!("\t|{:8}", " INDEX");
                (0..max_column).for_each(|i| print!("|{:8}|{:8}", i, " ALIAS"));
                println!("|{:8}|", " COND");

                print!("\t");
                (0..(max_column * 2 + 2) * 9 + 1).for_each(|_| print!("="));
                print!("\n");

                for (i, res) in nested.result.iter().enumerate() {
                    print!("\t|{:8}|", i);
                    match res {
                        Ok(QueryResult::Simple(simple)) => {
                            for (key, value) in &simple.result {
                                print!("{:8}|{:8}|", format!("{}",value), key);
                            }
                            println!("{:8}|", format!("{}", simple.cond.clone().unwrap_or(serde_json::Value::Null)));
                        },
                        Err(e) => {
                            println!("Error: {}|", e);
                        },
                        _ => {
                            println!("Error: Nested query must return simple result|");
                        }
                    }
                }

                if nested.cond.is_some() {
                    println!("WHERE");
                    println!("\t{}", nested.cond.unwrap());
                }
            }
        }
        
    }
}
