#![warn(clippy::all)]

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

        for row in res {
            if row.is_err() {
                println!("Error: {}", row.unwrap_err());
                continue;
            }

            let row = row.unwrap();
            println!("RESULT: {:?}", row);
        }
    }
}
