#![warn(clippy::all)]

mod json_math;
mod sql;

fn main() {
    sql::parse_and_execute(
        "SELECT
\tpayload.version AS version, 
\tpayload.meta.id AS id, version + 5, 
\tpayload.data.payload AS \"abc\" 
FROM \"/topic\" 
WHERE (version-1) = 0"
            .to_string(),
        &serde_json::json!({
            "version": 1,
            "data": {
                "payload": [1,2,3,4,5]
            },
            "meta": {
                "id": 2
            }
        }),
    );
}
