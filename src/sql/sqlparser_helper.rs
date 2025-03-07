use sqlparser::ast::{TableFactor, TableWithJoins};


pub fn get_table_name(table: TableWithJoins) -> Result<String, String> {
    if table.joins.len() > 0 {
        return Err("Joins not supported".to_string());
    }
    match table.relation {
        TableFactor::Table {
            name,
            alias: _,
            args: _,
            with_hints: _,
            version: _,
            partitions: _,
        } => Ok(name.to_string()),
        _ => Err("Unsupported table".to_string()),
    }
}