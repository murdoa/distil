use sqlparser::{ast::{SetExpr, Statement}, dialect::CustomDialect, parser::{Parser, ParserError}};

use super::{builder, types::{BuiltQuery}};

// Parsing function uses custom dialect and returns parsed ast from sqlparser
pub fn parse(query: String) -> Result<Vec<Statement>, String> {
    let dialect = CustomDialect {};

    let ast = Parser::parse_sql(&dialect, query.as_str());

    ast.map_err(|x| match x {
        ParserError::TokenizerError(str) => format!("TokenizerError: {}", str),
        ParserError::ParserError(str) => format!("ParserError: {}", str),
        ParserError::RecursionLimitExceeded => "RecursionLimitExceeded".to_string()
    })
}

pub fn parse_statement(stmt: Statement) -> Result<BuiltQuery, String> {
    let err_msg = format!("Error Query Type Unimplemented: {stmt:?}");
    match stmt.clone() {
        Statement::Foreach(ref foreach) => {
            let mut foreach = builder::build_foreach_query(foreach)?;
            foreach.sql_stmt = Some(stmt);
            Ok(BuiltQuery::FOREACH(foreach))
        }
        Statement::Query(query) => {
            match query.body.as_ref() {
                SetExpr::Select(select_query) => {
                    let mut select = builder::build_select_query(select_query.clone(), "payload".to_string())?;
                    select.sql_stmt = Some(stmt);
                    Ok(BuiltQuery::SELECT(select))
                },
                _ => Err(err_msg)
            }
        }
        _ => Err(err_msg)
    }
}

