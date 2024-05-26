use sqlparser::{ast::{SetExpr, Statement}, dialect::CustomDialect, parser::{Parser, ParserError}, tokenizer::TokenizerError};

use super::{types::Query, builder::build_select_query};


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

pub fn parse_statement(stmt: Statement) -> Result<Query, String> {

    let err_msg = format!("Error Query Type Unimplemented: {stmt:?}");

    match stmt {
        Statement::Query(ref query) => {
            match *query.body {
                SetExpr::Select(_) => {
                    build_select_query(stmt)
                },
                _ => Err(err_msg)
            }
        }
        _ => Err(err_msg)
    }
}

