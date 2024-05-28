use serde_json::Number;
use sqlparser::ast::{BinaryOperator, UnaryOperator};

use crate::json_math::JsonNumber;

use super::types::{BuiltQuery, BuiltQueryForeach, BuiltQuerySelect, QueryResult, TaskAction, TaskContext};

pub fn get_results(query: &mut BuiltQuerySelect) -> Result<QueryResult, String> {
    let query_select = match &query.query_select {
        Some(select) => Ok(select),
        None => Err("No select items".to_string())
    }?;

    let json_context = &query.json_context;
    let result_tasks = query_select.select_items.iter().map(|x| {
        let task = &query.task_graph[*x];
        let alias = task.alias.clone().unwrap_or("".to_string());
        let value = json_context[x.index()].clone();
        (alias, value)
    }).collect::<Vec<_>>();

    let conditional = match query_select.where_expr {
        Some(idx) => {
            let value = json_context[idx.index()].clone();
            Some(value)
        },
        None => None
    };

    Ok(QueryResult {
        result: result_tasks,
        cond: conditional
    })
}

pub fn execute_query_select(query: &mut BuiltQuerySelect, data: &serde_json::Value) -> Result<QueryResult, String> {
    query.json_context[0] = data.clone();

    for (idx, task) in &query.tasks {
        match task.context.as_ref().unwrap() {
            TaskContext::SingleParent(parent) => {
                match &task.action {
                    TaskAction::Accessor(ids) => {
                        let mut parent_value = &query.json_context[parent.index()];
                        for id in ids {
                            parent_value = &parent_value[id];
                        }
                        query.json_context[idx.index()] = parent_value.clone();
                    },
                    TaskAction::Link => {
                        query.json_context[idx.index()] = query.json_context[parent.index()].clone();
                    },
                    TaskAction::UnaryOp(op) => {
                        let res = execute_unary_op(&query.json_context[parent.index()], op)?;
                        query.json_context[idx.index()] = res;
                    },
                    _ => {}
                }
            },
            TaskContext::DualParent(parent1, parent2) => {
                let res = match &task.action {
                    TaskAction::BinaryOp(op) => {
                        match (&query.json_context[parent1.index()], &query.json_context[parent2.index()]) {
                            (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => execute_binary_op_numeric((n1, n2), op),
                            _ => Err("Other binary ops not implemented".to_string())
                        }
                    },
                    _ => { Err("Other dual parent actions".to_string())}
                }?;

                query.json_context[idx.index()] = res;

            },
            TaskContext::MultiParent(_parents) => {
            }
        }
    }

    get_results(query)
}

pub fn execute_query_foreach(query: &mut BuiltQueryForeach, data: &serde_json::Value) -> Result<Vec<Result<QueryResult, String>>,String> {
    let mut results = Vec::new();

    let query_result = execute_query_select(&mut query.main, data)?;

    if query_result.cond.is_some() && !query_result.cond.unwrap().as_bool().unwrap() {
        return Ok(results);
    }

    if query_result.result.len() != 1 {
        return Err("Foreach query must return single value".to_string());
    }
    
    let res = query_result.result[0].1.clone();

    println!("Foreach result: {:?}", res);

    match res {
        serde_json::Value::Array(arr) => {
            for item in arr {
                let res = execute_query_select(&mut query.foreach, &item);
                results.push(res);
            }
        },
        _ => {
            return Err("Foreach query must return array".to_string());
        }
    }

    Ok(results)
}

pub fn execute_query(query: &mut BuiltQuery, data: &serde_json::Value) -> Result<Vec<Result<QueryResult,String>>, String> {
    match query {
        BuiltQuery::SELECT(select) => Ok(vec![execute_query_select(select, data)]),
        BuiltQuery::FOREACH(foreach) => execute_query_foreach(foreach, data)
    }
}

fn execute_unary_op(param : &serde_json::Value, op : &UnaryOperator) -> Result<serde_json::Value, String> {
    match op {
        UnaryOperator::Plus => {
            Ok(param.clone())
        },
        UnaryOperator::Minus => {
            match param {
                serde_json::Value::Number(n) => if n.is_i64() {
                    Ok(serde_json::Value::Number(serde_json::Number::from(-n.as_i64().unwrap())))
                } else if n.is_u64() {
                    Ok(serde_json::Value::Number(serde_json::Number::from(0-n.as_u64().unwrap())))
                } else {
                    Ok(serde_json::Value::Number(serde_json::Number::from_f64(-n.as_f64().unwrap()).unwrap()))
                }
                _ => Err(format!("{:?} not implemented for type {:?}", op, param))
            }
        },
        UnaryOperator::Not => {
            let bool_val = param.as_bool();

            if bool_val.is_none() {
                return Err(format!("Not operator ({}) requires boolean", op));
            }

            Ok(serde_json::Value::Bool(
                !bool_val.unwrap()
            ))
        },
        _=> Err(format!("Unary op {:?} not implemented", op))
    }
}

fn execute_binary_op_numeric(parameters : (&Number, &Number), op : &BinaryOperator) -> Result<serde_json::Value, String> {
    let n1 = parameters.0;
    let n2 = parameters.1;

    match op {
        BinaryOperator::Plus => {
            Ok(serde_json::Value::from((JsonNumber::from(n1) + JsonNumber::from(n2)).to_number()))
        },
        BinaryOperator::Minus => {
            Ok(serde_json::Value::from((JsonNumber::from(n1) - JsonNumber::from(n2)).to_number()))
        },
        BinaryOperator::Eq => {
            Ok(serde_json::json!(JsonNumber::from(n1) == JsonNumber::from(n2)))
        },
        BinaryOperator::Lt => {
            Ok(serde_json::json!(JsonNumber::from(n1) < JsonNumber::from(n2)))
        },
        BinaryOperator::LtEq => {
            Ok(serde_json::json!(JsonNumber::from(n1) <= JsonNumber::from(n2)))
        },
        BinaryOperator::Gt => {
            Ok(serde_json::json!(JsonNumber::from(n1) > JsonNumber::from(n2)))
        },
        BinaryOperator::GtEq => {
            Ok(serde_json::json!(JsonNumber::from(n1) >= JsonNumber::from(n2)))
        },
        _ => Err(format!("Operation {:?} not implemented", op))
    }
}