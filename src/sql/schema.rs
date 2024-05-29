use std::collections::HashMap;

use petgraph::matrix_graph::Nullable;

use super::types::{NestedQueryResult, QueryResult, SimpleQueryResult};


#[derive(Debug, PartialEq)]
pub enum SchemaNode {
    Null,
    Bool,
    Number,
    String,
    Array(Option<Box<SchemaNode>>),
    Object(Option<HashMap<String, SchemaNode>>),
    Nullable(Box<SchemaNode>)
}

impl SchemaNode {
    pub fn from(discrim : std::mem::Discriminant<serde_json::Value>)-> SchemaNode {
        if discrim == std::mem::discriminant(&serde_json::Value::Null) {
            SchemaNode::Null
        } else if discrim == std::mem::discriminant(&serde_json::Value::Bool(false)) {
            SchemaNode::Bool
        } else if discrim == std::mem::discriminant(&serde_json::Value::Number(serde_json::Number::from(0))) {
            SchemaNode::Number
        } else if discrim == std::mem::discriminant(&serde_json::Value::String("".to_string())) {
            SchemaNode::String
        } else if discrim == std::mem::discriminant(&serde_json::Value::Array(vec![])) {
            SchemaNode::Array(None)
        } else if discrim == std::mem::discriminant(&serde_json::Value::Object(serde_json::Map::new())) {
            SchemaNode::Object(None)
        } else {
            panic!("Invalid Discriminant")
        }
    }

    pub fn validate_schema(&self, other: &SchemaNode) -> bool {
        if self == other {
            return true;
        }

        use std::mem::discriminant;
        if discriminant(self) != discriminant(other) {
            return match (self, other) {
                ( &SchemaNode::Nullable(ref opt), &SchemaNode::Null ) => return true,
                ( &SchemaNode::Null, &SchemaNode::Nullable(ref opt) ) => return true,
                _ => false
            }
        }

        return match (self, other) {
            ( &SchemaNode::Array(ref opt1), &SchemaNode::Array(ref opt2) ) => {
                opt1.is_none() || opt2.is_none()
            },
            ( &SchemaNode::Object(ref opt1), &SchemaNode::Object(ref opt2) ) => {
                opt1.is_none() || opt2.is_none()
            }
            _ => false
        }
    }


    pub fn validate_json(&self, json_value : &serde_json::Value) -> bool {
        use SchemaNode::*;
        use serde_json::Value;

        match (self, json_value) {
            (&Null, Value::Null) => true,
            (&Bool, Value::Bool(ref val)) => true,
            (&Number, Value::Number(ref val)) => true,
            (&String, Value::String(ref val)) => true,
            (&Array(ref opt), Value::Array(ref val)) => {
                if opt.is_none() {
                    true
                } else {
                    let schema_type = opt.as_ref().unwrap().as_ref();
                    val.iter().all(|x| schema_type.validate_json(x))
                }
            },
            (&Object(ref opt), Value::Object(ref map)) => {
                if opt.is_none() {
                    true
                } else {
                    opt.as_ref().unwrap().iter().all( |(k,v)| {
                        if !map.contains_key(k) {
                            false
                        } else {
                            v.validate_json(&map[k])
                        }
                    })
                }
            }
            _ => false
        }
    }
}

pub fn output_to_schema(output: &Vec<serde_json::Value>) -> Vec<SchemaNode> {
    output.iter().map(|x| SchemaNode::from(std::mem::discriminant(x))).collect()
}

pub fn validate_schema(schema: &Vec<SchemaNode>, output: &Vec<serde_json::Value>) -> bool {
    if schema.len() != output.len() {
        return false;
    }

    schema.iter()
        .zip(output.iter())
        .all(|(sch, val)| sch.validate_json(val))
}

pub fn validate_simple_query_result(schema: &Vec<SchemaNode>, output: &SimpleQueryResult) -> bool {
    validate_schema(schema, &output.result.iter().map(|x| x.1.clone()).collect())
}

pub fn validate_nested_query_result(schema: &Vec<SchemaNode>, output: &NestedQueryResult) -> Vec<bool> {

    output.result.iter().map(|x| {
        match x {
            Ok(qres) => match qres {
                QueryResult::Simple(x) => validate_simple_query_result(schema, x),
                _ => false
            },
            Err(_) => false
        }
    }).collect()
}