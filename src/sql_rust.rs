use petgraph::{
    dot::Dot,
    graph::{NodeIndex, Node},
    stable_graph::StableDiGraph,
    visit::{EdgeRef, IntoNodeReferences}
};
use serde_json::Number;
use sqlparser::{
    ast::{
        BinaryOperator, Expr, Select, SelectItem, SetExpr, Statement, TableFactor,
        TableWithJoins, UnaryOperator,
    },
    dialect::CustomDialect,
    parser::Parser,
};
use std::{collections::HashMap, fmt::{Debug, Binary}};

use crate::json_math::JsonNumber;


#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum SQLLiteral {
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug)]
enum QueryFunctions {}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum QueryAction {
    Literal(SQLLiteral),
    Link,
    Accessor(Vec<String>),
    UnaryOp(UnaryOperator),
    BinaryOp(BinaryOperator),
    Function(String),
    Root,
    Finalize,
    Stale,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum QueryContext {
    SingleParent(NodeIndex),
    DualParent(NodeIndex, NodeIndex),
    MultiParent(Vec<NodeIndex>),
}

#[derive(Debug, Clone, PartialEq)]
struct QueryTask {
    alias: Option<String>,
    action: QueryAction,
    required: bool,
    context: Option<QueryContext>,
}

#[derive(Debug)]
struct SelectQuery {
    task_graph: StableDiGraph<QueryTask, usize>,
    select_items: Vec<NodeIndex>,
    from_table: String,
    where_expr: Option<NodeIndex>,
}//

#[derive(Debug)]
enum QueryType {
    SELECT(SelectQuery),
}

fn print_graph<U, V>(graph: &StableDiGraph<U, V>)
where
    U: Debug,
    V: Debug,
{
    println!("{:?}", Dot::with_config(graph, &[]));
}

fn graph_add_expr(
    task_graph: &mut StableDiGraph<QueryTask, usize>,
    expr: Expr,
    alias: Option<String>,
) -> Result<NodeIndex, String> {
    match expr {
        Expr::Identifier(id) => {
            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: QueryAction::Accessor(Vec::from([id.to_string()])),
                required: false,
                context: None,
            });
            Ok(node)
        }
        Expr::CompoundIdentifier(ids) => {
            let multi_accessor = ids.iter().map(|x| x.to_string()).collect::<Vec<String>>();
            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: QueryAction::Accessor(multi_accessor),
                required: false,
                context: None,
            });
            Ok(node)
        }
        Expr::UnaryOp { op, expr } => {
            let parent_node = graph_add_expr(task_graph, *expr, None)?;
            let child_node = task_graph.add_node(QueryTask {
                alias: alias,
                action: QueryAction::UnaryOp(op),
                required: false,
                context: None,
            });
            task_graph.add_edge(parent_node, child_node, 1);
            Ok(child_node)
        }
        Expr::BinaryOp { left, op, right } => {
            let left_node = graph_add_expr(task_graph, *left, None)?;
            let right_node = graph_add_expr(task_graph, *right, None)?;
            let child_node = task_graph.add_node(QueryTask {
                alias: alias,
                action: QueryAction::BinaryOp(op),
                required: false,
                context: None,
            });

            task_graph.add_edge(left_node, child_node, 1);
            task_graph.add_edge(right_node, child_node, 2);

            Ok(child_node)
        }
        // Expr::Function(func) => {
        //     let mut args = Vec::<NodeIndex>::new();
        //     for arg in func.args {
        //         match arg {
        //             FunctionArg::Named { arg, name } => {
        //                 return Err("Named arguments not supported".to_string());
        //             },
        //             FunctionArg::Unnamed(arg) => {
        //                 args.push(graph_add_expr(task_graph, arg, None)?);
        //             },
        //             _ => {
        //                 return Err("Not implemented".to_string());
        //             }
        //         }
        //         args.push(graph_add_expr(task_graph, arg, None)?);
        //     }

        //     let node = task_graph.add_node(QueryTask {
        //         alias: alias,
        //         action: QueryAction::Function(func.name.to_string()),
        //         required: false,
        //     });

        //     args.iter().for_each(|idx| {
        //         task_graph.add_edge(*idx, node, 1);
        //     });

        //     Ok(node)
        // }
        Expr::Value(sqlparser::ast::Value::Number(str, _bool)) => {
            let sql_literal = if str.contains(".") {
                let number = str.parse::<f64>();
                if number.is_err() {
                    Err(number.unwrap_err().to_string())
                } else {
                    Ok(SQLLiteral::Float(number.unwrap()))
                }
            } else {
                let number = str.parse::<i64>();
                if number.is_err() {
                    Err(number.unwrap_err().to_string())
                } else {
                    Ok(SQLLiteral::Integer(number.unwrap()))
                }
            }?;

            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: QueryAction::Literal(sql_literal),
                required: false,
                context: None,
            });
            Ok(node)
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(str)) => {
            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: QueryAction::Literal(SQLLiteral::String(str)),
                required: false,
                context: None,
            });

            Ok(node)
        },
        Expr::Nested(nested_expr) => {
            graph_add_expr(task_graph, *nested_expr, alias)
        },
        _ => Err(format!("Unhandled expression type: {:?}", expr)),
    }
}

fn graph_add_select_item(
    task_graph: &mut StableDiGraph<QueryTask, usize>,
    item: SelectItem,
) -> Result<NodeIndex, String> {
    match item {
        SelectItem::UnnamedExpr(expr) => graph_add_expr(task_graph, expr, None),
        SelectItem::ExprWithAlias { expr, alias } => {
            graph_add_expr(task_graph, expr, Some(alias.value))
        }
        _ => Err("Expected unnamed expression".to_string()),
    }
}

fn get_table_name(table: TableWithJoins) -> Result<String, String> {
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

/// Traverses the given directed graph upward from the specified node index (`idx`),
/// searching for the nearest ancestor with a non-empty alias or a non-Link action.
/// Returns the index of the found ancestor, or `None` if no such ancestor is found.
fn find_parent_with_alias(
    task_graph: &StableDiGraph<QueryTask, usize>,
    idx: NodeIndex,
) -> Option<NodeIndex> {
    let parent = *task_graph
        .neighbors_directed(idx, petgraph::Direction::Incoming)
        .collect::<Vec<NodeIndex>>()
        .first()
        .unwrap();
    let parent_alias = &task_graph[parent].alias;

    if parent_alias.is_none() && task_graph[parent].action == QueryAction::Link {
        find_parent_with_alias(task_graph, parent)
    } else {
        Some(parent)
    }
}

fn compile_select_query(select: Box<Select>) -> Result<SelectQuery, String> {
    let mut task_graph = StableDiGraph::<QueryTask, usize>::new();

    let _root_node = task_graph.add_node(QueryTask {
        alias: None,
        action: QueryAction::Root,
        required: true,
        context: None,
    });

    let final_node = task_graph.add_node(QueryTask {
        alias: None,
        action: QueryAction::Finalize,
        required: true,
        context: None,
    });

    let select_items = select
        .projection
        .iter()
        .map(|x| graph_add_select_item(&mut task_graph, x.clone()))
        .collect::<Result<Vec<NodeIndex>, String>>()?;

    let from_table = get_table_name(select.from.first().unwrap().clone())?;

    let mut where_expr: Option<NodeIndex> = None;
    if select.selection.is_some() {
        where_expr = Some(graph_add_expr(
            &mut task_graph,
            select.selection.unwrap(),
            None,
        )?);
    }

    select_items.iter().for_each(|idx| {
        task_graph.node_weight_mut(*idx).unwrap().required = true;
        task_graph.add_edge(*idx, final_node, 1);
    });

    if where_expr.is_some() {
        task_graph
            .node_weight_mut(where_expr.unwrap())
            .unwrap()
            .required = true;
        task_graph.add_edge(where_expr.unwrap(), final_node, 1);
    }

    dealias_graph(&mut task_graph)?;

    // Get output aliases names
    select_items
        .iter()
        .filter(|idx| {
            task_graph[**idx].action == QueryAction::Link && task_graph[**idx].alias.is_none()
        })
        .collect::<Vec<&NodeIndex>>()
        .iter()
        .for_each(|idx| {
            let parent = find_parent_with_alias(&task_graph, **idx);
            if parent.is_some() {
                task_graph[**idx].alias = task_graph[parent.unwrap()].alias.clone();
            }
        });

    let select_query = SelectQuery {
        task_graph: task_graph,
        select_items: select_items,
        from_table: from_table,
        where_expr: where_expr,
    };

    Ok(select_query)
}

fn dealias_graph(task_graph: &mut StableDiGraph<QueryTask, usize>) -> Result<(), String> {
    let mut aliases = HashMap::<String, NodeIndex>::new();

    for (node_idx, task) in task_graph.node_references() {
        if task.alias.is_some() {
            aliases.insert(task.alias.as_ref().unwrap().clone(), node_idx);
        }
    }

    if aliases.contains_key("payload") {
        return Err("Error query uses alias \"payload\" which is reserved".to_string());
    }

    let root_node = task_graph
        .node_indices()
        .collect::<Vec<NodeIndex>>()
        .iter()
        .filter(|idx| task_graph[**idx].action == QueryAction::Root)
        .map(|idx| *idx)
        .take(1)
        .collect::<Vec<NodeIndex>>()
        .first()
        .unwrap()
        .clone();

    aliases.insert("payload".to_string(), root_node);

    for idx in task_graph.node_indices().collect::<Vec<NodeIndex>>() {
        match &task_graph[idx].action {
            QueryAction::Accessor(ids) => {
                let id0 = ids.first();
                if id0.is_none() {
                    continue;
                }
                let id0 = id0.unwrap();

                if aliases.contains_key(id0) {
                    let alias = aliases.get(id0).unwrap();

                    if ids.len() == 1 {
                        if !task_graph[idx].required {
                            task_graph[idx].action = QueryAction::Stale;
                            task_graph
                                .edges_directed(idx, petgraph::Direction::Outgoing)
                                .map(|edge| {
                                    return (edge.target(), *edge.weight());
                                })
                                .collect::<Vec<(NodeIndex, usize)>>()
                                .iter()
                                .for_each(|(target, weight)| {
                                    task_graph.add_edge(*alias, *target, *weight);
                                });
                        } else {
                            task_graph[idx].action = QueryAction::Link;
                            task_graph.add_edge(*alias, idx, 1);
                        }
                    } else {
                        task_graph[idx].action = QueryAction::Accessor(
                            ids.iter()
                                .skip(1)
                                .map(|x| x.clone())
                                .collect::<Vec<String>>(),
                        );
                        task_graph.add_edge(*alias, idx, 1);
                    }
                }
            }
            _ => {}
        }
    }

    // Remove stale nodes
    task_graph
        .node_indices()
        .collect::<Vec<NodeIndex>>()
        .iter()
        .filter(|idx| task_graph[**idx].action == QueryAction::Stale)
        .map(|idx| *idx)
        .collect::<Vec<NodeIndex>>()
        .iter()
        .for_each(|idx| {
            task_graph.remove_node(*idx);
        });

    Ok(())
}

fn parse_and_compile_sql(query: String) -> Result<Vec<QueryType>, String> {
    let dialect = CustomDialect {};
    let ast = Parser::parse_sql(&dialect, query.as_str()).unwrap_or_default();

    let mut statements = Vec::<QueryType>::new();

    for stmt in ast {
        let parsed_statement: QueryType = match stmt {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(select) => {
                    let query = compile_select_query(select)?;
                    Ok(QueryType::SELECT(query))
                }
                _ => Err("Not implemented".to_string()),
            },
            _ => Err("Not implemented".to_string()),
        }?;

        statements.push(parsed_statement);
    }

    Ok(statements)
}

fn graph_populate_context(task_graph: &mut StableDiGraph<QueryTask, usize>) -> Result<(), String> {
    // TODO add function support
        for idx in task_graph.node_indices().collect::<Vec<NodeIndex>>() {
        if task_graph[idx].action == QueryAction::Finalize {
            continue;
        }

        let required_context_len: Option<usize> =  match task_graph[idx].action {
            QueryAction::Accessor(_) => Some(1),
            QueryAction::Link => Some(1),
            QueryAction::UnaryOp(_) => Some(1),
            QueryAction::BinaryOp(_) => Some(2),
            QueryAction::Function(_) => None,
            _ => Some(0),
        };

        let mut edges = task_graph.edges_directed(idx, petgraph::Direction::Incoming)
        .collect::<Vec<_>>();

        if edges.len() != required_context_len.unwrap_or(edges.len()) {
            return Err(format!("Error missing required context for node: ({:?})", task_graph[idx]));
        }

        // Verify edges are consecutive weights up to argument max
        edges.sort_by(|a, b| a.weight().cmp(b.weight()));
        let weight_mismatches = edges.iter().enumerate().filter(|(a,b)| a + 1 != *b.weight()).count();
        if weight_mismatches > 0 {
            return Err("Weight mismatch".to_string());
        }

        let source_indexes = edges.iter().map(|x| x.source()).collect::<Vec<NodeIndex>>();

        match task_graph[idx].action {
            QueryAction::Accessor(_) => {
                task_graph[idx].context = Some(QueryContext::SingleParent(source_indexes[0]));
            },
            QueryAction::Link => {
                task_graph[idx].context = Some(QueryContext::SingleParent(source_indexes[0]));
            },
            QueryAction::UnaryOp(_) => {
                task_graph[idx].context = Some(QueryContext::SingleParent(source_indexes[0]));
            },
            QueryAction::BinaryOp(_) => {
                task_graph[idx].context = Some(QueryContext::DualParent(source_indexes[0], source_indexes[1]));
            },
            QueryAction::Function(_) => {
                task_graph[idx].context = Some(QueryContext::MultiParent(source_indexes));
            }
            _ => {}
        }
    }

    let missing_context = task_graph.node_weights().map(|task| -> Result<(), String> {
            match &task.action {
                QueryAction::Accessor(ids) => {
                    if task.context.is_none() {
                        Err(ids.join("."))
                    } else {
                        Ok(())
                    }
                },
                _ => Ok(())
            }
        }
    ).filter(|x| x.is_err()).map(|x| x.unwrap_err()).collect::<Vec<_>>();


    if missing_context.len() > 0 {
        return Err(format!("Invalid accesses in query nodes: ({})", missing_context.join("), (")));
    }

    Ok(())
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

fn execute_query(json_values : &mut Vec<serde_json::Value>, tasks : &Vec<(NodeIndex,QueryTask)>) -> Result<(), String> {
    for (idx, task) in tasks {
        match task.context.as_ref().unwrap() {
            QueryContext::SingleParent(parent) => {
                match &task.action {
                    QueryAction::Accessor(ids) => {
                        let mut parent_value = &json_values[parent.index()];
                        for id in ids {
                            parent_value = &parent_value[id];
                        }
                        json_values[idx.index()] = parent_value.clone();
                    },
                    QueryAction::Link => {
                        json_values[idx.index()] = json_values[parent.index()].clone();
                    },
                    QueryAction::UnaryOp(op) => {
                        let res = execute_unary_op(&json_values[parent.index()], op)?;
                        json_values[idx.index()] = res;
                    },
                    _ => {}
                }
            },
            QueryContext::DualParent(parent1, parent2) => {
                let res = match &task.action {
                    QueryAction::BinaryOp(op) => {
                        match (&json_values[parent1.index()], &json_values[parent2.index()]) {
                        (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => execute_binary_op_numeric((n1, n2), op),
                            _ => Err("Other binary ops not implemented".to_string())
                        }
                    },
                    _ => { Err("Other dual parent actions".to_string())}
                }?;

                json_values[idx.index()] = res;

            },
            QueryContext::MultiParent(_parents) => {
            }
        }
    }

    Ok(())
}


pub fn sql_main() {
    let sql_select = "SELECT \n\tpayload.version AS version, \n\tpayload.meta.id AS id, version + 5, \n\tpayload.data.payload AS \"abc\" \nFROM \"/topic\" \nWHERE (version-1) = 0";

    println!("\n\n{}\n", sql_select);

    let _statements = parse_and_compile_sql(sql_select.to_string())
        .map_err(|x| println!("Parse and compile error: {}", x))
        .map(|statements| {
            for stmt in statements {
                match stmt {
                    QueryType::SELECT(mut select_query) => {

                        // println!("{:?}", select_query);
                        // print_graph(&select_query.task_graph);

                        let context_res = graph_populate_context(&mut select_query.task_graph);
                        if context_res.is_err() {
                            println!("Context error: {}", context_res.unwrap_err());
                            continue;
                        }

                        let toposort_res = petgraph::algo::toposort(&select_query.task_graph, None);
                        if toposort_res.is_err() {
                            println!("Cycle in graph {:?}", toposort_res.unwrap_err());
                            break;
                        } 

                        let sorted = toposort_res.unwrap();
                        let mut json_values = vec![serde_json::Value::Null; sorted.iter().max().unwrap().index() + 1];

                        // Initalize literals
                        let literal_init_success = json_values.iter_mut().enumerate().map(|(i, val)| -> Result<(), NodeIndex> {
                            let node_idx = NodeIndex::new(i);
                            if select_query.task_graph.contains_node(node_idx) {
                                match &select_query.task_graph[node_idx].action {
                                    QueryAction::Literal(literal) => {
                                        match literal {
                                            SQLLiteral::Integer(i) => {
                                                *val = serde_json::Value::Number(serde_json::Number::from(*i));
                                            },
                                            SQLLiteral::Float(f) => {
                                                let number = serde_json::Number::from_f64(*f);
                                                if number.is_none()  {
                                                    return Err(node_idx);
                                                }
                                                *val = serde_json::Value::Number(number.unwrap());
                                            },
                                            SQLLiteral::String(s) => {
                                                *val = serde_json::Value::String(s.clone());
                                            }
                                        }
                                    },
                                    _ => {}
                                }
                            }
                            Ok(())
                        });

                        // Literal may fail if floating point is not finite
                        let literal_init_errors : Vec<_> = literal_init_success.filter(|x| x.is_err()).collect();
                        if literal_init_errors.len() > 0 {
                            println!("Error contains non finite floating point literals: {:?}", literal_init_errors);
                            break;
                        }

                        let tasks : Vec<_> = sorted.iter().filter(|&&x| {
                            match select_query.task_graph[x].action {
                                QueryAction::Accessor(_) => true,
                                QueryAction::Link => true,
                                QueryAction::UnaryOp(_) => true,
                                QueryAction::BinaryOp(_) => true,
                                QueryAction::Function(_) => true,
                                _ => false,
                            }
                        }).map(|&idx| (idx, select_query.task_graph[idx].clone()))
                        .collect();

                        let input_data = serde_json::json!({
                            "version": 1,
                            "data": {
                                "payload": [1,2,3,4,5]
                            },
                            "meta": {
                                "id": 2
                            }
                        });

                        println!("{}", serde_json::to_string_pretty(&input_data).unwrap());

                        json_values[0] = input_data;

                        let execute_res = execute_query(&mut json_values, &tasks).map_err(|x| println!("Error executing query {:?}", x));
                        
                        if execute_res.is_err() {
                            println!("Error executing query: {:?}", execute_res.unwrap_err());
                        }


                        println!("\nQuery results:");
                        for (i, select_item) in select_query.select_items.iter().enumerate() {
                            println!("\t{:?} {:?}", i, json_values[select_item.index()]);
                        }
                        println!("\tWHERE: {:?}", json_values[select_query.where_expr.unwrap().index()]);
                    }
                }
            }
        });
}
