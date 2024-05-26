use std::collections::HashMap;

use petgraph::{graph::NodeIndex, stable_graph::StableDiGraph, visit::{EdgeRef, IntoNodeReferences}};
use sqlparser::ast::{Expr, SelectItem};

use super::types::{QueryTask, SQLLiteral, TaskAction, TaskContext};

pub fn add_select_item(
    task_graph: &mut StableDiGraph<QueryTask, usize>,
    item: SelectItem,
) -> Result<NodeIndex, String> {
    match item {
        SelectItem::UnnamedExpr(expr) => add_expr(task_graph, expr, None),
        SelectItem::ExprWithAlias { expr, alias } => {
            add_expr(task_graph, expr, Some(alias.value))
        }
        _ => Err("Expected unnamed expression".to_string()),
    }
}

pub fn add_expr(
    task_graph: &mut StableDiGraph<QueryTask, usize>,
    expr: Expr,
    alias: Option<String>,
) -> Result<NodeIndex, String> {
    match expr {
        Expr::Identifier(id) => {
            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: TaskAction::Accessor(Vec::from([id.to_string()])),
                required: false,
                context: None,
            });
            Ok(node)
        }
        Expr::CompoundIdentifier(ids) => {
            let multi_accessor = ids.iter().map(|x| x.to_string()).collect::<Vec<String>>();
            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: TaskAction::Accessor(multi_accessor),
                required: false,
                context: None,
            });
            Ok(node)
        }
        Expr::UnaryOp { op, expr } => {
            let parent_node = add_expr(task_graph, *expr, None)?;
            let child_node = task_graph.add_node(QueryTask {
                alias: alias,
                action: TaskAction::UnaryOp(op),
                required: false,
                context: None,
            });
            task_graph.add_edge(parent_node, child_node, 1);
            Ok(child_node)
        }
        Expr::BinaryOp { left, op, right } => {
            let left_node = add_expr(task_graph, *left, None)?;
            let right_node = add_expr(task_graph, *right, None)?;
            let child_node = task_graph.add_node(QueryTask {
                alias: alias,
                action: TaskAction::BinaryOp(op),
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
                action: TaskAction::Literal(sql_literal),
                required: false,
                context: None,
            });
            Ok(node)
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(str)) => {
            let node = task_graph.add_node(QueryTask {
                alias: alias,
                action: TaskAction::Literal(SQLLiteral::String(str)),
                required: false,
                context: None,
            });

            Ok(node)
        },
        Expr::Nested(nested_expr) => {
            add_expr(task_graph, *nested_expr, alias)
        },
        _ => Err(format!("Unhandled expression type: {:?}", expr)),
    }
}

pub fn dealias(task_graph: &mut StableDiGraph<QueryTask, usize>) -> Result<(), String> {
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
        .filter(|idx| task_graph[**idx].action == TaskAction::Root)
        .map(|idx| *idx)
        .take(1)
        .collect::<Vec<NodeIndex>>()
        .first()
        .unwrap()
        .clone();

    aliases.insert("payload".to_string(), root_node);

    for idx in task_graph.node_indices().collect::<Vec<NodeIndex>>() {
        match &task_graph[idx].action {
            TaskAction::Accessor(ids) => {
                let id0 = ids.first();
                if id0.is_none() {
                    continue;
                }
                let id0 = id0.unwrap();

                if aliases.contains_key(id0) {
                    let alias = aliases.get(id0).unwrap();

                    if ids.len() == 1 {
                        if !task_graph[idx].required {
                            task_graph[idx].action = TaskAction::Stale;
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
                            task_graph[idx].action = TaskAction::Link;
                            task_graph.add_edge(*alias, idx, 1);
                        }
                    } else {
                        task_graph[idx].action = TaskAction::Accessor(
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
        .filter(|idx| task_graph[**idx].action == TaskAction::Stale)
        .map(|idx| *idx)
        .collect::<Vec<NodeIndex>>()
        .iter()
        .for_each(|idx| {
            task_graph.remove_node(*idx);
        });

    Ok(())
}


/// Traverses the given directed graph upward from the specified node index (`idx`),
/// searching for the nearest ancestor with a non-empty alias or a non-Link action.
/// Returns the index of the found ancestor, or `None` if no such ancestor is found.
pub fn find_parent_with_alias(
    task_graph: &StableDiGraph<QueryTask, usize>,
    idx: NodeIndex,
) -> Option<NodeIndex> {
    let parent = *task_graph
        .neighbors_directed(idx, petgraph::Direction::Incoming)
        .collect::<Vec<NodeIndex>>()
        .first()
        .unwrap();
    let parent_alias = &task_graph[parent].alias;

    if parent_alias.is_none() && task_graph[parent].action == TaskAction::Link {
        find_parent_with_alias(task_graph, parent)
    } else {
        Some(parent)
    }
}

pub fn populate_context(task_graph: &mut StableDiGraph<QueryTask, usize>) -> Result<(), String> {
    // TODO add function support
        for idx in task_graph.node_indices().collect::<Vec<NodeIndex>>() {
        if task_graph[idx].action == TaskAction::Finalize {
            continue;
        }

        let required_context_len: Option<usize> =  match task_graph[idx].action {
            TaskAction::Accessor(_) => Some(1),
            TaskAction::Link => Some(1),
            TaskAction::UnaryOp(_) => Some(1),
            TaskAction::BinaryOp(_) => Some(2),
            TaskAction::_Function(_) => None,
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
            TaskAction::Accessor(_) => {
                task_graph[idx].context = Some(TaskContext::SingleParent(source_indexes[0]));
            },
            TaskAction::Link => {
                task_graph[idx].context = Some(TaskContext::SingleParent(source_indexes[0]));
            },
            TaskAction::UnaryOp(_) => {
                task_graph[idx].context = Some(TaskContext::SingleParent(source_indexes[0]));
            },
            TaskAction::BinaryOp(_) => {
                task_graph[idx].context = Some(TaskContext::DualParent(source_indexes[0], source_indexes[1]));
            },
            TaskAction::_Function(_) => {
                task_graph[idx].context = Some(TaskContext::MultiParent(source_indexes));
            }
            _ => {}
        }
    }

    let missing_context = task_graph.node_weights().map(|task| -> Result<(), String> {
            match &task.action {
                TaskAction::Accessor(ids) => {
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

pub fn toposort(task_graph: &StableDiGraph<QueryTask, usize>) -> Result<Vec<NodeIndex>, String> {
    let sorted = petgraph::algo::toposort(task_graph, None);
    if sorted.is_err() {
        return Err("Error sorting graph".to_string());
    }

    Ok(sorted.unwrap())
}