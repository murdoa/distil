use petgraph::graph::NodeIndex;
use sqlparser::ast::{SetExpr, Statement};
use super::graph;
use super::types::{QuerySelect, QueryType};
use super::{types::{Query, QueryTask, TaskAction}, sqlparser_helper::get_table_name};

pub fn build_select_query(stmt: Statement) -> Result<Query, String> { 
    let mut query = Query::new(stmt);
    let task_graph = &mut query.task_graph;

    let _root_node = task_graph.add_node(QueryTask {
        alias: None,
        action: TaskAction::Root,
        required: true,
        context: None,
    });

    let final_node = task_graph.add_node(QueryTask {
        alias: None,
        action: TaskAction::Finalize,
        required: true,
        context: None,
    });

    let mut select_query = match query.sql_stmt {
        Statement::Query(ref sqlquery) => {
            match &*sqlquery.body {
                SetExpr::Select(select_query) => Ok(select_query),
                _ => Err(())
            }
        },
        _ => Err(())
    };

    if select_query.is_err() {
        return Err("build_select_query called on wrong query type".to_string());
    }

    let mut select_query = select_query.unwrap();

    let select_items = select_query
        .projection
        .iter()
        .map(|x| graph::add_select_item(task_graph, x.clone()))
        .collect::<Result<Vec<NodeIndex>, String>>()?;

    let from_table = get_table_name(select_query.from.first().unwrap().clone())?;

    let mut where_expr: Option<NodeIndex> = None;
    if select_query.selection.is_some() {
        where_expr = Some(graph::add_expr(
            task_graph,
            select_query.selection.clone().unwrap(),
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

    graph::dealias(task_graph)?;

    // Get output aliases names
    select_items
        .iter()
        .filter(|idx| {
            task_graph[**idx].action == TaskAction::Link && task_graph[**idx].alias.is_none()
        })
        .collect::<Vec<&NodeIndex>>()
        .iter()
        .for_each(|idx| {
            let parent = graph::find_parent_with_alias(&task_graph, **idx);
            if parent.is_some() {
                task_graph[**idx].alias = task_graph[parent.unwrap()].alias.clone();
            }
        });

    query.query_type = Some(QueryType::SELECT(QuerySelect {
        select_items: select_items,
        from: from_table,
        where_expr: where_expr,
    }));


    // Build execution plan
    query.initalize_execution_context()?;

    Ok(query)
}

