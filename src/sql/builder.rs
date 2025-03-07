use petgraph::graph::NodeIndex;
use sqlparser::ast::{self, ForeachStatement, GroupByExpr, Query, SetExpr, Statement};
use super::graph;
use super::types::{BuiltQuery, BuiltQueryForeach, QuerySelect};
use super::{types::{BuiltQuerySelect, QueryTask, TaskAction}, sqlparser_helper::get_table_name};

pub fn build_select_query(select_query: Box<ast::Select>, root_alias: String) -> Result<BuiltQuerySelect, String> { 
    let mut query = BuiltQuerySelect::new();
    let task_graph = &mut query.task_graph;

    // ROOT NODE NEEDS IDX 0 - DO NOT MOVE THIS LINE
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

    graph::dealias(task_graph, root_alias)?;

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

    query.query_select = Some(QuerySelect {
        select_items: select_items,
        from: from_table,
        where_expr: where_expr,
    });

    // Build execution plan
    query.initalize_execution_context()?;
    graph::print_graph(&query.task_graph);
    Ok(query)
}

pub fn build_foreach_query(foreach_query: &ForeachStatement) -> Result<BuiltQueryForeach, String> {
    
    let main_body_select = Box::new(ast::Select {
        distinct: None,
        top: None,
        projection: vec![foreach_query.select_item.clone()],
        into: None,
        from: vec![foreach_query.from_table.clone()],
        lateral_views: vec![],
        selection: foreach_query.where_expr.clone(),
        group_by: GroupByExpr::All,
        having: None,
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        named_window: vec![],
        qualify: None
    });

    let return_items_select = match foreach_query.return_items.clone() {
        Some(items) => items,
        None => vec![],
    };
    
    let foreach_select = Box::new(ast::Select {
        distinct: None,
        top: None,
        projection: return_items_select,
        into: None,
        from: vec![foreach_query.from_table.clone()],
        lateral_views: vec![],
        selection: foreach_query.when_expr.clone(),
        group_by: GroupByExpr::All,
        having: None,
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        named_window: vec![],
        qualify: None
    });

    let alias = match &foreach_query.select_item {
        ast::SelectItem::UnnamedExpr(_) => {
            Err("Select item must have an alias".to_string())
        },
        ast::SelectItem::ExprWithAlias { expr, alias } => {
            Ok(alias.value.clone())
        },
        _ => Err("Select item must have an alias".to_string())
    }?;

    let main_built =    build_select_query(main_body_select, "payload".to_string())?;
    let foreach_built = build_select_query(foreach_select, alias)?;

    Ok(BuiltQueryForeach::new(main_built, foreach_built))
}