use petgraph::{
    graph::{NodeIndex},
    stable_graph::StableDiGraph,
};
use sqlparser::ast::{BinaryOperator, UnaryOperator};

use super::types::SQLLiteral;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum TaskAction {
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
pub enum TaskContext {
    SingleParent(NodeIndex),
    DualParent(NodeIndex, NodeIndex),
    MultiParent(Vec<NodeIndex>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryTask {
    pub alias: Option<String>,
    pub action: TaskAction,
    pub required: bool,
    pub context: Option<TaskContext>,
}

#[derive(Debug)]
pub struct QuerySelect {
    select_items: Vec<NodeIndex>,
    from: String,
    where_expr: Option<NodeIndex>,
}

#[derive(Debug)]
pub struct QueryForeach {
    arr_expr: NodeIndex,
    return_items: Option<Vec<NodeIndex>>,
    when_expr: Option<NodeIndex>,
    from: String,
    where_expr: Option<NodeIndex>,
}


#[derive(Debug)]
pub enum QueryType {
    SELECT(QuerySelect),
    FOREACH(QueryForeach),
}

struct Query {
    task_graph: StableDiGraph<QueryTask, usize>,
    query_type: Option<QueryType>,
    sql : String,
}

impl Query {

    pub fn from_idx(&self, node_idx : NodeIndex) -> Option<&QueryTask> {
        self.task_graph.node_weight(node_idx)
    }

    pub fn from_idx_mut(&mut self, node_idx : NodeIndex) -> Option<&mut QueryTask> {
        self.task_graph.node_weight_mut(node_idx)
    }
}

