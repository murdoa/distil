use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use sqlparser::ast::{BinaryOperator, UnaryOperator};



#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum SQLLiteral {
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug)]
pub enum QueryFunctions {}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum QueryAction {
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
pub enum QueryContext {
    SingleParent(NodeIndex),
    DualParent(NodeIndex, NodeIndex),
    MultiParent(Vec<NodeIndex>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryTask {
    pub alias: Option<String>,
    pub action: QueryAction,
    pub required: bool,
    pub context: Option<QueryContext>,
}

#[derive(Debug)]
pub struct SelectQuery {
    task_graph: StableDiGraph<QueryTask, usize>,
    select_items: Vec<NodeIndex>,
    from_table: String,
    where_expr: Option<NodeIndex>,
}//

#[derive(Debug)]
pub enum QueryType {
    SELECT(SelectQuery),
}