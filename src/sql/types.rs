use std::vec;

use sqlparser::ast::{BinaryOperator, UnaryOperator, Statement};

use petgraph::{
    graph::NodeIndex,
    stable_graph::StableDiGraph,
};

use super::graph;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum SQLLiteral {
    Integer(i64),
    Float(f64),
    String(String),
}


#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum TaskAction {
    Literal(SQLLiteral),
    Link,
    Accessor(Vec<String>),
    UnaryOp(UnaryOperator),
    BinaryOp(BinaryOperator),
    _Function(String),
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
    pub select_items: Vec<NodeIndex>,
    pub from: String,
    pub where_expr: Option<NodeIndex>,
}

// #[derive(Debug)]
// pub struct QueryForeach {
//     arr_expr: NodeIndex,
//     return_items: Option<Vec<NodeIndex>>,
//     when_expr: Option<NodeIndex>,
//     from: String,
//     where_expr: Option<NodeIndex>,
// }


#[derive(Debug)]
pub enum QueryType {
    SELECT(QuerySelect),
    // FOREACH(QueryForeach),
}

#[derive(Debug)]
pub struct Query {
    pub task_graph: StableDiGraph<QueryTask, usize>,
    pub query_type: Option<QueryType>,
    pub sql_stmt : Statement,
    pub tasks: Vec<(NodeIndex, QueryTask)>,
    pub json_context: Vec<serde_json::Value>,
}

impl Query {
    pub fn new(sql_stmt: Statement) -> Self {
        Query {
            task_graph: StableDiGraph::<QueryTask, usize>::new(),
            query_type: None,
            sql_stmt: sql_stmt,
            tasks: vec![],
            json_context: vec![]
        }
    }

    pub fn _from_idx(&self, node_idx : NodeIndex) -> Option<&QueryTask> {
        self.task_graph.node_weight(node_idx)
    }

    pub fn _from_idx_mut(&mut self, node_idx : NodeIndex) -> Option<&mut QueryTask> {
        self.task_graph.node_weight_mut(node_idx)
    }

    pub fn initalize_execution_context(&mut self) -> Result<(), String> {

        graph::populate_context(&mut self.task_graph)?;
        let task_order = graph::toposort(&self.task_graph)?;

        let mut json_context = vec![serde_json::Value::Null; task_order.iter().max().unwrap().index() + 1];

        // Initialize literals
        let literal_init_success = json_context.iter_mut().enumerate().map(|(i, val)| -> Result<(), NodeIndex> {
            let node_idx = NodeIndex::new(i);
            if self.task_graph.contains_node(node_idx) {
                match &self.task_graph[node_idx].action {
                    TaskAction::Literal(literal) => {
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
            return Err(format!("Contains non finite floating point literals: {:?}", literal_init_errors));
        }

        let tasks : Vec<_> = task_order.iter().filter(|&&x| {
            match self.task_graph[x].action {
                TaskAction::Accessor(_) => true,
                TaskAction::Link => true,
                TaskAction::UnaryOp(_) => true,
                TaskAction::BinaryOp(_) => true,
                TaskAction::_Function(_) => true,
                _ => false,
            }
        }).map(|&idx| (idx, self.task_graph[idx].clone()))
        .collect();


        self.tasks = tasks;
        self.json_context = json_context;
        Ok(())
    }

    pub fn get_results(&mut self) -> Result<(Vec<(String, serde_json::Value)>, Option<serde_json::Value>), String> {
        match &self.query_type {
            Some(QueryType::SELECT(select_query)) => {
                let select_items = &select_query.select_items;
                let json_context = &self.json_context;
                let result_tasks = select_items.iter().map(|x| {
                    let task = &self.task_graph[*x];
                    let alias = task.alias.clone().unwrap_or("".to_string());
                    let value = json_context[x.index()].clone();
                    (alias, value)
                }).collect::<Vec<_>>();

                let conditional = match &select_query.where_expr {
                    Some(idx) => {
                        let value = json_context[idx.index()].clone();
                        Some(value)
                    },
                    None => None
                };


                Ok((result_tasks, conditional))
            },
            _ => { Err("Not a select query".to_string()) }
        }
    }
}

