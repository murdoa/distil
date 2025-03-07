use super::types::{QueryAction, QueryContext, QueryTask};

// pub fn sql_count(task: &QueryTask) -> u64 {

//     match &task.context {
//         Some(context) => {
//             match context {
//                 QueryContext::SingleParent(parent) => {
//                     match parent {

//                     }
//                 },
//                 QueryContext::DualParent(_,_) => 2,
//                 QueryContext::MultiParent(vec) => vec.len().unwrap_or(0)
//             }
//         },
//         None => 0
//     }
// }