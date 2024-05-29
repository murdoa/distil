use super::types::QueryResult;


pub fn print_query_result(result : &QueryResult) {
    match result {
        QueryResult::Simple(simple) => {
            println!("SELECT");
            for (key, value) in &simple.result {
                println!("\t{}: {}", key, value);
            }
            if simple.cond.is_some() {
                println!("WHERE");
                println!("\t{}", simple.cond.clone().unwrap());
            }
        }, 
        QueryResult::Nested(nested) => {
            println!("FOREACH RETURN");

            let max_column = nested.result.iter().map(|x| {
                match x {
                    Ok(QueryResult::Simple(simple)) => simple.result.len(),
                    _ => 0
                }
            }).max().unwrap_or(0);


            print!("\t|{:8}", " INDEX");
            (0..max_column).for_each(|i| print!("|{:8}|{:8}", i, " ALIAS"));
            println!("|{:8}|", " COND");

            print!("\t");
            (0..(max_column * 2 + 2) * 9 + 1).for_each(|_| print!("="));
            print!("\n");

            for (i, res) in nested.result.iter().enumerate() {
                print!("\t|{:8}|", i);
                match res {
                    Ok(QueryResult::Simple(simple)) => {
                        for (key, value) in &simple.result {
                            print!("{:8}|{:8}|", format!("{}",value), key);
                        }
                        println!("{:8}|", format!("{}", simple.cond.clone().unwrap_or(serde_json::Value::Null)));
                    },
                    Err(e) => {
                        println!("Error: {}|", e);
                    },
                    _ => {
                        println!("Error: Nested query must return simple result|");
                    }
                }
            }

            if nested.cond.is_some() {
                println!("WHERE");
                println!("\t{}", nested.cond.clone().unwrap());
            }
        }
    }
}