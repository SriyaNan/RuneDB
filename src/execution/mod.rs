use crate::structures::{AstNode, Database, ActiveDataBase};
use std::fs::File;
use crate::parser::Rule;
use std::io::Write;

use std::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ACTIVE_DB: Mutex<Option<ActiveDataBase>> = Mutex::new(None);
}


fn db_initialise(name: String) -> Database{
    let db = Database{
        tables : 0,
        name : name,
        table_details : Vec::new(),
    };
    return db;
}

pub fn execute(ast: AstNode) {
    match ast {
        AstNode::MakeRDB { name } =>{
            let path = format!("Databases/{}.rdb", name);
            let mut data_file = File::create(&path).expect("creation failed");
            let new_db = db_initialise(name.clone());
            let buf = rmp_serde::to_vec(&new_db).unwrap();
            data_file.write_all(&buf).expect("write failed");
            print!("New database {} created and selected!\n", name);
        }

        AstNode::OpenRDB { name}=>{
            match ActiveDataBase::open(&name) {
                Ok(active) => {
                    println!("Opened database: {}", active.active_db.name);
                }
                Err(_) => {
                    panic!("Problem opening the data file. Create a new database!\nType 'help' to see how");
                }
            }
        }
        
        AstNode::MakeTable { name, columns } => {
            println!("Created table: {}", name);
            for (col_name, col_type) in columns {
                println!("Column: {} Type: {}", col_name, col_type);
            }
        }

        AstNode::Pick { table, columns } => {
            println!("Pick from table: {}", table);
            for col in columns {
                println!("Selected column: {}", col);
            }
        }
        AstNode::Put { table, values } => {
            println!("Insert into table: {}", table);
            for (col, val) in values {
                println!("{} = {}", col, val);
            }
        }
        AstNode::ConditionalPick { table, columns, condition } => {
            println!("Conditional Pick from: {}", table);
            println!("Columns: {:?}", columns);
            println!("Conditions:");
            for (attr, op, val) in condition {
                println!("{} {} {}", attr, op, val);
            }
        }
    }
}

pub fn build_ast(pair: pest::iterators::Pair<Rule>) -> AstNode {
    match pair.as_rule() {
        Rule::make_rdb => {
        let mut inner = pair.into_inner();
        let name_db = inner
            .next()
            .expect("expected dbname")   // unwrap safely
            .as_str()
            .to_string();
        AstNode::MakeRDB { name: name_db }
        }
        Rule::open_rdb =>{
            let mut inner = pair.into_inner();
            let name_db = inner
                .next()
                .expect("expected dbname")   // unwrap safely
                .as_str()
                .to_string();
            AstNode::OpenRDB { name: (name_db) }
        }

        Rule::make_table => {
            let mut inner = pair.into_inner();
            let name = inner.next().unwrap().as_str().to_string();
            let mut cols = vec![];
            for def in inner {
                if def.as_rule() == Rule::att_def {
                    let mut parts = def.into_inner();
                    let col_name = parts.next().unwrap().as_str().to_string();
                    let col_type = parts.next().unwrap().as_str().to_string();
                    cols.push((col_name, col_type));
                }
            }
            AstNode::MakeTable { name, columns: cols }
        }
        Rule::put => {
            let mut inner_rules = pair.into_inner();
            let table = inner_rules.next().unwrap().as_str().to_string();
            let assignments_pair = inner_rules.next().unwrap();

            let mut assignments = Vec::new();
            for assign in assignments_pair.into_inner() {
                if assign.as_rule() == Rule::assignment {
                    let mut parts = assign.into_inner();
                    let column = parts.next().unwrap().as_str().to_string();
                    let value = parts.next().unwrap().as_str().to_string();
                    assignments.push((column, value));
                }
            }
            AstNode::Put { table, values: assignments }
        }
        Rule::pick => {
            let mut inner_pick = pair.into_inner();
            let table = inner_pick.next().unwrap().as_str().to_string();
            let selectives_pair = inner_pick.next().unwrap();
            let mut picked = vec![];

            for sel in selectives_pair.into_inner() {
                if sel.as_rule() == Rule::selective {
                    picked.push(sel.as_str().to_string());
                }
            }
            AstNode::Pick { table, columns: picked }
        }
        Rule::conditional_pick => {
            let mut inner_cond = pair.into_inner();
            let table = inner_cond.next().unwrap().as_str().to_string();
            let selectives_pair = inner_cond.next().unwrap();
            let mut picked = vec![];

            for sel in selectives_pair.into_inner() {
                if sel.as_rule() == Rule::selective {
                    picked.push(sel.as_str().to_string());
                }
            }

            let which_cond = inner_cond.next().unwrap();
            let mut conditions = Vec::new();
            for cond in which_cond.into_inner() {
                if cond.as_rule() == Rule::cond {
                    let mut parts = cond.into_inner();
                    let attr = parts.next().unwrap().as_str().to_string();
                    let op = parts.next().unwrap().as_str().to_string();
                    let val = parts.next().unwrap().as_str().to_string();
                    conditions.push((attr, op, val));
                }
            }
            AstNode::ConditionalPick { table, columns: picked, condition: conditions }
        }
        _ => unimplemented!(),
    }
}