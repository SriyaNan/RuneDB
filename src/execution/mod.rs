use crate::structures::{AstNode, Database, ActiveDataBase, TableSchema, DataType, Attr, table_info};
use std::collections::HashMap;
use std::fs::File;
use crate::parser::Rule;
use rmp_serde::from_slice;
use std::fs::OpenOptions;
use std::io::{Read, Write};

use std::io::{self, SeekFrom, Seek};

const PAGE_SIZE: usize = 4096;
use std::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ACTIVE_DB: Mutex<Option<ActiveDataBase>> = Mutex::new(None);
}




fn db_initialise(name: String) -> Database{
    let mut db = Database{
        tables : 0,
        name : name,
        table_details : Vec::new(),
    };
    return db;
}

fn table_initialise() -> table_info {
    let table_info = table_info{
        tables: HashMap::new()
    };
    return table_info;
}

pub fn execute(ast: AstNode) {
    match ast {
        AstNode::MakeRDB { name } =>{
            let path = format!("Databases/{}.rdb", name);
            let mut data_file = File::create(&path).expect("creation failed");
            // At offset 0 — Database struct
            let db = db_initialise(name.clone());
            let db_buf = rmp_serde::to_vec(&db).unwrap();
            data_file.write_all(&db_buf).unwrap();

            // At offset 4096 — table_info
            data_file.seek(SeekFrom::Start(4096)).unwrap();
            let tbl = table_initialise(); // empty HashMap
            let tbl_buf = rmp_serde::to_vec(&tbl).unwrap();
            data_file.write_all(&tbl_buf).expect("something went wrong with initialising a database");
            
            print!("New database {} created and selected!\n", name);
            match ActiveDataBase::open(&name) {
                Ok(active) => {
                    let mut db_guard = ACTIVE_DB.lock().unwrap();
                    *db_guard = Some(active);
                    println!("Opened database: {}", name);
                }
                Err(_) => {
                    panic!("Problem opening the data file.");
                }
            }
        }

        AstNode::OpenRDB { name}=>{
            match ActiveDataBase::open(&name) {
                Ok(active) => {
                    let mut db_guard = ACTIVE_DB.lock().unwrap();
                    *db_guard = Some(active);
                    println!("Opened database: {}", name);
                }
                Err(_) => {
                    panic!("Problem opening the data file. Create a new database!\nType 'help' to see how");
                }
            }
        }
        
        AstNode::MakeTable { name, columns } => {
            let db_guard = ACTIVE_DB.lock().unwrap();
            if let Some(active_db) = &*db_guard {
                println!("Currently using DB: {}", active_db.active_db.name);
                let path = format!("Databases/{}.rdb", active_db.active_db.name);

                
                let mut data_file = File::open(&path).unwrap();  //open databse
                data_file.seek(SeekFrom::Start(0)).unwrap();
                //first read Database structure and update number of tables
                let mut db = vec![0u8; PAGE_SIZE];
                let bytes_read = data_file.read(&mut db).unwrap();
                db.truncate(bytes_read);

                let mut decodeddb: Database = rmp_serde::from_slice(&db).unwrap();
                
                let number_of_table = decodeddb.tables;

                decodeddb.tables+=1; //update number of tables
                
                

                let mut buf = Vec::new();
                data_file.read_to_end(&mut buf).unwrap();
                let mut table_new :TableSchema = TableSchema{
                    name: name.to_string(),
                    attributes: Vec::new(),
                };
                
                println!("New table created");

                for (col_name, col_type) in columns {
                    println!("Column: {} Type: {}", col_name, col_type);
                    let datatype = match col_type.to_lowercase().as_str() {
                        "int" => DataType::Int,
                        "string" => DataType::String,
                        "bool" => DataType::Bool,
                        _ => panic!("Unknown data type: {}", col_type),
                    };
                    let new_attr: Attr = Attr {col_name: col_name, datatype: datatype};
                    table_new.attributes.push(new_attr);
                }

                //update table details
                decodeddb.table_details.push(table_new);
                print!("{:#?}", decodeddb);

                let mut data_file = OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap();
                let newentry = rmp_serde::to_vec(&decodeddb).unwrap();
                data_file.write_all(&newentry).expect("write failed");
                print!("Updated structure successfully\n");

                //update table information
                let mut data_file = File::open(&path).unwrap(); 
                data_file.seek(SeekFrom::Start(4096)).unwrap();
                let mut table_info = vec![0u8; PAGE_SIZE];
                let table_bytes = data_file.read(&mut table_info).unwrap();
                table_info.truncate(table_bytes);

                let mut decodedtable: table_info = rmp_serde::from_slice(&table_info).unwrap();

                decodedtable.tables.insert(name, number_of_table+1);

                let mut data_file = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap();
                data_file.seek(SeekFrom::Start(4096)).unwrap();

                let table_buf = rmp_serde::to_vec(&decodedtable).unwrap();
                data_file.write_all(&table_buf).expect("write failed");

                print!("{:#?}", decodedtable);
            } else {
                println!("No database is active.");
            }

            
        }

        AstNode::Put { table, values } => {
            println!("Insert into table: {}", table);
            let db_guard = ACTIVE_DB.lock().unwrap();
            if let Some(active_db) = &*db_guard {
                println!("Currently using DB: {}", active_db.active_db.name);
                for nm in active_db.table_details{

                }
                for (col, val) in values {
                println!("{} = {}", col, val);
            }
            } else {
                println!("No database is active.");
            }
            
        }

        AstNode::Pick { table, columns } => {
            println!("Pick from table: {}", table);
            for col in columns {
                println!("Selected column: {}", col);
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
        let mut cols = Vec::new();

        for attr_pair in inner {
            if attr_pair.as_rule() == Rule::attribute {
                for def in attr_pair.into_inner() {
                    if def.as_rule() == Rule::att_def {
                        let mut parts = def.into_inner();
                        let col_name = parts.next().unwrap().as_str().to_string();
                        let col_type = parts.next().unwrap().as_str().to_string();
                        cols.push((col_name, col_type));
                    }
                }
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

