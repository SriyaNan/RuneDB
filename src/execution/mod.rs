use crate::parser::Rule;
use crate::structures::{
    ActiveDataBase, AstNode, Attr, Cell, DataType, Database, Operation, Row, TableInfo, TableRow,
    TableSchema,
};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::io::{Seek, SeekFrom};
const PAGE_SIZE: usize = 4096;
use lazy_static::lazy_static;
use std::sync::Mutex;
use std::fs;

lazy_static! {
    pub static ref ACTIVE_DB: Mutex<Option<ActiveDataBase>> = Mutex::new(None);
}

fn db_initialise(name: String) -> Database {
    let db = Database {
        tables: 0,
        name: name,
        table_details: Vec::new(),
    };
    return db;
}

fn table_initialise() -> TableInfo {
    let table_info = TableInfo {
        tables: HashMap::new(),
    };
    return table_info;
}


fn check_validity(row: &Row, attributes: &Vec<Attr>) -> bool {
    if row.cells.len() != attributes.len() {
        println!(
            "Column count mismatch: expected {}, got {}",
            attributes.len(),
            row.cells.len()
        );
        return false;
    } else {
        for (cell, attr) in row.cells.iter().zip(attributes.iter()) {
            let value = &cell.value;

            match attr.datatype {
                DataType::Int => {
                    if value.parse::<i32>().is_err() {
                        println!("Column {} expects Int, got {}", attr.col_name, value);
                        return false;
                    }
                }
                DataType::Bool => {
                    if value.parse::<bool>().is_err() {
                        println!("Column {} expects a boolean, got {}", attr.col_name, value);
                        return false;
                    }
                }
                DataType::String => {
                    return true;
                }
            }
        }
        true
    }
}

fn operation(cell: &Cell, op: Operation, val: String) -> bool {
    let cell_num = cell.value.parse::<i32>();
    let val_num = val.parse::<i32>();

    match (cell_num, val_num) {
        (Ok(cn), Ok(vn)) => match op {
            Operation::Equal => cn == vn,
            Operation::Grt => cn > vn,
            Operation::Les => cn < vn,
            Operation::GrtEq => cn >= vn,
            Operation::LesEq => cn <= vn,
            Operation::NotEq => cn != vn,
        },
        _ => match op {
            Operation::Equal => cell.value == val,
            Operation::NotEq => cell.value != val,
            _ => false,
        },
    }
}

pub fn execute(ast: AstNode) {
    match ast {
        AstNode::MakeRDB { name } => {
            let dir = "Databases";
            fs::create_dir_all(dir).expect("failed to create Databases directory");

            let path = format!("Databases/{}.rdb", name);
            let mut data_file = File::create(&path).expect("creation failed");

            let db = db_initialise(name.clone());
            let db_buf = rmp_serde::to_vec(&db).unwrap();
            data_file.write_all(&db_buf).unwrap();

            data_file.seek(SeekFrom::Start(4096)).unwrap();
            let tbl = table_initialise();
            let tbl_buf = rmp_serde::to_vec(&tbl).unwrap();
            data_file
                .write_all(&tbl_buf)
                .expect("something went wrong with initialising a database");

            print!("New database {} created and selected!\n", name);
            match ActiveDataBase::open(&name) {
                Ok(active) => {
                    let mut db_guard = ACTIVE_DB.lock().unwrap();
                    *db_guard = Some(active);
                }
                Err(_) => {
                    panic!("Problem opening the data file.");
                }
            }
        }

        AstNode::OpenRDB { name } => match ActiveDataBase::open(&name) {
            Ok(active) => {
                let mut db_guard = ACTIVE_DB.lock().unwrap();
                *db_guard = Some(active);
                println!("Opened database: {}", name);
            }
            Err(_) => {
                println!(
                    "Problem opening the data file. Create a new database!\nType 'help' to see how"
                );
            }
        },

        AstNode::MakeTable { name, columns } => {
            let db_guard = ACTIVE_DB.lock().unwrap();
            if let Some(active_db) = &*db_guard {
                println!("Database: {}", active_db.active_db.name);
                let path = format!("Databases/{}.rdb", active_db.active_db.name);

                let mut data_file = File::open(&path).unwrap(); //open databse
                data_file.seek(SeekFrom::Start(0)).unwrap();
                //first read Database structure and update number of tables
                let mut db = vec![0u8; PAGE_SIZE];
                let bytes_read = data_file.read(&mut db).unwrap();
                db.truncate(bytes_read);

                let mut decodeddb: Database = rmp_serde::from_slice(&db).unwrap();

                let number_of_table = decodeddb.tables;

                decodeddb.tables += 1; //update number of tables

                let mut buf = Vec::new();
                data_file.read_to_end(&mut buf).unwrap();
                let mut table_new: TableSchema = TableSchema {
                    name: name.to_string(),
                    attributes: Vec::new(),
                };

                for (col_name, col_type) in columns {
                    //println!("Column: {} Type: {}", col_name, col_type);
                    let datatype = match col_type.to_lowercase().as_str() {
                        "int" => DataType::Int,
                        "string" => DataType::String,
                        "bool" => DataType::Bool,
                        _ => panic!("Unknown data type: {}", col_type),
                    };
                    let new_attr: Attr = Attr {
                        col_name: col_name,
                        datatype: datatype,
                    };
                    table_new.attributes.push(new_attr);
                }

                //update table details
                decodeddb.table_details.push(table_new);

                let mut data_file = OpenOptions::new().write(true).open(&path).unwrap();
                let newentry = rmp_serde::to_vec(&decodeddb).unwrap();
                data_file.write_all(&newentry).expect("write failed");

                //update table information
                let mut data_file = File::open(&path).unwrap();
                data_file.seek(SeekFrom::Start(4096)).unwrap();
                let mut table_info = vec![0u8; PAGE_SIZE];
                let table_bytes = data_file.read(&mut table_info).unwrap();
                table_info.truncate(table_bytes);

                let mut decodedtable: TableInfo = rmp_serde::from_slice(&table_info).unwrap();

                decodedtable.tables.insert(name, number_of_table + 2);

                let mut data_file = OpenOptions::new().write(true).open(&path).unwrap();
                data_file.seek(SeekFrom::Start(4096)).unwrap();

                let table_buf = rmp_serde::to_vec(&decodedtable).unwrap();
                data_file.write_all(&table_buf).expect("write failed");

                println!("New table created");
            } else {
                println!("No database is active.");
            }
        }

        AstNode::Add { table, values } => {
            println!("Insert into table: {}", table);

            let db_guard = ACTIVE_DB.lock().unwrap();
            if let Some(active_db) = &*db_guard {
                println!("Database: {}", active_db.active_db.name);

                let path = format!("Databases/{}.rdb", active_db.active_db.name);
                let mut data_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();

                data_file.seek(SeekFrom::Start(0)).unwrap();
                let mut db_page = vec![0u8; PAGE_SIZE];
                let bytes_read = data_file.read(&mut db_page).unwrap();
                db_page.truncate(bytes_read);
                let decodeddb: Database = rmp_serde::from_slice(&db_page).unwrap();

                data_file.seek(SeekFrom::Start(4096)).unwrap();
                let mut table_page = vec![0u8; PAGE_SIZE];
                let table_bytes = data_file.read(&mut table_page).unwrap();
                table_page.truncate(table_bytes);
                let decodedtable: TableInfo = rmp_serde::from_slice(&table_page).unwrap();

                if let Some(&pointer) = decodedtable.tables.get(&table) {
                    let table_offset = (pointer as u64) * 4096;
                    data_file.seek(SeekFrom::Start(table_offset)).unwrap();
                    let mut rows_bytes = vec![0u8; PAGE_SIZE];
                    let n = data_file.read(&mut rows_bytes).unwrap();
                    rows_bytes.truncate(n);

                    let mut all_rows: TableRow = if n == 0 {
                        TableRow { rows: Vec::new() }
                    } else {
                        match rmp_serde::from_slice(&rows_bytes) {
                            Ok(r) => r,
                            Err(_) => TableRow { rows: Vec::new() },
                        }
                    };

                    let mut new_row = Row { cells: Vec::new() };
                    for val in values {
                        new_row.cells.push(Cell { value: val });
                    }

                    let details = &decodeddb.table_details;
                    if let Some(schema) = details.iter().find(|t| t.name == table) {
                        let attributes = &schema.attributes;

                        // for attr in attributes {
                        //     println!("{} ({:?})", attr.col_name, attr.datatype);
                        // }

                        if check_validity(&new_row, attributes) {
                            all_rows.rows.push(new_row);
                            let updated_bytes = rmp_serde::to_vec(&all_rows).unwrap();

                            data_file.seek(SeekFrom::Start(table_offset)).unwrap();
                            data_file
                                .set_len(table_offset + updated_bytes.len() as u64)
                                .unwrap();
                            data_file.write_all(&updated_bytes).expect("write failed");
                            println!("1 Row added");
                        } else {
                            println!("Datatype mismatch, Row not added");
                        }
                    } else {
                        panic!("Table {} not found in table_details", table);
                    }
                } else {
                    panic!("Table not available, create a table before adding records!");
                }
            } else {
                println!("No database is active.");
            }
        }

        AstNode::Pick { table, columns } => {
            let db_guard = ACTIVE_DB.lock().unwrap();
            if let Some(active_db) = &*db_guard {
                println!("Database: {}\n", active_db.active_db.name);
                println!("Pick from table: {}", table);

                let path = format!("Databases/{}.rdb", active_db.active_db.name);
                let mut data_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                data_file.seek(SeekFrom::Start(0)).unwrap();
                let mut db_page = vec![0u8; PAGE_SIZE];
                let bytes_read = data_file.read(&mut db_page).unwrap();
                db_page.truncate(bytes_read);
                let decodeddb: Database = rmp_serde::from_slice(&db_page).unwrap();
                let alltables = decodeddb.table_details;

                let mut i: i32 = -1;
                for k in alltables.iter() {
                    i += 1;
                    if k.name == table {
                        break;
                    }
                }

                let mut indexes: Vec<usize> = Vec::new();

                if i == -1 {
                    print!("Table not found")
                } else {
                    if let Some(table_picked) = alltables.get((i) as usize) {
                        for (index, attribute) in table_picked.attributes.iter().enumerate() {
                            if columns.contains(&attribute.col_name) {
                                indexes.push(index);
                            }
                        }
                    } else {
                        println!("No table found at index {}", i);
                    }
                }

                data_file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                let mut table_page = vec![0u8; PAGE_SIZE];
                let table_bytes = data_file.read(&mut table_page).unwrap();
                table_page.truncate(table_bytes);
                let decodedtable: TableInfo = rmp_serde::from_slice(&table_page).unwrap();
                let pointer = decodedtable.tables[&table];

                let table_offset = (pointer as u64) * 4096;
                data_file.seek(SeekFrom::Start(table_offset)).unwrap();
                let mut rows_bytes = vec![0u8; PAGE_SIZE];
                let n = data_file.read(&mut rows_bytes).unwrap();
                rows_bytes.truncate(n);
                let table_rows: TableRow = rmp_serde::from_slice(&rows_bytes).unwrap();

                for name in &columns {
                    print!("{:<10}", name);
                }
                println!();
                println!("{}", "-".repeat(columns.len() * 10));
                for (_index, element) in table_rows.rows.iter().enumerate() {
                    for &col in &indexes {
                        if let Some(cell) = element.cells.get(col) {
                            print!("{:<10}", cell.value);
                        }
                    }
                    println!();
                }
            } else {
                println!("No database is active.");
            }
        }

        AstNode::ConditionalPick {
            table,
            columns,
            att,
            oper,
            val,
        } => {
            println!("Pick from table: {}", table);
          
            let db_guard = ACTIVE_DB.lock().unwrap();
            if let Some(active_db) = &*db_guard {
                println!("Database: {}", active_db.active_db.name);

                let path = format!("Databases/{}.rdb", active_db.active_db.name);
                let mut data_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                data_file.seek(SeekFrom::Start(0)).unwrap();
                let mut db_page = vec![0u8; PAGE_SIZE];
                let bytes_read = data_file.read(&mut db_page).unwrap();
                db_page.truncate(bytes_read);
                let decodeddb: Database = rmp_serde::from_slice(&db_page).unwrap();
                let alltables = decodeddb.table_details;

                let mut i: i32 = -1;
                for k in alltables.iter() {
                    i += 1;
                    if k.name == table {
                        break;
                    }
                }

                let mut indexes: Vec<usize> = Vec::new();
                let mut cond: Vec<usize> = Vec::new();

                if i == -1 {
                    print!("Table not found")
                } else {
                    if let Some(table_picked) = alltables.get((i) as usize) {
                        for (index, attribute) in table_picked.attributes.iter().enumerate() {
                            if columns.contains(&attribute.col_name) {
                                indexes.push(index);
                            }
                        }
                    } else {
                        println!("No table found at index {}", i);
                    }

                    if let Some(table_picked) = alltables.get((i) as usize) {
                        for (index, attribute) in table_picked.attributes.iter().enumerate() {
                            if att.contains(&attribute.col_name) {
                                cond.push(index);
                            }
                        }
                    } else {
                        println!("No table found at index {}", i);
                    }
                }

                data_file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap();
                let mut table_page = vec![0u8; PAGE_SIZE];
                let table_bytes = data_file.read(&mut table_page).unwrap();
                table_page.truncate(table_bytes);
                let decodedtable: TableInfo = rmp_serde::from_slice(&table_page).unwrap();
                let pointer = decodedtable.tables[&table];

                let table_offset = (pointer as u64) * 4096;
                data_file.seek(SeekFrom::Start(table_offset)).unwrap();
                let mut rows_bytes = vec![0u8; PAGE_SIZE];
                let n = data_file.read(&mut rows_bytes).unwrap();
                rows_bytes.truncate(n);
                let table_rows: TableRow = rmp_serde::from_slice(&rows_bytes).unwrap();

                let mut table_rows_cond: Vec<Row> = Vec::new();

                for element in &table_rows.rows {
                    let mut satisfies_all = true;

                    for (cond_idx, &col) in cond.iter().enumerate() {
                        if let Some(c) = element.cells.get(col) {
                            let op = &oper[cond_idx];
                            let oper: Operation = match op.as_str() {
                                "==" => Operation::Equal,
                                ">" => Operation::Grt,
                                "<" => Operation::Les,
                                ">=" => Operation::GrtEq,
                                "<=" => Operation::LesEq,
                                "!=" => Operation::NotEq,
                                _ => panic!("Unknown operation"),
                            };

                            let v = &val[cond_idx];
                            if !operation(&c, oper, v.clone()) {
                                satisfies_all = false;
                                break;
                            }
                        }
                    }

                    if satisfies_all {
                        let mut currcells: Vec<Cell> = Vec::new();
                        for &col in &indexes {
                            if let Some(c) = element.cells.get(col) {
                                currcells.push(Cell {
                                    value: c.value.clone(),
                                });
                            }
                        }
                        table_rows_cond.push(Row { cells: currcells });
                    }
                }

                for name in &columns {
                    print!("{:<10}", name);
                }
                println!();
                println!("{}", "-".repeat(columns.len() * 10));
                for element in &table_rows_cond {
                    for c in &element.cells {
                        print!("{:<10}", c.value);
                    }
                    println!();
                }
            } else {
                println!("No database is active.");
            }
        }
    }
}

pub fn build_ast(pair: pest::iterators::Pair<Rule>) -> AstNode {
    match pair.as_rule() {
        Rule::make_rdb => {
            let mut inner = pair.into_inner();
            let name_db = inner.next().expect("expected dbname").as_str().to_string();
            AstNode::MakeRDB { name: name_db }
        }

        Rule::open_rdb => {
            let mut inner = pair.into_inner();
            let name_db = inner.next().expect("expected dbname").as_str().to_string();
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

            AstNode::MakeTable {
                name,
                columns: cols,
            }
        }

        Rule::add => {
            let mut inner_rules = pair.into_inner();
            let table = inner_rules.next().unwrap().as_str().to_string();
            let assignments_pair = inner_rules.next().unwrap();

            let mut assignments = Vec::new();
            for assign in assignments_pair.into_inner() {
                if assign.as_rule() == Rule::assignment {
                    let mut parts = assign.into_inner();
                    let value = parts.next().unwrap().as_str().to_string();
                    assignments.push(value);
                }
            }
            AstNode::Add {
                table,
                values: assignments,
            }
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
            AstNode::Pick {
                table,
                columns: picked,
            }
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
            let mut attri: Vec<String> = Vec::new();
            let mut oper = Vec::new();
            let mut values = Vec::new();

            for cond in which_cond.into_inner() {
                if cond.as_rule() == Rule::cond {
                    let mut parts = cond.into_inner();
                    let attr = parts.next().unwrap().as_str().to_string();
                    let op = parts.next().unwrap().as_str().to_string();
                    let val = parts.next().unwrap().as_str().to_string();
                    attri.push(attr);
                    oper.push(op);
                    values.push(val);
                }
            }
            AstNode::ConditionalPick {
                table,
                columns: picked,
                att: attri,
                oper: oper,
                val: values,
            }
        }
        _ => unimplemented!(),
    }
}
