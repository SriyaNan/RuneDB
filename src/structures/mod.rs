use rmp_serde::{from_slice};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read};
#[derive(Debug)]
pub enum AstNode {
    MakeRDB {
        name: String,
    },
    OpenRDB {
        name: String,
    },
    MakeTable {
        name: String,
        columns: Vec<(String, String)>,
    },
    Add {
        table: String,
        values: Vec<String>,
    },
    Pick {
        table: String,
        columns: Vec<String>,
    },
    ConditionalPick {
        table: String,
        columns: Vec<String>,
        att: Vec<String>,
        oper: Vec<String>,
        val: Vec<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub tables: HashMap<String, i32>, //this tells the table number which can be used to find the page number (number*4096)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableRow {
    pub rows: Vec<Row>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    pub cells: Vec<Cell>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Cell {
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub tables: i32,
    pub name: String,
    pub table_details: Vec<TableSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub attributes: Vec<Attr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attr {
    pub col_name: String,
    pub datatype: DataType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DataType {
    Int,
    String,
    Bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Equal,
    Grt,
    Les,
    GrtEq,
    LesEq,
    NotEq,
}

#[derive(Debug)]
pub struct ActiveDataBase {
    pub path: String,
    pub active_db: Database,
}

impl ActiveDataBase {
    pub fn open(name: &str) -> std::io::Result<Self> {
        let path = format!("Databases/{}.rdb", name);
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let db: Database = from_slice(&buf).expect("Failed to decode DB file");

        Ok(Self {
            path: name.to_string(),
            active_db: db,
        })
    }
}
