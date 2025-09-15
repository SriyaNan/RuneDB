use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{Write, Read};
use rmp_serde::{encode, decode};
use rmp_serde::{from_slice, to_vec};
use std::collections::HashMap;
#[derive(Debug)]
pub enum AstNode {
    MakeRDB {name: String},
    OpenRDB{name: String},
    MakeTable { name: String, columns: Vec<(String, String)> },
    Add { table: String, values: Vec<String> },
    Pick { table: String, columns: Vec<String> },
    ConditionalPick { table: String, columns: Vec<String>, condition: Vec<(String,String,String)> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct table_info{
    pub tables: HashMap<String, i32> //this tells the table number which can be used to find the page number (number*4096)
}

#[derive(Serialize, Deserialize)]
pub struct tableRow {
    pub rows: Vec<row>,
}

#[derive(Serialize, Deserialize)]
pub struct row {
    pub cells: Vec<cell>,
}

#[derive(Serialize, Deserialize)]
pub struct cell {
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database{
    pub tables: i32,
    pub name: String,
    pub table_details: Vec<TableSchema>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema{
    pub name: String,
    pub attributes: Vec<Attr>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attr{
    pub col_name: String,
    pub datatype: DataType,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DataType{
    Int,
    String, 
    Bool, 
}

// use std::collections::HashMap;

// const PAGE_SIZE: usize= 4096;
// #[derive(Debug)]
// pub struct Page{
//     pub id: u32,
//     pub data: [u8; PAGE_SIZE],
// }

// #[derive(Debug,Serialize, Deserialize)]
// pub struct LookUp{
    
// }

// #[derive(Debug)]
// pub struct Hashmap{
//     map: HashMap<u32,u64>,
// }

#[derive(Debug)]
pub struct ActiveDataBase{
     pub path: String,
     pub active_db: Database,
}

impl ActiveDataBase{
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