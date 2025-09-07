use serde::{Serialize, Deserialize};
#[derive(Debug)]
pub enum AstNode {
    MakeRDB {name: String},
    MakeTable { name: String, columns: Vec<(String, String)> },
    Put { table: String, values: Vec<(String, String)> },
    Pick { table: String, columns: Vec<String> },
    ConditionalPick { table: String, columns: Vec<String>, condition: Vec<(String,String,String)> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database{
    pub tables: i32,
    pub name: String,
    pub table_details: Vec<TableSchema>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema{
    name: String,
    attributes: Vec<Attr>,
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