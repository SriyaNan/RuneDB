mod parser;
use serde::{Serialize, Deserialize};
use crate::structures::{Database, TableSchema, Attr, DataType};
use rmp_serde::{from_slice, to_vec};
use crate::parser::parse_input;
use execution::{execute, build_ast};
use crate::structures::ActiveDataBase;
pub mod execution;
mod structures;
use std::io::{self, Read, Write, SeekFrom, Seek};
use std::sync::Mutex;
use lazy_static::lazy_static;
use std::fs::File;

lazy_static! {
    pub static ref ACTIVE_DB: Mutex<Option<ActiveDataBase>> = Mutex::new(None);
}


fn main() {
    const HELP: &str = "Functionalities ->
create databases, make tables, insert rows, pick rows using syntax similar to Python's

Commands:
  Create a new Database: make rdb <database-name>
  Create a table: make table <table-name>( <attr-name> : datatype, ... )
  Insert rows: <table-name>.add( <attr-name> = value, ... )
  Select columns: <table-name>.pick( <attr-name>, ... )
  Select with condition: <table-name>.pick( <attr-name>, ... ) where ( <attr-name> = value, ... )

Note: nested AND/OR conditions not yet supported.";

    println!("Welcome to RuneDB! Type 'help' for commands, or 'quit' to exit.");

    // loop {
        // print!("db> ");
        // io::stdout().flush().unwrap();

        // let mut input = String::new();
        // io::stdin().read_line(&mut input).unwrap();
        // let input = input.trim();

        // if input.eq_ignore_ascii_case("quit") {
        //     break;
        // }
        // if input.eq_ignore_ascii_case("help") {
        //     println!("{}", HELP);
        //     continue;
        // }

        // let parse_result = parse_input(input);
        // print!("{:#?}", parse_result);
        // let inner_command = parse_result.into_inner().next().unwrap();
        
        // let ast = build_ast(inner_command);
        // print!("{:#?}\n", ast);
        //execute(ast);
        const PAGE_SIZE: usize = 4096;

        let name = "sriya";
        let path = format!("Databases/{}.rdb", name);
        let mut data_file = File::open(&path).unwrap();

        // let mut buffer = [0u8; PAGE_SIZE];

        // data_file.seek(SeekFrom::Start(0)).unwrap();
        // data_file.read(&mut buffer).unwrap();

        // println!("{:?}", &buffer[..64]);
        let mut buf = Vec::new();
        data_file.read_to_end(&mut buf).unwrap();

        let mut new_attr: Attr = Attr { col_name: "sno".to_string(), datatype: DataType::Int };
        let mut table_new :TableSchema = TableSchema{
            name: "sriya".to_string(),
            attributes: Vec::new(),
        };
        table_new.attributes.push(new_attr);

        let mut decoded: Database = from_slice(&buf).unwrap();
        decoded.tables+=1;
        decoded.table_details.push(table_new);
        println!("{:#?}", decoded);

        



    // }
}

