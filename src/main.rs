mod parser;
use crate::parser::parse_input;
use execution::{execute, build_ast};
pub mod execution;
mod structures;
use std::io::{self, Write};
use std::fs::File;


// #[derive(Debug)]
// pub struct Active_DataBase{
//      path: str,

// }

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

    loop {
        print!("db> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        if input.eq_ignore_ascii_case("quit") {
            break;
        }
        if input.eq_ignore_ascii_case("help") {
            println!("{}", HELP);
            continue;
        }

        let parse_result = parse_input(input);
        print!("{:#?}", parse_result);
        let inner_command = parse_result.into_inner().next().unwrap();
        
        let ast = build_ast(inner_command);
        print!("{:#?}\n", ast);
        execute(ast);

    }
}

