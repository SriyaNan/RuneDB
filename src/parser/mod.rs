use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "parser/syntax.pest"]
pub struct MyParser;


pub fn parse_input(input: &str) -> pest::iterators::Pair<'_, Rule> {
    MyParser::parse(Rule::main, input)
        .expect("parse failed")
        .next()
        .unwrap()
}
