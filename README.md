<h1 align = center>RuneDB</h1>
<br>
In this fully functional mini database engine made from scratch in Rust, the user can write queries using python-like syntax to store data in a database, just like how real relational database systems like SQLite work internally. This project 
implements a custom query language, paging-based 
storage engine, and an interactive terminal-based interface for running queries — just like using SQL in a CLI.

<h4>Tech Stack</h4> 

![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)

<h4>Why RuneDB?</h4>
This project aims to simplify database development with a focus on ease of use and extensibility. The core features include:<br>
▫️Modular Architecture: Clear separation of parsing, data structures, and execution layers for easy customization.<br>
▫️Interactive CLI: Create databases, define tables, insert data, and perform queries with straightforward commands.<br>
▫️Custom DSL: A domain-specific language for schema design, data insertion, and complex queries.<br>
▫️Serialization & Parsing: Robust handling of data encoding, decoding, and input validation.<br>
▫️Execution Engine: Manages in-memory databases, ensuring data integrity and efficient query execution.<br>

<h4>📌 Key Features</h4>
▫️Custom query language with SQL-like syntax (pick, where, etc.)<br>
▫️Grammar and parsing implemented using pest<br>
▫️Paging-based storage engine (4KB pages) for efficient data management<br>
▫️Table-to-page mapping using a page directory<br>
▫️Attribute schema with support for int, string, and bool data types<br>
▫️Conditional query evaluation (==, !=, <, <=, >, >=)<br>
▫️Tabular formatted output in the terminal<br>
▫️Persistent storage using rmp-serde (MessagePack serialization)<br>

<h4>Get started</h4>
<h5>Prerequisites</h5>
This project requires the following dependencies:<br>
▫️Programming Language: Rust<br>
▫️Package Manager: Cargo<br>

<h5>Installation</h5>
▫️Option 1: Build RuneDB from the source and install dependencies:<br>
Clone the repository<br>
<pre lang="md">git clone https://github.com/SriyaNan/RuneDB</pre>
Navigate to the project directory<br>
<pre lang="md">cd RuneDB</pre>
Using cargo<br>
<pre lang="md">cargo build</pre>
Run the project<br>
<pre lang="md">cargo run</pre>

▫️Option 2: Download from crates.io<br>
<a href="https://crates.io/crates/RuneDB">Go to this link!</a><br>
In your command prompt
<pre lang="md">cargo install RuneDB</pre>
To run it
<pre lang="md">RuneDB</pre>

<h4>Syntax</h4>
▫️Create a new Database: make rdb database-name<br>
▫️Create a table: make table table-name( attr-name : datatype, ... )<br>
  ▫️Insert rows: table-name.add(value, ... )<br>
 ▫️Select columns: table-name.pick( attr-name, ... )<br>
 ▫️Select with condition: table-name.pick( (attr-name, ... ) where ( attr-name = value, ... ) )<br>

<h4>Outcome</h4>
A standalone terminal-based database engine executable that allows users to create, query, and manage tables through a custom syntax.






