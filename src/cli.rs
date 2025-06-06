use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};

use crate::calc_engine::{Calculation, CalculationEngine};
use crate::calc_data_types::Operator;
use crate::storage_engine::{Query, StorageEngine};

use crate::ext_string::StringStorage;
use crate::{interpreter, storage_engine::Storage};

use std::mem;

#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct GraphdbArgs {
    ///Folder for database. This is used to store the DB by CreateDB and to load the DB by other modes.
    #[arg(short, long, default_value = "wikidata")]
    pub database_dir: PathBuf,

    #[clap(subcommand)]
    pub run_mode: Option<RunMode>,
}

#[derive(Debug, Subcommand)]
pub enum RunMode {
    ///start a webserver on given port (default: 8005)
    Server(ServerArgs),

    ///run graphdb in CLI mode
    Cli,

    ///Creates a database from a wikidata json export file
    CreateDB(ParserArgs),

    ///Create index. Primarily used to benchmark index creation.
    CreateIndex,
}

#[derive(Debug, Args)]
pub struct ParserArgs {
    ///overwrite file path to load JSON data from
    #[arg(short, long, default_value = "./tests/data/first_5_lines.txt")]
    pub file: PathBuf,
    
    ///if flag is set qualifiers are not stored
    #[arg(short, long)]
    pub no_qualifiers: bool,

    #[arg(short, long, default_value = "false")]
    ///This will create a database that can be mmapped. This option is strictly necessary, if the database will be larger than the available RAM.
    ///Also this option will use a parallel parser that uses num_cores many threads to parse the json file.
    pub mmaped: bool,
    
    ///language codes of label, descriptor, alias.
    ///if not set store all, can be empty for none
    #[arg(short, long, num_args = 0..)]
    pub lang: Option<Vec<String>>,
}

#[derive(Debug, Args)]
pub struct ServerArgs {
    ///overwrite the port the server is running on
    #[arg(short, long, default_value = "8005")]
    pub port: u16,
}

pub fn run_cli_mode<S>(s: Storage<'static,S>)
where S: StringStorage  {
    let calc_engine = Arc::new(Calculation::new(s));

    println!("CLI mode started");
    let mut is_running = true;

    while is_running {
        println!("Enter a SPARQL query or type 'exit' to stop the program: (Press Enter once for new line, twice to execute)");
        let mut user_input = String::new();
        io::stdin().read_line(&mut user_input).unwrap();

        let mut lines = io::stdin().lock().lines();

        while let Some(line) = lines.next() {
            let last_input = line.unwrap();

            if last_input.len() == 0 {
                break;
            }
            if user_input.len() > 0 {
                user_input.push_str("\n");
            }
            user_input.push_str(&last_input);
        }

        let input = user_input.trim();
        match input {
            "exit" => {
                is_running = false;
            }
            query => {
                print!("query: {}", query);
                if let Ok(query_result) = interpreter::InterpreterStruct::str_to_query(&query) {
                    let qep = interpreter::InterpreterStruct::query_to_pattern(&query_result);
                    println!("Ok! This is your Vector-Operator-Tree:\n{:#?}", qep);

                    //create Query-Object
                    let mut query = Query{id: 0, query_plan: qep.clone(), relation: None, storage: &calc_engine.storage};

                    query.id = query.storage.generate_id();

                    calc_engine.execute_query(&mut query);

                    //let res = calc_engine.recursive_qep_traversal(qep,query);

                    let res = mem::replace(&mut query.relation, None).unwrap();
                    res.print();
                } else if let Err(query_result) =
                    interpreter::InterpreterStruct::str_to_query(&query)
                {
                    println!("{}", query_result);
                }

                println!("\n#######################\n");
            }
        }
    }
}
