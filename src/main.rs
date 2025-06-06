use clap::Parser;
use parser::{parse_file, parse_file_parallel};
use std::path::Path;
mod data_types;
mod calc_engine;
mod calc_data_types;
mod interpreter;
mod parser;
mod small_string;
pub mod relation;
mod storage_engine;
use storage_engine::builder::{StorageBuilder, StorageEngineBuilder, ParallelStorageBuilder};
mod to_json_result;

extern crate alloc;
mod ext_string;

mod file_vec;

mod server;

mod cli;
use cli::{GraphdbArgs, ParserArgs};

fn create_database(args: ParserArgs, path: &Path) {
    let language_filter = args.lang.map(|v| v.into_iter().collect());

    match args.mmaped {
        false => {    
            let mut seb = StorageBuilder::new();
            parse_file(&args.file, &mut seb, !args.no_qualifiers, &language_filter);
            let storage = seb.finalize();
            let file = std::fs::File::create(path).unwrap();
            let writer = std::io::BufWriter::new(file);
            bincode::serialize_into(writer, &storage).unwrap();
        },
        true => {
            let mut seb = ParallelStorageBuilder::new(path);
            parse_file_parallel(&args.file, &mut seb, !args.no_qualifiers, &language_filter);
            seb.finalize();
        },
    }
}

fn create_index(path: &Path) {
    let seb = ParallelStorageBuilder::create_index(path);
    seb.finalize();
}

#[tokio::main]
async fn main() {
    let args = GraphdbArgs::parse();

    match args.run_mode {
        Some(cli::RunMode::Cli) => {
            let file = &args.database_dir;
            if file.is_dir() {
                let storage = storage_engine::load_storage_engine(&args.database_dir).unwrap();
                cli::run_cli_mode(storage);
            } else {
                let storage = storage_engine::load_ram_storage_engine(&args.database_dir).unwrap();
                cli::run_cli_mode(storage);
            }
        }
        Some(cli::RunMode::CreateDB(parser_args)) => {
            create_database(parser_args, &args.database_dir);
        }
        Some(cli::RunMode::Server(cli::ServerArgs { port })) => {
            let file = &args.database_dir;
            if file.is_dir() {
                let storage = storage_engine::load_storage_engine(&args.database_dir).unwrap();
                server::run_server(port, storage).await;
            } else {
                let storage = storage_engine::load_ram_storage_engine(&args.database_dir).unwrap();
                server::run_server(port, storage).await;
            }
        }
        Some(cli::RunMode::CreateIndex) => {
            create_index(&args.database_dir);
        }
        None => {
            // let s = load_storage_engine(&args.database_dir).unwrap();
            // server::run_server(8005, s).await;
        }
    }
}
