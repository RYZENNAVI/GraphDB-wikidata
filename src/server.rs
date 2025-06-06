use std::collections::HashMap;

use std::sync::Arc;
use warp::{http::Method, http::Response, Filter};

use crate::calc_engine::{Calculation, CalculationEngine};
use crate::ext_string::StringStorage;
use crate::{interpreter, storage_engine};
use crate::storage_engine::{Storage, Query};
use crate::storage_engine::StorageEngine;

use std::mem;

///async function which runs the webserver on the given port
/// # Arguments
/// * `port` - port the server is running on
/// * `s` - StorageEngine
/// # Example
/// ```
/// run_server(8005, s);
/// ```
/// # Panics
/// No custom panics are implemented, but warp might panic if the server is not able to run for example because the port is already in use
pub async fn run_server<S>(port: u16, s: Storage<'static,S>)
where S:StringStorage + 'static {
    // let file_name = "wikidata.graphdb";
    // let file = File::open(file_name).expect("Could not open graphdb File");
    // let mut reader = BufReader::new(file);
    // let s: Storage = bincode::deserialize_from(&mut reader).expect("Could not deserialize graphdb");

    let calc_engine = Arc::new(Calculation::new(s));

    //test s:
    // let x = s.get_nodes_from_edge_id(Node {
    //   id: 1369094286720637555,
    //});
    //x.print();

    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "User-Agent",
            "Sec-Fetch-Mode",
            "Referer",
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
            "Access-Control-Allow-Origin",
            "Content-Type",
            "Accept",
        ])
        .allow_methods(&[
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
            Method::HEAD,
        ]);

    let query_request = warp::get()
        .and(warp::path("query"))
        .and(warp::query::<HashMap<String, String>>())
        .map(move |p: HashMap<String, String>| compute_query(p, calc_engine.clone()))
        .with(cors);

    println!("Starting server on port {}", port);
    warp::serve(query_request).run(([127, 0, 0, 1], port)).await;
}

///function which computes the query and returns the result
/// # Arguments
/// * `p` - HashMap containing the query
/// * `calc_engine` - CalculationEngine
/// # Example
/// ```
/// //p is a HashMap containing parameter of the request and is created by warp
/// let calc_engine = Arc::new(Calculation::new(storage));
///
/// compute_query(p, calc_engine);
/// ```
/// # Panics
/// No custom panics are implemented
/// # Returns
/// Response<String> is a Response containing the result of the query and the status code. Will send it back to the client
fn compute_query<S>(p: HashMap<String, String>, calc_engine: Arc<Calculation<S>>) -> Response<String> 
where S:StringStorage {
    //p should contain a query parameter, if the request is send correctly by the client. If not, the server will return a 400 Bad Request.
    match p.get("query") {
        Some(query_string) => {
            println!("Incoming query: {}", query_string);

            //query_string is a String containing the query. It will be parsed by the interpreter.
            if let Ok(query_result) = interpreter::InterpreterStruct::str_to_query(&query_string) {
                // query_result is a QueryStruct containing the parsed query. It will be converted to a Vector-Operator-Tree (VOT).
                let qep = interpreter::InterpreterStruct::query_to_pattern(&query_result);
                println!("Ok! This is your Vector-Operator-Tree:\n{:#?}", qep);

                //create Query-Object
                let mut query = Query{id: 0, query_plan: qep.clone(), relation: None, storage: &calc_engine.storage};

                query.id = query.storage.generate_id();

                calc_engine.execute_query(&mut query);

                let res = mem::replace(&mut query.relation, None).unwrap();

                //execute the query with the calculation engine and print the result
                //let res = calc_engine.recursive_qep_traversal(qep,query);
                res.print();

                //if the query was executed successfully, the server will return a 200 OK with the result of the query (not implemented yet)
                return Response::builder()
                    .status(200)
                    .header("Content-Type", "application/json")
                    .body(calc_engine.to_json(res))
                    .unwrap();
            } else if let Err(query_result) =
                interpreter::InterpreterStruct::str_to_query(&query_string)
            {
                //In this case, something gone wrong parsing the query. Print Error to the console and return a 400 Bad Request with the error message.
                println!("{}", query_result);
                return Response::builder()
                    .status(400)
                    .body(format!("{} , Query: {}", query_result, query_string))
                    .unwrap();
            }
            //this case should never happen, but if it does, the server will return a 500 Internal Server Error
            return Response::builder()
                .status(500)
                .body(format!("{}", query_string))
                .unwrap();
        }
        //If the client sent a correct request, it should have a query parameter. This is the error handling if this requirement is not met by clients request.
        None => Response::builder()
            .status(400)
            .body(String::from("Bad Request: \"query\" parameter is missing."))
            .unwrap(),
    }
}
