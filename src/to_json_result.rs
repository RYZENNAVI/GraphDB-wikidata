use crate::{
    data_types::DataValue, relation::Relation, small_string::SmallString,
    storage_engine::StorageEngine,
};
use serde::Serialize;
use std::collections::HashMap;

/// top-level struct to serialize into this JSON format: https://www.w3.org/TR/sparql11-results-json/
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparqlQueryJson<'a> {
    pub head: SparqlQueryHead<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<SparqlQueryResults<'a>>, // SELECT query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boolean: Option<SmallString<'a>>, // ASK query
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparqlQueryHead<'a> {
    pub vars: Vec<SmallString<'a>>, // variables in the SELECT statement
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<Vec<SmallString<'a>>>, // unused; for future compatibility
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparqlQueryResults<'a> {
    pub bindings: Vec<HashMap<SmallString<'a>, JsonObject<'a>>>, // variable name followed by a RDF term
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonObject<'a> {
    #[serde(rename = "type")]
    pub type_member: SmallString<'a>, // "uri", "literal" or "bnode"
    pub value: SmallString<'a>,
    #[serde(rename = "xml:lang", skip_serializing_if = "Option::is_none")]
    pub lang_tag: Option<SmallString<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datatype: Option<SmallString<'a>>, // IRI
}
/// This function is_null can test, if the value of the Tupel is null 
/// It will be used in function datavalue_to_json_object to check, if the value is null 

pub fn is_null(input: DataValue) -> bool
{
    match input {
        DataValue::Null() => true,
        _ => false // other Datavalue Type
    }
}

/// This function converts internally stored datavalues into the output json format used by the Wikidata Query Service.
/// Returns "JsonObject" structs from data_types.rs

pub fn datavalue_to_json_object(input: DataValue) -> JsonObject {
    JsonObject { 
        type_member: input.get_rdf_type().into(), 
        value: input.get_rdf_value().into(),
        lang_tag: input.get_lang(), 
        datatype: input.get_datatype() 
    }
}


/// converts a relation to a SparqlQueryJason struct
/// needs a relation and the stroage engine to load the data from
pub fn relation_to_json<'a, 'b>(
    relation: &Box<dyn Relation<'a> + 'a>,
    storage: &'b impl StorageEngine<'b>,
) -> SparqlQueryJson<'b> {
    let mut bindings: Vec<HashMap<SmallString, JsonObject>> =
        Vec::with_capacity(relation.get_row_count());
    let vars: Vec<SmallString> = relation
        .get_attributes()
        .into_iter()
        .map(|x| SmallString::new(&x[1..]))
        .collect(); //convert to Smallsting and remove '?'
    for i in 0..relation.get_row_count() {
        let row = relation.get_row(i);
        bindings.push(HashMap::new());        
        for j in 0..vars.len() {

            if (is_null(storage.retrieve_data(row[j]))) {continue;}
            // if the datavalue is the Datavalue::Null() then break this loop, that means this datavalue will not be converted to json 
            bindings[i].insert(
                vars[j].clone(),            
                datavalue_to_json_object(storage.retrieve_data(row[j])),
            );
        }
    }
    let res = SparqlQueryResults { bindings: bindings };
    return SparqlQueryJson {
        head: SparqlQueryHead {
            vars,
            link: None,
        },
        results: Some(res),
        boolean: None,
    };
}
