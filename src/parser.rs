use std::fs::{File};
use std::path::Path;
use crate::storage_engine::builder::{StorageEngineBuilder, ParallelStorageBuilder};
use std::io::{BufRead, BufReader};
use crate::data_types::{DataValue, Time, Quantity, MonolinText, CalendarModel, Coord};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use lazy_static::lazy_static;
use regex::Regex;
use crate::small_string::SmallString;
use rayon::prelude::*;
use serde::de::Error;

impl<'de> Deserialize<'de> for Time {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let time = <WikiTime>::deserialize(deserializer)?;
        let (year, month, day, hour, minute, second) = match_date(&time.time)?;
        Ok(Time {
            year,
            month,
            day,
            hour,
            minute,
            second,
            before: time.before,
            after: time.after,
            precision: time.precision,
            timezone: time.timezone,
            calendarmodel: time.calendarmodel,
        })
    }
}

fn match_date<'de, E: Error>(date_str: &str) -> Result<(i64, u8, u8, u8, u8, u8), E> {
    lazy_static! {
        static ref RE: Regex =
            Regex::new(r"^([+-]\d+)-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$").unwrap();
    }
    let cap = RE
        .captures_iter(date_str)
        .next()
        .ok_or_else(|| E::custom(format!("Could not parse date: {}", date_str)))?;

    Ok((
        cap[1].parse().unwrap(),
        cap[2].parse().unwrap(),
        cap[3].parse().unwrap(),
        cap[4].parse().unwrap(),
        cap[5].parse().unwrap(),
        cap[6].parse().unwrap(),
    )) //cannot fail due to regex structure
}

// structs to deserialise wikidata items
//any string that has special characters for example "\" has to be owned because they need to be changed while parsing 

/// Top level structure of a wikidata item
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WikidataItem<'a> {
    #[serde(borrow)]
    pub id: SmallString<'a>,
    #[serde(borrow)]
    #[serde(rename = "type")]
    pub type_field: SmallString<'a>,
    #[serde(borrow)]
    pub datatype: Option<SmallString<'a>>, // not to be confused with a datatype of a snak; usually this is just "wikibase-item"
    #[serde(borrow)]
    #[serde(with = "tuple_vec_map")]
    pub labels: Vec<(SmallString<'a>, LabDescEntry<'a>)>,
    #[serde(borrow)]
    #[serde(with = "tuple_vec_map")]
    pub descriptions: Vec<(SmallString<'a>, LabDescEntry<'a>)>,
    #[serde(borrow)]
    #[serde(with = "tuple_vec_map")]
    pub aliases: Vec<(SmallString<'a>, Vec<LabDescEntry<'a>>)>,
    #[serde(borrow)]
    #[serde(with = "tuple_vec_map")]
    pub claims: Vec<(SmallString<'a>, Vec<Claim<'a>>)>,
    #[serde(borrow)]
    #[serde(default)]
    #[serde(with = "tuple_vec_map")]
    pub sitelinks: Vec<(SmallString<'a>, Sitelink<'a>)>,
    #[serde(borrow)]
    #[serde(default)]
    #[serde(with = "tuple_vec_map")]
    pub lemmas: Vec<(SmallString<'a>, Vec<LabDescEntry<'a>>)>,
    #[serde(borrow)]
    pub forms: Option<Vec<Form<'a>>>,
    #[serde(borrow)]
    pub senses: Option<Vec<Sense<'a>>>
}

/// labels and descriptions have the same structure
#[derive(Debug, Deserialize)]
pub struct LabDescEntry<'a>{
    #[serde(borrow)]
    pub value: SmallString<'a>
}

/// aliases (unlike labels and descriptions) can have multiple values for each language; unused 
/*
#[derive(Debug, Deserialize)]
pub struct AliasEntry<'a>{
    #[serde(borrow)]
    pub lang_code: SmallString<'a>,
    #[serde(borrow)]
    pub value: Vec<SmallString<'a>>
}
*/



#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Form<'a>{
    #[serde(borrow)]
    pub id: SmallString<'a>,
    #[serde(borrow)]
    pub representations: Vec<LabDescEntry<'a>>,
    #[serde(borrow)]
    pub grammatical_features: Vec<SmallString<'a>>,
    #[serde(borrow)]
    pub claims: HashMap<SmallString<'a>, Vec<Claim<'a>>>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sense<'a>{
    #[serde(borrow)]
    pub id: SmallString<'a>,
    #[serde(borrow)]
    pub glosses: Vec<LabDescEntry<'a>>,
    #[serde(borrow)]
    pub claims: HashMap<SmallString<'a>, Vec<Claim<'a>>>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim<'a> {
    #[serde(borrow)]
    pub mainsnak: Snak<'a>,
    #[serde(borrow)]
    #[serde(rename = "type")]
    pub type_field: SmallString<'a>,    //according to wikidata, this is always "statement"
    #[serde(borrow)]
    pub id: SmallString<'a>,
    #[serde(borrow)]
    pub rank: SmallString<'a>,
    #[serde(borrow)]
    #[serde(default)]
    #[serde(with = "tuple_vec_map")]
    pub qualifiers: Vec<(SmallString<'a>, Vec<Snak<'a>>)>,
    #[serde(borrow)]
    pub references: Option<Vec<Reference<'a>>>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sitelink<'a>{
    #[serde(borrow)]
    pub site: SmallString<'a>,
    #[serde(borrow)]
    pub title: SmallString<'a>,
    #[serde(borrow)]
    pub badges: Vec<SmallString<'a>>,
    #[serde(borrow)]
    pub url: Option<SmallString<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference<'a> {
    #[serde(borrow)]
    pub hash: SmallString<'a>,
    #[serde(borrow)]
    pub snaks: HashMap<SmallString<'a>, Vec<Snak<'a>>>,
    #[serde(borrow)]
    #[serde(rename = "snaks-order")]
    pub order: Vec<SmallString<'a>>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snak<'a>{
    #[serde(borrow)]
    pub snaktype: SmallString<'a>, //"value", "somevalue" or "novalue"
    #[serde(borrow)]
    pub property: SmallString<'a>,
    pub datatype: SnakDatatype,
    #[serde(borrow)]
    pub datavalue: Option<Data<'a>> // what happens to somevalue and novalue nodes (currently data = NULL node)
}


#[derive(Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum SnakDatatype
{
    ExternalId,
    String,
    GeoShape,
    #[serde(rename = "commonsMedia")]
    CommonsMedia,
    Url,
    Math,
    MusicalNotation,
    TabularData,
    GlobeCoordinate,
    WikibaseItem,
    WikibaseProperty,
    WikibaseLexeme,
    WikibaseSense,
    WikibaseForm,
    Quantity,
    Time,
    Monolingualtext,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Data<'a>{
    #[serde(borrow)]
    pub value: SuperDatatype<'a>,
    #[serde(borrow)]
    #[serde(rename = "type")]
    pub field_type: SmallString<'a>
}

// SuperDatatype consists of the superclasses of datatypes that have to be treated differently when they are converted into DataValues
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SuperDatatype<'a>  {
    #[serde(borrow)]
    String(SmallString<'a>),
    #[serde(borrow)]
    Entityid(EntityType<'a>),
    #[serde(borrow)]
    Globecoordinate(WikiCoord<'a>),
    #[serde(borrow)]
    Quantity(Quantity<'a>),
    Time(WikiTime<'a>),
    #[serde(borrow)]
    MonolinText(MonolinText<'a>),
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityType<'a> {
// The following fields are defined in json but unused because the content is redundant.
// Therefore, this is in a comment.
//    #[serde(borrow)]
//    #[serde(rename = "entity-type")]
//    entity_type: SmallString<'a>,
//    #[serde(rename = "numeric-id")]
//    numeric_id: Option<i64>,
    #[serde(borrow)]
    id: SmallString<'a>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WikiCoord<'a>
{
    pub latitude: f32,                  // WGS84 coordinates
    pub longitude: f32,                 // WGS84 coordinates
    #[serde(borrow)]
    pub globe: SmallString<'a>,                // WikiData entity ID describing the globe
    pub precision: Option<f32>,         // saves 8 bytes compared to Option<f64>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WikiTime<'a>
{
    pub time: &'a str,                  // example: "+2013-01-01T00:00:00Z"
    pub timezone: i16,                  // offset from UTC in minutes (can be negative)
    pub before: u8,                                         
    pub after: u8,                      // before and after describe the uncertainty range of timestamp; the unit is given by the precision
    pub precision: u8,                  // 0 = billion years, 1 = 100 million years, ..., 7 = century, 8 = decade, 9 = year, 10 = month, 11 = day, 12 = hour, 13 = minute, 14 = second
    pub calendarmodel: CalendarModel    // given as URI; example: "http:\/\/www.wikidata.org\/entity\/Q1985727"
}


///convert a str to a DataValue::WikidataidQ or DataValue::WikidataidP
fn str_to_wikidataid(input: SmallString) -> DataValue {
    let mut chars = input.chars();
    let first = chars.next().expect("tried to convert empty string to wikidata id");
    let number = chars.as_str().parse::<u64>().unwrap_or(0);
    match first {
        'Q' => DataValue::WikidataidQ(number),
        'P' => DataValue::WikidataidP(number),
        'L' => DataValue::WikidataidL(number),
        _ => DataValue::Null(),
    }
}
/// get the str from SuperDatatype
fn d_get_str<'a>(input: SuperDatatype<'a>) -> SmallString<'a>{
    match input {
        SuperDatatype::String(x) => x,
        _ => panic!("not a string"), //should not happen because this function is only called after datatype is checked
    }
}

fn get_entity_from_iri(iri: &str) -> u64 {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"Q(\d+)$").unwrap();
    }

    RE.captures_iter(iri).next().unwrap().get(1).unwrap().as_str().parse().unwrap()
}

fn super_datatype_to_datavalue<'a>(input: SuperDatatype<'a>) -> DataValue<'a>{
    match input {
        SuperDatatype::Entityid(x) => str_to_wikidataid(x.id),
        SuperDatatype::Globecoordinate(x) => DataValue::Coord({
            Coord {
                latitude: x.latitude,
                longitude: x.longitude,
                globe: get_entity_from_iri(&x.globe),
                precision: x.precision,
            }
        }),
        SuperDatatype::Quantity(x) => DataValue::Quantity(Box::new(x.clone())),
        SuperDatatype::Time(x) => DataValue::Time({
                let (year,month,day, hour, minute, second) = match_date::<serde_json::Error>(x.time).expect("Error while converting string to date");
                
                Time {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    before: x.before,
                    after: x.after,
                    precision: x.precision,
                    timezone: x.timezone,
                    calendarmodel: x.calendarmodel,
                }
            }),
            SuperDatatype::MonolinText(x) => DataValue::MonolinText(Box::new(x.clone())),
        _ => panic!("SuperDatatype was string"), //should not happen because this function is only called after datatype is checked
    }
}

///get Datavalue from Snak; is null if Snak is somevalue or novalue
fn get_datavalue<'a>(input: Snak<'a>) -> DataValue<'a>{
    match input.datavalue{
        Some(x) => {
            match input.datatype{
               
                SnakDatatype::String => DataValue::String(d_get_str(x.value)),
                SnakDatatype::GeoShape => DataValue::GeoShape(d_get_str(x.value)),
                SnakDatatype::CommonsMedia => DataValue::Media(d_get_str(x.value)),
                SnakDatatype::Url => DataValue::Url(d_get_str(x.value)),
                SnakDatatype::Math => DataValue::MathExp(d_get_str(x.value)),
                SnakDatatype::MusicalNotation => DataValue::MusicNotation(d_get_str(x.value)),
                SnakDatatype::TabularData => DataValue::Tabular(d_get_str(x.value)),
                SnakDatatype::GlobeCoordinate => super_datatype_to_datavalue(x.value),
                SnakDatatype::WikibaseItem => super_datatype_to_datavalue(x.value),
                SnakDatatype::WikibaseProperty => super_datatype_to_datavalue(x.value),
                SnakDatatype::WikibaseLexeme => super_datatype_to_datavalue(x.value),
                SnakDatatype::WikibaseSense => super_datatype_to_datavalue(x.value),
                SnakDatatype::WikibaseForm => super_datatype_to_datavalue(x.value),
                SnakDatatype::Quantity => super_datatype_to_datavalue(x.value),
                SnakDatatype::Time => super_datatype_to_datavalue(x.value),
                SnakDatatype::Monolingualtext => super_datatype_to_datavalue(x.value),
                SnakDatatype::ExternalId => DataValue::Identifier(d_get_str(x.value)),
            }
        },
        None => DataValue::Null(),
    }
}

pub fn parse_file<'a>(filepath: &Path, storage: &'a mut impl StorageEngineBuilder, parse_qualifiers: bool, language_filter: &Option<HashSet<String>>) {
    if filepath == Path::new("-") {
        parse(BufReader::new(std::io::stdin()), storage, parse_qualifiers, language_filter);
    } else {
        let file = match File::open(filepath) {
            Ok(file) => file,
            Err(error) => {
                panic!("unable to open file: {error}")
            },
        };
    
        parse(BufReader::new(file), storage, parse_qualifiers, language_filter);
    };
}

pub fn parse_file_parallel(filepath: &Path, storage: &mut ParallelStorageBuilder, parse_qualifiers: bool, language_filter: &Option<HashSet<String>>) {
    if filepath == Path::new("-") {
        parse_parallel(BufReader::new(std::io::stdin()), storage, parse_qualifiers, language_filter);
    } else {
        let file = match File::open(filepath) {
            Ok(file) => file,
            Err(error) => {
                panic!("unable to open file: {error}")
            },
        };
    
        parse_parallel(BufReader::with_capacity(1024*1024, file), storage, parse_qualifiers, language_filter);
    };
}


/// read a file containing multiple wikidata items and store in StorageEngineBuilder
/// qualifiers are only parsed if parse_qualifiers == true
/// language_filter: Some() => store only lables, descriptors, aliases with specified language
///                  None   => store all
pub fn parse<T: std::io::Read>(reader: BufReader<T>, storage: &mut impl StorageEngineBuilder, parse_qualifiers: bool, language_filter: &Option<HashSet<String>>)
{
    for line in reader.lines() {
        parse_line(storage, &line.unwrap(), parse_qualifiers, language_filter);
    }
}

pub fn parse_parallel<'a,T: std::io::Read + Send>(reader: BufReader<T>, storage: &'a mut ParallelStorageBuilder, parse_qualifiers: bool, language_filter: &Option<HashSet<String>>) {
    reader.lines().par_bridge().for_each_init(|| storage.get_storage_builder(), |builder,line| parse_line(builder, &line.unwrap(), parse_qualifiers, language_filter));
}

fn parse_line(storage: &mut impl StorageEngineBuilder, line: &str, parse_qualifiers: bool, language_filter: &Option<HashSet<String>>) {
    if line.len() == 1 { //skip first and last line
        return; 
    }
    let new_len = if line.ends_with(","){ // remove "," at the end of the line if it exists
        line.len() - 1
    } else {
        line.len()
    };
    let item = match serde_json::from_str::<WikidataItem>(&line[0..new_len]) {
        Ok(x) => x,
        Err(e) => { // if deserialistion fails skip line
            println!("Error parsing: {}", e);
            return;
        },
    };
    let current = storage.store_data(str_to_wikidataid(item.id)); //turn item id into node
    //insert labels
    for (lang,label) in item.labels { //insert every entry of hashmap into storage
        if let Some(filter) = language_filter {
            if !filter.contains(lang.as_str()) {
                continue;
            }
        }
        let p = storage.store_data(DataValue::Label(lang));
        let o = storage.store_data(DataValue::String(label.value));
        storage.insert_edge(current, p, o);
    }
    //insert descriptions
    for (lang, description) in item.descriptions {
        if let Some(filter) = language_filter {
            if !filter.contains(lang.as_str()) {
                continue;
            }
        }
        let p = storage.store_data(DataValue::Description(lang));
        let o = storage.store_data(DataValue::String(description.value));
        storage.insert_edge(current, p, o);
    }
    //insert aliases
    for (lang,alias) in item.aliases.into_iter().map(
        |(lang, alias_vec)| alias_vec.into_iter().map(move |alias| (lang.clone(),alias))
    ).flatten() {
        if let Some(filter) = language_filter {
            if !filter.contains(lang.as_str()) {
                continue;
            }
        }
        let p = storage.store_data(DataValue::Alias(lang));
        let o = storage.store_data(DataValue::String(alias.value));
        storage.insert_edge(current, p, o);
    }
    //insert claims
    for claim in item.claims.into_iter().map(|(_,claim_vec)| claim_vec).flatten() {
        let p = storage.store_data(str_to_wikidataid(claim.mainsnak.property.clone()));
        let o = storage.store_data(get_datavalue(claim.mainsnak));
        
        // In json there is a $ delimiter between the entity id and the claim id. However the query
        // service expects a - as delimiter.
        let claim_id = &claim.id.to_owned().replace("$", "-");
        let edge = storage.insert_named_edge(&claim_id, current, p, o); //store edge id
        if parse_qualifiers {
        for (p_str, o_vec) in claim.qualifiers {
                let p = storage.store_data(str_to_wikidataid(p_str));
                for snak in o_vec.into_iter() {
                    let o = storage.store_data(get_datavalue(snak));       
                    storage.insert_edge(edge, p, o);
                }
            }
        }
    }
}
