use std::path::Path;

use crate::{
    data_types::Time,
    ext_string::{ExtStringStorage, RamStorage, StringStorage},
};

use crate::data_types::CalendarModel;
use crate::data_types::DataValue;
use crate::calc_data_types::Operator;
use crate::relation::materialized_relation::MaterializedRelation;
use crate::relation::{Relation, SortingOrder};
use crate::small_string::SmallString;
use enum_primitive_derive::Primitive;
use num_traits::FromPrimitive;
use serde::{Deserialize, Serialize};
use warp::redirect::temporary;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::collections::HashMap;
use crate::storage_engine::builder::{StorageBuilder, StorageEngineBuilder};
//use self::builder::MyBuilder;


pub mod builder;

pub struct Query<'a>
{
    pub id: u64,
    pub query_plan: Operator,
    pub relation: Option<Box<dyn Relation<'a> + 'a>>,
    pub storage: &'a dyn IdGenerator<'a>,
}

impl Drop for Query<'_>{
    fn drop(&mut self) { 
        println!("Dropping Query!");
        //release id for further use
        self.storage.release_id(self.id);
        //remove storage for temporary strings

        self.storage.release_storage(self.id);
    }
}

/// Node used by storage engine for internal data
#[derive(PartialEq, PartialOrd, Clone, Copy, Debug, Serialize, Deserialize, Ord, Eq)]
pub struct Node {
    pub id: u64,
}
/// Assigns (u8-)integer values to the node-types.
/// The first byte(u8) of a Node(u64) will denote its type.
#[derive(PartialEq, Primitive)]
pub enum NodeType {
    Null = 0,
    Q = 1,
    P = 2,
    L = 3,
    String = 4,
    MonolinText = 5,
    Url = 6,
    Media = 7,
    GeoShape = 8,
    Tabular = 9,
    MathExp = 10,
    MusicNotation = 11,
    Quantity = 12,
    Time = 13,
    Date = 14,
    Coord = 15,
    Label = 16,
    Description = 17,
    Alias = 18,
    Edge = 19,
    Identifier = 20,
    NumberInt = 21,
    NumberFloat = 22,
    Boolean = 23,
    NamedEdge = 24,
    Pstmt = 25,
    Err = 255,
}

const PATH_TMP_EDGES: &str = "edges";
const PATH_RELATION_ID: &str = "relation_id";
const PATH_RELATION_SUB: &str = "relation_sub";
const PATH_RELATION_PRE: &str = "relation_pre";
const PATH_RELATION_OBJ: &str = "relation_obj";
const PATH_EXT_STR_STORAGE: &str = "strings";

pub trait StorageEngine<'a> {
    /// retrieve the data stored in an Node
    fn retrieve_data(&'a self, input: Node) -> DataValue<'a>;

    /// get a node form a Datavalue. This does not store the data only return the id if the data is already stored
    fn get_node_from_datavalue(&self, input: DataValue, query_id: Option<u64>) -> Node;

    // Basic Functions
    /// Get sub pred obj edge rows which connect subject s over predicate p with object o
    fn get_rows_from_subject_pred_object(&'a self, s: Node, p: Node, o: Node) -> 
        Box<dyn Relation<'a> + 'a>;    
    /// Get edge nodes which connect subject s over predicate p with object o
    fn get_edge_ids_from_subject_pred_object(&'a self, s: Node, p: Node, o: Node) -> 
        Box<dyn Relation<'a> + 'a>;
    fn get_nodes_from_edge_id(&'a self, s: Node) -> Box<dyn Relation<'a> + 'a>;
    /// Get all edges made with a given P-Node.
    fn get_all_edges_using_predicat(&'a self, p: Node) -> Box<dyn Relation<'a> + 'a>;
    /// Get all edges made with a given Q-Node saved as object.
    fn get_all_edges_using_object(&'a self, p: Node) -> Box<dyn Relation<'a> + 'a>;
    // Get all edges made with a given Q-Node saved as subject.
    fn get_all_edges_using_subject(&'a self, p: Node) -> Box<dyn Relation<'a> + 'a>;

    /// Get all edges with subject s and object o.
    fn get_all_predicat_edges_of(&'a self, s: Node, o: Node) -> Box<dyn Relation<'a> + 'a>;
    /// Get all edges with subject s and predicat p.
    fn get_all_object_edges_of(&'a self, s: Node, p: Node) -> Box<dyn Relation<'a> + 'a>;
    /// Get all edges with predicat p and object o.
    fn get_all_subject_edges_of(&'a self, p: Node, o: Node) -> Box<dyn Relation<'a> + 'a>;

    fn get_all_edges(&'a self) -> Box<dyn Relation<'a> + 'a>;
    
    fn relation_to_datavalue_vec(&'a self, rel: Box<dyn Relation<'a> +'a>, 
                                      columns: Option<Vec<&str>>) -> Vec<DataValue>;
}

#[derive(Serialize, Deserialize)]
pub struct Storage<'s, S>
where
    S: StringStorage,
{
    // Bei der List<Node>-Rückgaben: Man kann die als Iterator verwenden, allerdings müssen vermutlich da alle Ergebnisse auf einmal zurückgegeben werden.
    string_storage: S,
    /// Sorted by ID, column_names: ["ID", "Subject", "Predicate", "Object"]
    relation_id: MaterializedRelation<'s>,
    /// Sorted by ascending subject node and secondly by ascending predicate node
    /// column_names: ["Subject", "Predicate", "Object", "ID"]
    relation_sub: MaterializedRelation<'s>,
    /// Sorted by ascending predicate node
    relation_pre: MaterializedRelation<'s>,
    /// Sorted by ascending object node and secondly by ascending predicate node
    relation_obj: MaterializedRelation<'s>,

    ///counter for ids
    #[serde(skip)]
    counter: AtomicU64,
    ///set for released ids (can be reused)
    #[serde(skip)]
    released_ids: Mutex<HashSet<u64>>,

    ///storage for temporary strings
    #[serde(skip)]
    temp_strings: Mutex<HashMap<u64,RamStorage>>,
}

pub trait IdGenerator<'a> {
        ///generates unique id for query
        fn generate_id(&self) -> u64;

        ///releases used id, this id can now be used again
        fn release_id(&self, id: u64);    

        ///releases storage for temp strings belonging to finished query
        fn release_storage(&self, id: u64);
}

impl<'a, S> IdGenerator<'a> for Storage<'a, S>
where
    S: StringStorage,
{
    fn generate_id(&self) -> u64 {
        //try to reuse a released ID
        if let Ok(mut set) = self.released_ids.lock() {
            if let Some(&id) = set.iter().next() {
                set.remove(&id);
                return id;
            }
        }

        //no released IDs are available -> generate new one
        self.counter.fetch_add(1, Ordering::Relaxed)
    }


    fn release_id(&self, id: u64) {
        if let Ok(mut set) = self.released_ids.lock() {
            set.insert(id);
        }
    }

    fn release_storage(&self, id: u64){
        if let Ok(mut set) = self.temp_strings.lock() {
            set.remove(&id);
        }
    }
}

pub fn load_storage_engine(path: &Path) -> Result<Storage<'static, ExtStringStorage>, String> {
    if !path.is_dir() {
        return Err(format!(
            "Path is not a directory: {}",
            path.as_os_str().to_string_lossy()
        ));
    }

    let relation_id =
        MaterializedRelation::load_file_backed_mat_relation(&path.join(PATH_RELATION_ID))?;
    let relation_obj =
        MaterializedRelation::load_file_backed_mat_relation(&path.join(PATH_RELATION_OBJ))?;
    let relation_pre =
        MaterializedRelation::load_file_backed_mat_relation(&path.join(PATH_RELATION_PRE))?;
    let relation_sub =
        MaterializedRelation::load_file_backed_mat_relation(&path.join(PATH_RELATION_SUB))?;
    Ok(Storage {
        relation_id,
        relation_sub,
        relation_pre,
        relation_obj,
        string_storage: ExtStringStorage::open(
            &path.join(PATH_EXT_STR_STORAGE),
            128 * 1024 * 1024 * 1024,
        ),
        counter: AtomicU64::new(0),
        released_ids: Mutex::new(HashSet::new()),
        temp_strings: Mutex::new(HashMap::new()),
    })
}

pub fn load_ram_storage_engine(path: &Path) -> Result<Storage<'static, RamStorage>, String> {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);
    Ok(bincode::deserialize_from(reader).unwrap())
}

impl<'a, S> StorageEngine<'a> for Storage<'a, S>
where
    S: StringStorage,
{
    fn retrieve_data(&self, input: Node) -> DataValue {
        let mut bytes = input.id.to_be_bytes();
        match NodeType::from_u8(bytes[0]).unwrap_or(NodeType::Err) {
            NodeType::Null => DataValue::Null(),
            NodeType::Q => DataValue::WikidataidQ(to_wikidata_id(input)),
            NodeType::P => DataValue::WikidataidP(to_wikidata_id(input)),
            NodeType::L => DataValue::WikidataidL(to_wikidata_id(input)),
            NodeType::String => DataValue::String(to_string_long(self, input)),
            NodeType::Pstmt => DataValue::WikidataidPstmt(to_wikidata_id(input)),
            NodeType::Identifier => {
                DataValue::Identifier(to_string_long(self, input))
            }
            NodeType::Url => DataValue::Url(to_string_long(self, input)),
            NodeType::Media => DataValue::Media(to_string_long(self, input)),
            NodeType::GeoShape => DataValue::GeoShape(to_string_long(self, input)),
            NodeType::Tabular => DataValue::Tabular(to_string_long(self, input)),
            NodeType::MathExp => DataValue::MathExp(to_string_long(self, input)),
            NodeType::MusicNotation => {
                DataValue::MusicNotation(to_string_long(self, input))
            }
            NodeType::Label => DataValue::Label(to_string_long(self, input)),
            NodeType::Description => {
                DataValue::Description(to_string_long(self, input))
            }
            NodeType::Alias => DataValue::Alias(to_string_long(self, input)),
            NodeType::MonolinText => DataValue::MonolinText(Box::new(
                to_string_long(self, input).as_str().into(),
            )), // deserialize structs
            NodeType::Quantity => DataValue::Quantity(Box::new(
                to_string_long(self, input).as_str().into(),
            )),
            NodeType::Date => DataValue::Time(node_to_time(input)), // date Node -> struct -> datavalue
            NodeType::Time => DataValue::Time(
                serde_json::from_str(&to_string_long(self, input)).unwrap(),
            ),
            NodeType::Coord => DataValue::Coord(
                serde_json::from_str(&to_string_long(self, input).to_owned())
                    .unwrap(),
            ),
            NodeType::NumberInt => DataValue::NumberInt(to_number_int(input)),
            NodeType::NumberFloat => DataValue::NumberFloat(to_number_float(input)),
            NodeType::Boolean => DataValue::Boolean(to_boolean(input)),
            NodeType::Edge => DataValue::Edge(to_edge_id(input)),
            NodeType::NamedEdge => DataValue::NamedEdge(to_string_long(self, input)),
            _ => DataValue::Null(),
        }
    }

    fn get_node_from_datavalue(&self, input: DataValue, query_id: Option<u64>) -> Node {
        match input {
            DataValue::WikidataidQ(input) => from_wikidata_id(input, NodeType::Q),
            DataValue::WikidataidP(input) => from_wikidata_id(input, NodeType::P),
            DataValue::WikidataidL(input) => from_wikidata_id(input, NodeType::L),
            DataValue::String(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::String, query_id)
            }
            DataValue::Identifier(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::Identifier, query_id)
            }
            DataValue::Url(input) => get_string_id(&self.string_storage,
                                                   &self.temp_strings,
                                                   input, NodeType::Url, query_id),
            DataValue::Media(input) => get_string_id(&self.string_storage,
                                                     &self.temp_strings,
                                                     input, NodeType::Media, query_id),
            DataValue::GeoShape(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::GeoShape, query_id)
            }
            DataValue::Tabular(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::Tabular, query_id)
            }
            DataValue::MathExp(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::MathExp, query_id)
            }
            DataValue::MusicNotation(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::MusicNotation, query_id)
            }
            DataValue::Label(input) => get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::Label, query_id),
            DataValue::Description(input) => {
                get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::Description, query_id)
            }
            DataValue::Alias(input) => get_string_id(&self.string_storage, &self.temp_strings, input, NodeType::Alias, query_id),
            DataValue::MonolinText(input) => get_string_id(
                &self.string_storage,
                &self.temp_strings, 
                format!("{}\x1f{}", input.text, input.language).into(),
                NodeType::MonolinText,
                query_id
            ),
            DataValue::Time(input) => time_to_node(&self.string_storage, input),
            DataValue::Coord(input) => get_string_id(
                &self.string_storage,
                &self.temp_strings, 
                serde_json::to_string(&input).unwrap().into(),
                NodeType::Coord,
                query_id
            ),
            DataValue::Quantity(input) => get_string_id(
                &self.string_storage,
                &self.temp_strings, 
                serde_json::to_string(&input).unwrap().into(),
                NodeType::Quantity,
                query_id
            ),
            DataValue::NumberInt(input) => from_number_int(input, NodeType::NumberInt),
            DataValue::NumberFloat(input) => from_number_float(input,
                                                               NodeType::NumberFloat),
            DataValue::Boolean(input) => from_boolean(input, NodeType::Boolean),
            DataValue::Edge(input) => from_edge_id(input, NodeType::Edge),

            DataValue::NamedEdge(input) => get_string_id(&self.string_storage,
                                                         &self.temp_strings,
                                                         input, NodeType::NamedEdge,
                                                         query_id),
            DataValue::WikidataidPstmt(input) => from_wikidata_id(input, NodeType::Pstmt),
            DataValue::Null() => Node { id: 0 },
        }
    }

    fn get_rows_from_subject_pred_object(&'a self, s: Node, p: Node, o: Node) -> 
        Box<dyn Relation<'a> + 'a> {
            let intermediate  =  self.get_all_object_edges_of(s, p).
                sort(vec![("Object", SortingOrder::Ascending)]).to_materialized();
            let intermediate2 = intermediate.binary_search(&[2], &[o]).to_materialized();
            // somehow the column names got lost
            let rename = HashMap::from([("0", "Subject"), 
                                        ("1", "Predicate"), 
                                        ("2", "Object"), 
                                        ("3", "ID")]);
            Box::new(intermediate2.into_vec_iter().collect::<MaterializedRelation>())            .rename_attributes(&rename) 
        }
    fn get_edge_ids_from_subject_pred_object(&'a self, s: Node, p: Node, o: Node) 
                               -> Box<dyn Relation<'a> + 'a> {
        self.get_rows_from_subject_pred_object(s, p, o).project(vec!["ID"])
    }

    fn get_nodes_from_edge_id(&'a self, id: Node) -> Box<dyn Relation<'a> + 'a> {
        self.relation_id.binary_search(&[0], &[id])
    }
    fn get_all_edges_using_predicat(&'a self, p: Node) -> Box<dyn Relation<'a> + 'a> {
        self.relation_pre.binary_search(&[0], &[p])
    }
    fn get_all_edges_using_object(&'a self, o: Node) -> Box<dyn Relation<'a> + 'a> {
        self.relation_obj.binary_search(&[0], &[o])
    }
    fn get_all_edges_using_subject(&'a self, s: Node) -> Box<dyn Relation<'a> + 'a> {
        self.relation_sub.binary_search(&[0], &[s])
    }

    fn get_all_predicat_edges_of(&'a self, s: Node, o: Node) -> Box<dyn Relation<'a> + 'a> {
        let ref_1 = self.relation_sub.binary_search(&[0], &[s]);
        let mut rel_1 = ref_1.clone_to_owned();
        rel_1.merge_sort(vec![(2, SortingOrder::Ascending)]); //sort for object
        let result = rel_1.binary_search(&[2], &[o]);
        result.clone_to_owned()
    }

    fn get_all_object_edges_of(&'a self, s: Node, p: Node) -> Box<dyn Relation<'a> + 'a> {
        self.relation_sub.binary_search(&[0, 1], &[s, p])
    }
    fn get_all_subject_edges_of(&'a self, p: Node, o: Node) -> Box<dyn Relation<'a> + 'a> {
        self.relation_obj.binary_search(&[0, 1], &[o, p])
    }

    fn get_all_edges(&'a self) -> Box<dyn Relation<'a> + 'a> {
        self.relation_id.create_reference()
    }
    
    fn relation_to_datavalue_vec(&'a self,
                                      mut rel: Box<dyn Relation<'a> +'a>,
                                      columns: Option<Vec<&str>>)
                                      -> Vec<DataValue> {
        if let Some(cols) = columns{
            rel = rel.project(cols);
        }
        let mut result : Vec<DataValue> = Vec::with_capacity(
            rel.get_row_count() * rel.get_column_count());
        for i in 0..rel.get_row_count(){
            result.extend(rel.get_row(i).into_iter().
                    map(|node| self.retrieve_data(*node)));                    
        }
        result
    }
}


pub fn from_wikidata_id(input: u64, node_type: NodeType) -> Node {
    let mut bytes = input.to_be_bytes();
    bytes[0] = node_type as u8; //problem if wikidata id > 2^56 - 1
    Node {
        id: u64::from_be_bytes(bytes),
    }
}

pub fn to_wikidata_id(input: Node) -> u64 {
    let mut bytes = input.id.to_be_bytes();
    // let first = NodeType::from_u8(bytes[0]).unwrap_or(NodeType::Err);
    bytes[0] = 0;
    u64::from_be_bytes(bytes)
}

pub fn wikidataidp_to_wikidataidpstmt(input: Node) -> Node{
    let mut be_bytes = input.id.to_be_bytes();
    if be_bytes[0] == (NodeType::P as u8) {
        be_bytes[0] = NodeType::Pstmt as u8;
        Node{id: u64::from_be_bytes(be_bytes)}
    } else {        
        panic!("tried to convertnon WikidataidP-Node bytes {:?} to WikidataidPstmt Node", 
        be_bytes[0]);
    }

}


///converts id to bytes, copies the last 4 values and converts them to i32
fn to_number_int(input: Node) -> i32 {
    let bytes = input.id.to_be_bytes();
    let mut number: [u8; 4] = [0; 4];
    number.copy_from_slice(&bytes[4..]);

    i32::from_be_bytes(number)
}

///converts id to bytes, copies the last 4 values and converts them to f32
fn to_number_float(input: Node) -> f32 {
    let bytes = input.id.to_be_bytes();
    let mut number: [u8; 4] = [0; 4];
    number.copy_from_slice(&bytes[4..]);

    f32::from_be_bytes(number)
}

///converts id to bytes, copies last value and converts it into bool
fn to_boolean(input: Node) -> bool {
    let bytes = input.id.to_be_bytes();
    let byte = bytes[bytes.len() - 1];

    if byte == 0 {
        false
    } else {
        true
    }
}

///converts number into bytes, stores it in id and returns node
fn from_number_int(input: i32, node_type: NodeType) -> Node {
    let bytes = input.to_be_bytes();
    let mut id: [u8; 8] = [0; 8];
    id[4..].copy_from_slice(&bytes);

    id[0] = node_type as u8;
    Node {
        id: u64::from_be_bytes(id),
    }
}

///converts number into bytes, stores it in id and returns node
fn from_number_float(input: f32, node_type: NodeType) -> Node {
    let bytes = input.to_be_bytes();
    let mut id: [u8; 8] = [0; 8];
    id[4..].copy_from_slice(&bytes);

    id[0] = node_type as u8;
    Node {
        id: u64::from_be_bytes(id),
    }
}

///converts boolean into byte, stores it in id and returns node
fn from_boolean(input: bool, node_type: NodeType) -> Node {
    let number = u8::from(input);
    let mut bytes: [u8; 8] = [0; 8];
    bytes[7] = number;

    bytes[0] = node_type as u8;
    Node {
        id: u64::from_be_bytes(bytes),
    }
}

/// converts edge id stored in a u64 to a Node
/// panics if id uses more that 56 bits
pub fn from_edge_id(input: u64, node_type: NodeType) -> Node {
    if input > 0x00_ff_ff_ff_ff_ff_ff_ff_u64 {
        panic!("edge id uses more than 56 bits");
    }
    let mut bytes: [u8; 8] = input.to_be_bytes();
    bytes[0] = node_type as u8;
    Node {
        id: u64::from_be_bytes(bytes),
    }
}

/// converts Node of type Edge to 56bit-id in u64 value
fn to_edge_id(input: Node) -> u64 {
    let mut bytes: [u8; 8] = input.id.to_be_bytes();
    bytes[0] = 0_u8;
    u64::from_be_bytes(bytes)
}

/// Stores input in an external string storage and returns the corresponding node
pub fn from_string<S: StringStorage>(
    string_storage: &S,
    input: SmallString,
    node_type: NodeType,
) -> Node {
    let mut id = string_storage.insert_string(&input);
    let mut bytes = id.to_be_bytes();
    bytes[0] = node_type as u8;
    id = u64::from_be_bytes(bytes);
    Node { id }
}

fn to_string_long<'a,S: StringStorage>(storage: &'a Storage<'a,S>, input: Node) -> SmallString<'a> {
    let mut bytes = input.id.to_be_bytes();
    bytes[0] = 0;
    let id = u64::from_be_bytes(bytes);
    if bytes[1]==254 {
        let guard = storage.temp_strings.lock().unwrap();
        let string_storage = guard.get(&(bytes[2] as u64)).unwrap();
        bytes[1]=255;
        bytes[2]=0;
        string_storage.get_string(u64::from_be_bytes(bytes)).to_owned()
    } else {
        storage.string_storage.get_string(id)
    }
}

fn time_to_node<S: StringStorage>(string_storage: &S, time: Time) -> Node {
    let Time {
        year,
        month,
        day,
        hour: _,
        minute: _,
        second: _,
        before,
        after,
        precision,
        calendarmodel,
        timezone: _,
    } = time;
    match precision {
        0..=11 => Node {
            id: ((NodeType::Date as u64) << 56)
                | ((year as u64 & 0xFFFF) << 40)
                | ((month as u64) << 32)
                | ((day as u64) << 24)
                | ((before as u64 & 0xF) << 16)
                | ((after as u64 & 0xF) << 12)
                | ((precision as u64) << 8)
                | calendarmodel as u64,
        },
        12..=14 => from_string(
            string_storage,
            serde_json::to_string(&time).unwrap().into(),
            NodeType::Time,
        ),
        _ => panic!("Invalid precision in Time: {precision}"),
    }
}

pub fn node_to_time(date_node: Node) -> Time // convert Nodetype::Date to struct Time
{
    let node_id = date_node.id;
    let node_type = node_id >> 56; // to get the type of node

    if node_type == NodeType::Date as u64
    // it is a Date Node,
    {
        let calendar_type_u8 = ((node_id << 56) >> 56) as u8;
        return Time
        //The first right shift operation moves the rightmost bit of the needed data to the rightmost end of the u64
        //The first left shift operation and the second right shift operation remove the unnecessary data in the left
        //And move the rightmost bits of needed data to the rightmost end of the u64 again
        //Then convert them to corresponding types.(i64, u8,i16)
        //This is totally the reverse of time_to_node.
        {
            year: ((((node_id >> 40)<< 48) >> 48)) as i64,
            month: (((node_id >> 32)<< 56) >> 56 )as u8,
            day: (((node_id >> 24)<< 56)>> 56) as u8,
            hour:(00) as u8,
            minute:(00) as u8,
            second:(00) as u8,
            before: (((node_id >> 20) << 60)>> 60) as u8,
            after: (((node_id >> 16)<< 60)>> 60 ) as u8,
            precision:(((node_id >> 8)<< 56) >> 56)as u8,
            calendarmodel: CalendarModel::from_u8(calendar_type_u8).unwrap(),
            timezone:(0) as i16,
        };
    } else {
        panic!("Invalid Nodetype\n");
    }
}

/// get id of a stored string
fn get_string_id(
    string_storage: &impl StringStorage,
    temp_strings: &Mutex<HashMap<u64, RamStorage>>,
    input: SmallString,
    node_type: NodeType,
    query_id: Option<u64>
) -> Node {
    //println!("{}",input);
    let opt_id = string_storage.get_id(&input);
    match (query_id,opt_id){
        //exists -> return Node
        //without query_id -> from method pattern_to_node
        (Some(_), Some(value)) | (None, Some(value)) =>{;
            let mut bytes = value.to_be_bytes();
            bytes[0] = node_type as u8;
            let id = u64::from_be_bytes(bytes);
            Node { id }
        },
        //doesn't exist and has a query_id -> store
        (Some(query),None) => {
            //println!("stores");
            if let Ok(mut temp_strings_storages) = temp_strings.lock(){
            //checks if there is already a String-Storage for this query
            if temp_strings_storages.contains_key(&query) == false{
                //create new entry
                temp_strings_storages.insert(query, RamStorage::new());
                //println!("NEW STORAGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            }

            //stores input in String Storage
            let storage = temp_strings_storages.get(&query).unwrap();
            let mut id = storage.insert_string(&input);

            //modify id
            let mut bytes: [u8; 8] = id.to_be_bytes();
            //println!("{:#?}",bytes);
            bytes[0] = node_type as u8;
            //identifies as temporary string
            bytes[1] = 254 as u8;
            //identifies which String-Storage to use
            //assumption: maximum of 256 queries at the same time
            bytes[2] = query as u8; 
            //println!("{:#?}",bytes);
            id = u64::from_be_bytes(bytes);

            //println!("{}",id);
            return Node{ id }
            }
            Node{ id:0 }
        },
        //should store but no query_id -> panic!
        (None,None) => Node{ id:0 },
        //{println!("NONE;NONE"); panic!();},
    }
}

/// translate a string of prefix quads  into a vec of DataValue quads
/// is used as fast format for filling few quads test databases
/// see test below
pub fn prefix_str_to_datavalue_vec(quads : &str) -> Vec<DataValue>{
    quads.split(',').
        map(|word| prefix_to_datavalue(word.trim())).
        collect()
}

pub fn prefix_to_datavalue(word : &str) -> DataValue{
    let words : Vec<&str> =  word.split(':').collect();
    let prefix = words[0];
    let name = words[1];
    match prefix {
        "wd" => DataValue::WikidataidQ(str::parse(&name.replace('Q', "")).unwrap()),       
        "wdt" => DataValue::WikidataidP(str::parse(&name.replace('P', "")).unwrap()), 
        "p" => DataValue::WikidataidPstmt(str::parse(&name.replace('P', "")).unwrap()),
        "wds" => DataValue::NamedEdge(SmallString::from(name)), 
        _ => panic!("used bad prefix minilanguage to fill test database!")
    }
}

/// setup small test database by giving a prefix_str
/// the string consists of comma seperated DataValue items like
/// "wd:Q1, wdt:P2, wd:Q3, wds:4" 
/// of those prefixes just used and also p:P2 for WikidataidPstmt type
/// it must have a multiple of 4 items (Subject, Predicate, Object, NamedEdge)
pub fn setup_test_storage(prefix_str: &str)-> Storage<RamStorage> {
    let mut builder : StorageBuilder<RamStorage>= StorageBuilder::new();
    for row in prefix_str_to_datavalue_vec(prefix_str).chunks(4){
        let s = builder.store_data(row[0].clone());
        let p = builder.store_data(row[1].clone());
        let o = builder.store_data(row[2].clone());
        let  DataValue::NamedEdge(ref name) = row[3]
        else {panic!("cannot extract edge name in custom tiny test database")};
        builder.insert_named_edge(name , s, p, o);
    }
    builder.finalize()
}

#[cfg(test)]
mod tests {
    use crate::parser::parse_file;
    use crate::calc_engine::setup_calc_engine;
    use super::*;

    ///counts how many nodes match the target
    ///only for testing if binary_search finds finds the correct number of rows
    fn simple_count(relation: &MaterializedRelation, column: usize, target: Node) -> usize {
        let mut count: usize = 0;
        for i in 0..relation.get_row_count() {
            if relation.get_row(i)[column] == target {
                //println!("found in row {i}");
                count = count + 1;
            }
        }
        return count;
    }
    ///tests if every element of one column is smaller than the next element
    fn is_sorted(relation: &MaterializedRelation, column: usize) -> bool {
        for i in 0..(relation.get_row_count() - 1) {
            if relation.get_row(i)[column] > relation.get_row(i + 1)[column] {
                //println!("Comparing {0} to {1}", relation.get_row(i)[column].id, relation.get_row(i + 1)[column].id);
                //println!("unsorted at line {0} from {1}", i, relation.get_row_count());
                return false;
            }
        }
        return true;
    }
    #[test]
    fn storage_test() {
        let mut seb = builder::StorageBuilder::new();
        parse_file(
            Path::new("./tests/data/first_5_lines.txt"),
            &mut seb,
            true,
            &None,
        );
        let s = builder::StorageEngineBuilder::finalize(seb);
        assert!(is_sorted(&s.relation_sub, 0));
        assert!(is_sorted(&s.relation_pre, 0));
        assert!(is_sorted(&s.relation_obj, 0));
        assert!(is_sorted(&s.relation_id, 0));
        let data = s.get_node_from_datavalue(DataValue::WikidataidQ(31),None);
        assert_eq!(
            s.get_all_edges_using_subject(data).get_row_count(),
            simple_count(&s.relation_sub, 0, data)
        );
    }

    #[test]
    fn bool_int_float_test() {
        let node = from_boolean(true, NodeType::Boolean);
        let value = to_boolean(node);
        println!("{}", value);
        assert_eq!(value, true);

        let node2 = from_boolean(false, NodeType::Boolean);
        let value2 = to_boolean(node2);
        println!("{}", value2);
        assert_eq!(value2, false);

        let node3 = from_number_int(10, NodeType::NumberInt);
        let value3 = to_number_int(node3);
        println!("{}", value3);
        assert_eq!(value3, 10);

        let node4 = from_number_float(15.93245, NodeType::NumberFloat);
        let value4 = to_number_float(node4);
        println!("{}", value4);
        assert_eq!(value4, 15.93245);
    }

    #[test]
    fn converting_to_node_from_an_edge_id() {
        let edge_id: u64 = 4;
        let edgy_node = from_edge_id(edge_id, NodeType::Edge);
        // assumption edge id = 19 = 0x13
        let supposed: u64 = 0x13_00_00_00_00_00_00_04_u64;
        assert!(edgy_node.id == supposed);
    }

    #[test]
    fn converting_to_edge_id_from_node() {
        let edgy_node: Node = Node {
            id: 0x13_00_00_00_00_00_00_04_u64,
        };
        assert!(to_edge_id(edgy_node) == 4_u64);
    }

    #[test]
    fn test_prefix_to_datavalue(){
        assert_eq!(prefix_to_datavalue("wd:Q1"), DataValue::WikidataidQ(1));
        assert_eq!(prefix_to_datavalue("wdt:P2"), DataValue::WikidataidP(2));
        assert_eq!(prefix_to_datavalue("p:P2"), DataValue::WikidataidPstmt(2));
        assert_eq!(prefix_to_datavalue("wds:4"), DataValue::NamedEdge(SmallString::from("4")));
    }

    #[test]
    fn translate_prefix_str_to_datavalue_vec(){
    
        let datavalue_str = "wd:Q1, wdt:P2, wd:Q3, wds:4, 
                             wd:Q1, p:P2, wds:4, wds:5";
        let expected = vec![
            DataValue::WikidataidQ(1), 
            DataValue::WikidataidP(2), 
            DataValue::WikidataidQ(3),
            DataValue::NamedEdge(SmallString::from("4")), 
            // second quad
            DataValue::WikidataidQ(1), 
            DataValue::WikidataidPstmt(2), 
            DataValue::NamedEdge(SmallString::from("4")), 
            DataValue::NamedEdge(SmallString::from("5"))];
        assert_eq!(prefix_str_to_datavalue_vec(datavalue_str), expected);
    }

    #[test]
    fn test_wikidataidp_to_wikidatataidpstmt(){
        let store = setup_test_storage("wd:Q1, wdt:P2, wd:Q3, wds:4");
        let direct_node = store.get_node_from_datavalue(DataValue::WikidataidP(4), None);
        assert_eq!(wikidataidp_to_wikidataidpstmt(direct_node), 
                   store.get_node_from_datavalue(DataValue::WikidataidPstmt(4), None));
    }

    #[test]
    fn test_get_edge_ids_from_subject_pred_object(){
        let store = setup_test_storage("wd:Q1, wdt:P2, wd:Q3, wds:4, 
                                        wd:Q1, wdt:P2, wd:Q3, wds:5, 
                                        wd:Q1, wdt:P2, wd:Q11, wds:6");
        let q1_node = from_wikidata_id(1, NodeType::Q);
        let p2_node = from_wikidata_id(2, NodeType::P);
        let q3_node = from_wikidata_id(3, NodeType::Q);
        let rel_edge = store.get_edge_ids_from_subject_pred_object(q1_node, p2_node, q3_node);
        let result_edge = store.relation_to_datavalue_vec(rel_edge, None);
        let expected_edge = prefix_str_to_datavalue_vec("wds:4, wds:5");
        assert_eq!(result_edge, expected_edge);
    }
    
        #[test]
    fn test_get_rows_from_subject_pred_object(){
        let store = setup_test_storage("wd:Q1, wdt:P2, wd:Q3, wds:4, 
                                        wd:Q1, wdt:P2, wd:Q3, wds:5, 
                                        wd:Q1, wdt:P2, wd:Q11, wds:6");
        let q1_node = from_wikidata_id(1, NodeType::Q);
        let p2_node = from_wikidata_id(2, NodeType::P);
        let q3_node = from_wikidata_id(3, NodeType::Q);
        let rel_rows = store.get_rows_from_subject_pred_object(q1_node, p2_node, q3_node);
        let result_rows = store.relation_to_datavalue_vec(rel_rows, None);
        let expected_rows = prefix_str_to_datavalue_vec("wd:Q1, wdt:P2, wd:Q3, wds:4,
                                                         wd:Q1, wdt:P2, wd:Q3, wds:5");
        assert_eq!(result_rows, expected_rows);
    }


}
