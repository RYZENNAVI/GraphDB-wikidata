use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
};

use crate::{
    data_types::DataValue,
    ext_string::{ExtStringStorage, RamStorage, StringStorage},
    file_vec::{FileVec, ParallelFileVec, Writer},
    relation::{materialized_relation::MaterializedRelation, SortingOrder},
    storage_engine::{PATH_RELATION_ID, PATH_RELATION_OBJ, PATH_RELATION_PRE, PATH_RELATION_SUB},
};

use super::{
    from_boolean, from_edge_id, from_number_float, from_number_int, from_string, from_wikidata_id,
    time_to_node, Node, NodeType, Storage, StorageEngine, PATH_EXT_STR_STORAGE,
};

use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

use std::collections::HashMap;

pub trait StorageEngineBuilder {
    type MyStorageEngine: StorageEngine<'static>;

    /// store_data returns a Node that refers to the data
    fn store_data(&mut self, input: DataValue) -> Node;

    /// insert an edge into the graph; returns node that refers to the edge
    // Zu beachten: intern werden 4-Tupel  mit Subjekt, Prädikat, Objekt, Kanten-id gespeichert
    fn insert_edge(&mut self, s: Node, p: Node, o: Node) -> Node;

    fn insert_named_edge(&mut self, name: &str, s: Node, p: Node, o: Node) -> Node;

    /// Finalizes creation and disables usage of insert_edge() and store_data()
    /// Throws Error if this function was already called.
    // Finish creation, initialize ids
    fn finalize(self) -> Self::MyStorageEngine;
}
pub trait MyBuilder {
    type MyStorageEngine: StorageEngine<'static>;
    type MyStringStorage: StringStorage;

    fn get_string_storage(&mut self) -> &Self::MyStringStorage;

    ///insert an edge into the graph returns node that refers to the edge.
    // Beachten, intern werden 4-Tupel gespeichert mit subject, prädikat, object, kanten-id
    fn insert_edge(&mut self, s: Node, p: Node, o: Node) -> Node;

    fn insert_named_edge(&mut self, name: &str, s: Node, p: Node, o: Node) -> Node;

    /// Finalizes creation and disables usage of insert_edge() and store_data()
    /// Throws Error, when this function was already called.
    // Finish creation, initialize ids
    fn finalize(self) -> Self::MyStorageEngine;
}

impl<'a, T: MyBuilder> StorageEngineBuilder for T {
    type MyStorageEngine = T::MyStorageEngine;

    fn store_data(&mut self, input: DataValue) -> Node {
        match input {
            DataValue::WikidataidQ(input) => from_wikidata_id(input, NodeType::Q), //pass node id here no need to match twice
            DataValue::WikidataidP(input) => from_wikidata_id(input, NodeType::P),
            DataValue::WikidataidL(input) => from_wikidata_id(input, NodeType::L),
            DataValue::String(input) => {
                from_string(self.get_string_storage(), input, NodeType::String)
            }
            DataValue::Identifier(input) => {
                from_string(self.get_string_storage(), input, NodeType::Identifier)
            }
            DataValue::Url(input) => from_string(self.get_string_storage(), input, NodeType::Url),
            DataValue::Media(input) => {
                from_string(self.get_string_storage(), input, NodeType::Media)
            }
            DataValue::GeoShape(input) => {
                from_string(self.get_string_storage(), input, NodeType::GeoShape)
            }
            DataValue::Tabular(input) => {
                from_string(self.get_string_storage(), input, NodeType::Tabular)
            }
            DataValue::MathExp(input) => {
                from_string(self.get_string_storage(), input, NodeType::MathExp)
            }
            DataValue::MusicNotation(input) => {
                from_string(self.get_string_storage(), input, NodeType::MusicNotation)
            }
            DataValue::Label(input) => {
                from_string(self.get_string_storage(), input, NodeType::Label)
            }
            DataValue::Description(input) => {
                from_string(self.get_string_storage(), input, NodeType::Description)
            }
            DataValue::Alias(input) => {
                from_string(self.get_string_storage(), input, NodeType::Alias)
            }
            DataValue::MonolinText(input) => from_string(
                self.get_string_storage(),
                format!("{}\x1f{}", input.text, input.language).into(),
                NodeType::MonolinText,
            ), //serialize structs with serde to store as string
            DataValue::Quantity(input) => from_string(
                self.get_string_storage(),
                input.to_string().into(),
                NodeType::Quantity,
            ),
            DataValue::Time(input) => time_to_node(self.get_string_storage(), input),
            DataValue::Coord(input) => from_string(
                self.get_string_storage(),
                serde_json::to_string(&input).unwrap().into(),
                NodeType::Coord,
            ),
            DataValue::NumberInt(input) => from_number_int(input, NodeType::NumberInt),
            DataValue::NumberFloat(input) => from_number_float(input, NodeType::NumberFloat),
            DataValue::Boolean(input) => from_boolean(input, NodeType::Boolean),
            DataValue::Null() => Node { id: 0 },
            DataValue::Edge(input) => from_edge_id(input, NodeType::Edge),
            DataValue::NamedEdge(input) => from_string(self.get_string_storage(), input, NodeType::NamedEdge),
            DataValue::WikidataidPstmt(input) => from_wikidata_id(input, NodeType::Pstmt),
        }
    }

    fn insert_edge(&mut self, s: Node, p: Node, o: Node) -> Node {
        T::insert_edge(self, s, p, o)
    }

    fn insert_named_edge(&mut self, name: &str, s: Node, p: Node, o: Node) -> Node {
        T::insert_named_edge(self, name, s, p, o)
    }

    fn finalize(self) -> Self::MyStorageEngine {
        T::finalize(self)
    }
}

pub struct StorageBuilder<S>
where
    S: StringStorage,
{
    // Kantenliste (Subjekt, Prädikat, Objekt, Kanten ID), node id -> Datavalues
    pub edges: Vec<[Node; 4]>,
    // Bei der List<Node>-Rückgaben: Man kann die als Iterator verwenden, allerdings müssen vermutlich da alle Ergebnisse auf einmal zurückgegeben werden.
    string_storage: S,
}

fn convert_vec(mut input: Vec<[Node; 4]>) -> Vec<Node> {
    // SAFETY
    // the memory layout of Vec<[Node;4]> is linear.
    let output = unsafe {
        Vec::from_raw_parts(
            input.as_mut_ptr() as *mut Node,
            input.len() * 4,
            input.capacity() * 4,
        )
    };
    std::mem::forget(input);

    output
}

impl<S> MyBuilder for StorageBuilder<S>
where
    S: StringStorage,
{
    /// returns an edge-node from a given subject, predicate and object
    fn insert_edge(&mut self, s: Node, p: Node, o: Node) -> Node {
        let len: u64 = (self.edges.len() + 3)
            .try_into()
            .expect("edge id exeeds u64");
        let mut bytes = len.to_be_bytes();
        bytes[0] = NodeType::Edge as u8; //problem if len > 2^56
        self.edges.push([
            Node {
                id: u64::from_be_bytes(bytes),
            },
            s,
            p,
            o,
        ]);
        Node {
            id: u64::from_be_bytes(bytes),
        }
    }

    /// creates and sorts materialized relations and returns them in a storage engine struct
    fn finalize(self) -> Storage<'static, S> {
        let mut edges_by_id = self.edges;

        let mut edges_by_sub = Vec::with_capacity(edges_by_id.len());
        let mut edges_by_pre = Vec::with_capacity(edges_by_id.len());
        let mut edges_by_obj = Vec::with_capacity(edges_by_id.len());

        for [id, s, p, o] in edges_by_id.iter() {
            edges_by_sub.push([*s, *p, *o, *id]);
            edges_by_obj.push([*o, *p, *s, *id]);
            edges_by_pre.push([*p, *s, *o, *id]);
        }

        edges_by_id.sort_unstable();
        edges_by_sub.sort_unstable();
        edges_by_pre.sort_unstable();
        edges_by_obj.sort_unstable();

        let sort_order = vec![
            (0, SortingOrder::Ascending),
            (1, SortingOrder::Ascending),
            (2, SortingOrder::Ascending),
            (3, SortingOrder::Ascending),
        ];

        let column_names = vec![
            "Predicate".to_string(),
            "Subject".to_string(),
            "Object".to_string(),
            "ID".to_string(),
        ];

        let relation_pre = MaterializedRelation::create_presorted_mat_relation(
            column_names,
            convert_vec(edges_by_pre),
            sort_order.clone(),
        );

        let column_names = vec![
            "Subject".to_string(),
            "Predicate".to_string(),
            "Object".to_string(),
            "ID".to_string(),
        ];

        let relation_sub = MaterializedRelation::create_presorted_mat_relation(
            column_names,
            convert_vec(edges_by_sub),
            sort_order.clone(),
        );

        let column_names = vec![
            "Object".to_string(),
            "Predicate".to_string(),
            "Subject".to_string(),
            "ID".to_string(),
        ];

        let relation_obj = MaterializedRelation::create_presorted_mat_relation(
            column_names,
            convert_vec(edges_by_obj),
            sort_order.clone(),
        );

        let column_names = vec![
            "ID".to_string(),
            "Subject".to_string(),
            "Predicate".to_string(),
            "Object".to_string(),
        ];

        let relation_id = MaterializedRelation::create_presorted_mat_relation(
            column_names,
            convert_vec(edges_by_id),
            sort_order.clone(),
        );

        Storage::<S> {
            relation_id,
            relation_sub,
            relation_pre,
            relation_obj,
            string_storage: self.string_storage,
            counter: AtomicU64::new(0),
            released_ids: Mutex::new(HashSet::new()),
            temp_strings: Mutex::new(HashMap::new()),
        }
    }

    type MyStorageEngine = Storage<'static, S>;
    type MyStringStorage = S;

    fn get_string_storage(&mut self) -> &S {
        &self.string_storage
    }

    fn insert_named_edge(&mut self, name: &str, s: Node, p: Node, o: Node) -> Node {
        let edge = self.store_data(DataValue::NamedEdge(name.into()));
        self.edges.push([edge,s,p,o]);

        edge
    }
}

impl StorageBuilder<RamStorage> {
    pub fn new() -> StorageBuilder<RamStorage> {
        StorageBuilder {
            string_storage: RamStorage::new(),
            edges: Vec::new(),
        }
    }
}

pub struct Builder<'a> {
    parent: &'a ParallelStorageBuilder,
    writer: Writer<'a, (Node, Node, Node)>,
    writer_named: Writer<'a, (Node, Node, Node, Node)>,
}

impl MyBuilder for Builder<'_> {
    type MyStorageEngine = Storage<'static, ExtStringStorage>;
    type MyStringStorage = ExtStringStorage;

    fn insert_edge(&mut self, s: Node, p: Node, o: Node) -> Node {
        let index = self.writer.insert((s, p, o));

        Node {
            id: ((NodeType::Edge as u64) << 56 | (index) as u64),
        }
    }

    fn finalize(self) -> Self::MyStorageEngine {
        panic!("Called finalize() on a Builder. In parallel builds, finalize() has to be called on the ParallelStorageBuilder.")
    }

    fn get_string_storage(&mut self) -> &ExtStringStorage {
        &self.parent.ext_strings
    }

    fn insert_named_edge(&mut self, name: &str, s: Node, p: Node, o: Node) -> Node {
        let edge = self.store_data(DataValue::NamedEdge(name.into()));
        self.writer_named.insert((edge, s, p , o));

        edge
    }
}

pub struct ParallelStorageBuilder {
    ext_strings: ExtStringStorage,
    edges: ParallelFileVec<(Node, Node, Node)>,
    named_edges: ParallelFileVec<(Node, Node, Node, Node)>,
    path: PathBuf,
}

impl ParallelStorageBuilder {
    pub fn new<'b>(path: &'b Path) -> ParallelStorageBuilder {
        create_dir_all(path).unwrap();
        ParallelStorageBuilder {
            path: path.to_owned(),
            ext_strings: ExtStringStorage::open(
                &path.join(PATH_EXT_STR_STORAGE),
                128 * 1024 * 1024 * 1024,
            ),
            edges: unsafe {
                ParallelFileVec::new_named(
                    &path.join(Path::new("edges_parallel")),
                    1024 * 1024 * 1024 * 8,
                    1024 * 1024,
                )
            },
            named_edges: unsafe {
                ParallelFileVec::new_named(
                    &path.join(Path::new("named_edges_parallel")),
                    1024 * 1024 * 1024 * 8,
                    1024 * 1024,
                )
            }
        }
    }

    pub fn get_storage_builder(&self) -> Builder {
        Builder {
            parent: self,
            writer: self.edges.get_writer(),
            writer_named: self.named_edges.get_writer(),
        }
    }

    pub fn create_index(path: &Path) -> ParallelStorageBuilder {
        ParallelStorageBuilder {
            path: path.to_owned(),
            ext_strings: ExtStringStorage::open(
                &path.join(PATH_EXT_STR_STORAGE),
                128 * 1024 * 1024 * 1024,
            ),
            edges: unsafe {
                ParallelFileVec::open(
                    &path.join("edges_parallel"),
                    32 * 1024 * 1024 * 1024,
                    1024 * 1024,
                )
            },
            named_edges: unsafe {
                ParallelFileVec::open(
                    &path.join("named_edges_parallel"),
                    32 * 1024 * 1024 * 1024,
                    1024 * 1024,
                )
            },
        }
    }

    pub fn finalize(self) -> Storage<'static, ExtStringStorage> {
        println!("Cloning for relation by id...");
        let edges = self.edges.into_inner();
        let mut named_edges = self.named_edges.into_inner();

        let mut edges_by_id = unsafe {
            FileVec::new_named(&self.path.join(PATH_RELATION_ID), 16 * 1024 * 1024 * 1024)
        };
        edges_by_id.reserve(edges.len());

        let mut edges_by_sub = unsafe {
            FileVec::new_sorted(&self.path.join(PATH_RELATION_SUB), 16 * 1024 * 1024 * 1024)
        };
        //        edges_by_sub.reserve(edges.len());

        let mut edges_by_pre = unsafe {
            FileVec::new_sorted(&self.path.join(PATH_RELATION_PRE), 16 * 1024 * 1024 * 1024)
        };
        //        edges_by_pre.reserve(edges.len());

        let mut edges_by_obj = unsafe {
            FileVec::new_sorted(&self.path.join(PATH_RELATION_OBJ), 16 * 1024 * 1024 * 1024)
        };
        //        edges_by_obj.reserve(edges.len());

        edges
            .iter()
            .enumerate()
            .filter(|(_, (s, _, _))| s.id != 0)
            .for_each(|(id, (s, p, o))| {
                let id = Node {
                    id: (NodeType::Edge as u64) << 56 | id as u64,
                };
                edges_by_id.push([id, *s, *p, *o]);

                edges_by_sub.push([*s, *p, *o, id]);
                edges_by_obj.push([*o, *p, *s, id]);
                edges_by_pre.push([*p, *s, *o, id]);
            });

        named_edges.parallel_sort();

        named_edges
            .iter()
            .filter(|(e, _, _, _)| e.id != 0)
            .for_each(|(e, s, p, o)| {
                edges_by_id.push([*e, *s, *p, *o]);
                edges_by_sub.push([*s, *p, *o, *e]);
                edges_by_obj.push([*o, *p, *s, *e]);
                edges_by_pre.push([*p, *s, *o, *e]);
            });


        let edges_by_sub = edges_by_sub.merge();
        let edges_by_pre = edges_by_pre.merge();
        let edges_by_obj = edges_by_obj.merge();
        edges_by_id.shrink_to_fit();

        let sort_order = vec![
            (0, SortingOrder::Ascending),
            (1, SortingOrder::Ascending),
            (2, SortingOrder::Ascending),
            (3, SortingOrder::Ascending),
        ];

        let column_names = vec![
            "Predicate".to_string(),
            "Subject".to_string(),
            "Object".to_string(),
            "ID".to_string(),
        ];

        let relation_pre = MaterializedRelation::from_filevec(
            column_names,
            edges_by_pre.join().unwrap().into(),
            sort_order.clone(),
        )
        .unwrap();

        let column_names = vec![
            "Subject".to_string(),
            "Predicate".to_string(),
            "Object".to_string(),
            "ID".to_string(),
        ];

        let relation_sub = MaterializedRelation::from_filevec(
            column_names,
            edges_by_sub.join().unwrap().into(),
            sort_order.clone(),
        )
        .unwrap();

        let column_names = vec![
            "Object".to_string(),
            "Predicate".to_string(),
            "Subject".to_string(),
            "ID".to_string(),
        ];

        let relation_obj = MaterializedRelation::from_filevec(
            column_names,
            edges_by_obj.join().unwrap().into(),
            sort_order.clone(),
        )
        .unwrap();

        let column_names = vec![
            "ID".to_string(),
            "Subject".to_string(),
            "Predicate".to_string(),
            "Object".to_string(),
        ];

        let relation_id = MaterializedRelation::from_filevec(
            column_names,
            edges_by_id.into(),
            sort_order.clone(),
        )
        .unwrap();

        Storage::<ExtStringStorage> {
            relation_id,
            relation_sub,
            relation_pre,
            relation_obj,
            string_storage: self.ext_strings,
            counter: AtomicU64::new(0),
            released_ids: Mutex::new(HashSet::new()),
            temp_strings: Mutex::new(HashMap::new()),
        }
    }
}
