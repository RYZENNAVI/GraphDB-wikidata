//! Provides an implementation for the trait [Relation]
use serde::{Deserialize, Serialize};

use crate::file_vec::FileVec;
use crate::relation::{Relation, RelationIterator, SortingOrder};
use crate::storage_engine::Node;
use crate::relation::Pattern;
use crate::data_types::DataValue;
use std::cmp::Ordering;
use std::fs::File;
use std::ops::{Deref, DerefMut, Index};
use std::path::{Path, PathBuf};
// TODO: Why do I need that import??
use crate::relation::materialized_relation::Data::{FileOwned, Owned, Ref};
use std::collections::HashMap;

/// A mathematical tuple with multiple named columns and rows.
///
/// This struct is modeled after a [relation from database theory](https://en.wikipedia.org/wiki/Relation_(database)).
/// Each row consists of a consistent number of [Nodes](Node).
/// Each row has four nodes: subject, predicate, object, id
/// # Example
///
/// ```
/// use graphdb::relation::materialized_relation::MaterializedRelation;
/// use graphdb::storage_engine::{Node};
///
/// let column_name = vec!["subject".to_string(), "predicate".to_string(), "object".to_string()];
/// let relation_data = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 2,} ].into_boxed_slice();
/// let relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
/// ```
///
/// # Additional Notes
///
/// Internally, a relation can be one of these three following types:
/// - Own data and save it only in RAM
/// - Own data and save it in a file, then memory map it to RAM
/// - Reference to the Data of another relation
#[derive(Serialize, Deserialize)]
pub struct MaterializedRelation<'a> {
    /// Name of the columns, also implies number of columns
    column_names: Vec<String>,
    /// Contains columns this vector was sorted after, from first / most recent onwars to last.
    /// E.g. : If sorted_after == [2, 0], then the relation is primarly sorted after column 2, then secondarly by column 0
    sorted_after: Vec<(usize, SortingOrder)>,
    /// Path to meta file, where this data is saved persistenetly
    path_to_meta: Option<PathBuf>,
    /// Points to the rows of the relation
    data: Data<'a>,
}

/// Saves the data of a relation
#[derive(Clone, Serialize, Deserialize)]
enum Data<'a> {
    // Save the data in RAM
    Owned(Vec<Node>),
    // Reference to the data slice of another relation
    #[serde(skip)] // Serde is only needed for StorageEngine that owns the relations.
    Ref(&'a [Node]),
    // Save the data in a file and memory map it to RAM
    #[serde(skip)]
    // Serializing the external memory variant is not supported. It already is in EM.
    FileOwned(FileVec<Node>),
}

impl Deref for Data<'_> {
    type Target = [Node];

    fn deref(&self) -> &Self::Target {
        match &self {
            Data::Ref(slice) => *slice,
            Data::Owned(vector) => vector.as_slice(),
            Data::FileOwned(file_vec) => file_vec.as_ref(),
        }
    }
}

impl DerefMut for Data<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            // Panics, since returning that slice here makes it possible to accidentally edit the storage_engine relations.
            Data::Ref(_slice) => panic!("Referenced Relation can't be edited."),
            Data::Owned(vector) => vector.as_mut_slice(),
            Data::FileOwned(file_vec) => file_vec.as_mut(),
        }
    }
}

const PATH_TMP_RELATION: &str = "merge_sort";

impl<'a> MaterializedRelation<'a> {
    fn new(column_names: Vec<String>, sorted_after: Vec<(usize, SortingOrder)>) -> MaterializedRelation<'a>{

        Self { 
            column_names,
            sorted_after,
            path_to_meta: None,
            data: Data::Owned(Vec::<Node>::new()),
        }
    }

    /// Creates and returns a materialized_relation, which stores its data in RAM.
    ///
    /// The column names are took over, while the given node slice is copied from.
    /// This option should be used for smaller relations (< 10000 Nodes).
    ///
    /// # Example
    ///
    /// ```
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["subject".to_string(), "predicate".to_string()];
    /// let relation_data = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 2,}, Node{id: 2,} ];
    /// let relation = MaterializedRelation::create_mat_relation(column_name, relation_data.as_slice());
    /// ```
    pub fn create_mat_relation(
        column_names: Vec<String>,
        nodes: &[Node],
    ) -> MaterializedRelation<'static> {
        MaterializedRelation {
            column_names,
            sorted_after: vec![(0 as usize, SortingOrder::Ascending); 0],
            path_to_meta: None,
            data: Data::Owned(nodes.into()),
        }
    }

    /// Creates and returns a new file-backed materialized_relation, whose data will be stored on a file and memory-mapped to RAM.
    ///
    /// The column names are took over, while the given node slice is copied from.
    /// Two files called "file_path" and "filepath.json" are created. Existing files will be truncated and overwritten.
    /// "file_path" saves the rows of the relation.
    /// "filepath.json" saves the column names and the lenght of the relation.
    /// This option should be not used for smaller relation (< 10000 Nodes), since it comes with a considerable overhead.
    ///
    /// # Errors
    /// Returns an error with corresponding error message, if:
    /// - the read of the meta file fails
    /// - the serialization of the meta file fails
    ///
    /// # Panics
    ///
    /// Panics, when file_path can't be converted to a &str.
    /// TODO: What does file_vec::from_slice does on panic or error?
    ///
    /// # Example
    ///
    /// ```
    /// use std::path::Path;
    /// use std::fs::create_dir_all;
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["subject".to_string(), "predicate".to_string()];
    /// let relation_data = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 2,}, Node{id: 2,} ].into_boxed_slice();
    /// create_dir_all("cache");
    /// let path = Path::new("cache/relation_id");
    /// let relation = MaterializedRelation::create_file_backed_mat_relation(column_name, &*relation_data, path);
    /// ```
    pub fn create_file_backed_mat_relation(
        column_names: Vec<String>,
        nodes: &[Node],
        file_path: &Path,
    ) -> Result<MaterializedRelation<'static>, String> {
        let mut file_meta_path = PathBuf::from(file_path);
        file_meta_path.set_extension("json");
        let mut new_rel = MaterializedRelation {
            column_names,
            sorted_after: vec![(0 as usize, SortingOrder::Ascending); 0],
            path_to_meta: Some(file_meta_path),
            data: Data::FileOwned(unsafe { FileVec::from_slice(file_path, nodes) }),
        };
        new_rel.update_meta_file();
        Ok(new_rel)
    }

    /// Creates and returns a new file-backed materialized_relation, whose data will be stored on a file and memory-mapped to RAM.
    ///
    /// The column names and sorting order are took over, while the given node slice is copied from.
    /// Two files called "file_path" and "filepath.json" are created. Existing files will be truncated and overwritten.
    /// "file_path" saves the rows of the relation.
    /// "filepath.json" saves the column names and the lenght of the relation.
    /// This option should be not used for smaller relation (< 10000 Nodes), since it comes with a considerable overhead.
    ///
    /// # Errors
    /// Returns an error with corresponding error message, if:
    /// - the read of the meta file fails
    /// - the serialization of the meta file fails
    ///
    /// # Panics
    ///
    /// Panics, when file_path can't be converted to a &str.
    /// TODO: What does file_vec::from_slice does on panic or error?
    ///
    /// # Example
    ///
    /// ```
    /// use std::path::Path;
    /// use std::fs::create_dir_all;
    /// use graphdb::relation::SortingOrder;
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["subject".to_string(), "predicate".to_string()];
    /// let relation_data = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,}, Node{id: 2,} ].into_boxed_slice();
    /// create_dir_all("cache");
    /// let path = Path::new("cache/relation_id");
    /// let relation = MaterializedRelation::create_file_backed_presorted_mat_relation(column_name, &*relation_data, path, vec![(0, SortingOrder::Ascending), (1, SortingOrder::Descending)]);
    /// ```
    pub fn create_file_backed_presorted_mat_relation(
        column_names: Vec<String>,
        nodes: &[Node],
        file_path: &Path,
        sort_order: Vec<(usize, SortingOrder)>,
    ) -> Result<MaterializedRelation<'static>, String> {
        let mut file_meta_path = PathBuf::from(file_path);
        file_meta_path.set_extension("json");
        let mut new_rel = MaterializedRelation {
            column_names,
            sorted_after: sort_order,
            path_to_meta: Some(file_meta_path),
            data: Data::FileOwned(unsafe { FileVec::from_slice(file_path, nodes) }),
        };
        new_rel.update_meta_file();
        Ok(new_rel)
    }

    pub fn from_filevec(
        column_names: Vec<String>,
        nodes: FileVec<Node>,
        sort_order: Vec<(usize, SortingOrder)>,
    ) -> Result<MaterializedRelation<'static>, String> {
        let mut file_meta_path = PathBuf::from(nodes.get_path().unwrap());
        file_meta_path.set_extension("json");
        let mut new_rel = MaterializedRelation {
            column_names,
            sorted_after: sort_order,
            path_to_meta: Some(file_meta_path),
            data: Data::FileOwned(nodes),
        };
        new_rel.update_meta_file();
        Ok(new_rel)
    }


    pub fn create_presorted_mat_relation(
        column_names: Vec<String>,
        nodes: Vec<Node>,
        sort_order: Vec<(usize, SortingOrder)>,
    ) -> MaterializedRelation<'static> {
        MaterializedRelation {
            column_names,
            sorted_after: sort_order,
            data: Data::Owned(Vec::from(nodes).into()),
            path_to_meta: None,
        }
    }

    /// Loads and returns an existing file-backed materialized_relation, whose data will be loaded from a file and memory-mapped to RAM.
    ///
    /// Given file_path must have been used with [create_file_backed_mat_relation()]. Otherwise an error will be thrown.
    ///
    /// # Errors
    /// Returns an error with corresponding error message, if:
    /// - the read of the meta file fails
    /// - the serialization of the meta file fails
    ///
    /// # Panics
    ///
    /// Panics, when file_path can't be converted to a &str.
    /// TODO: What does file_vec::open does on panic or error?
    ///
    /// # Example
    ///
    /// ```
    /// use std::path::Path;
    /// use std::fs::create_dir_all;
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// create_dir_all("cache");
    /// let path = Path::new("cache/relation_id");
    /// let relation = MaterializedRelation::load_file_backed_mat_relation(path);
    /// ```
    pub fn load_file_backed_mat_relation(
        file_path: &Path,
    ) -> Result<MaterializedRelation<'static>, String> {
        let mut file_meta_path = PathBuf::from(file_path);
        let file_path_str = match file_path.to_str() {
            Some(value) => value,
            None => panic!("Can't convert filepath to &str"),
        };
        file_meta_path.set_extension("json");
        let meta_input = match File::open(&file_meta_path) {
            Err(e) => {
                return Err("Reading of file ".to_owned()
                    + file_path_str
                    + " failed: "
                    + &e.to_string())
            }
            Ok(f) => f,
        };
        let meta_data =
            match serde_json::from_reader::<_, (Vec<String>, Vec<(usize, SortingOrder)>, usize)>(
                &meta_input,
            ) {
                Err(e) => {
                    return Err("Serialize of file ".to_owned()
                        + file_path_str
                        + " failed: "
                        + &e.to_string())
                }
                Ok(data) => data,
            };
        Ok(MaterializedRelation {
            column_names: meta_data.0,
            sorted_after: meta_data.1,
            path_to_meta: Some(file_meta_path),
            data: Data::FileOwned(unsafe {
                FileVec::open(file_path, meta_data.2, Some(meta_data.2))
            }),
        })
    }

    /// Returns the stored row data as non-mut slice
    ///
    /// # Example
    /// ```
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["subject".to_string(), "predicate".to_string(), "object".to_string()];
    /// let relation_data = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 2,} ].into_boxed_slice();
    /// let relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// let second_node = relation.as_slice()[1];
    /// assert!(second_node == Node{id: 1,});
    /// ```
    pub fn as_slice(&self) -> &[Node] {
        &*self.data
    }

    /// Returns a relation only containing the rows with selected node in a column.
    ///
    /// Consumes the relation and returns a new relation with data in RAM.
    /// Represents Select operation from Database Theory.
    ///
    /// # Example
    ///
    /// ```
    /// use graphdb::storage_engine::Node;
    /// use graphdb::relation::Relation;
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    ///
    /// let column_name = vec!["col1".to_string(), "col2".to_string()];
    /// let relation_data = vec![ Node{id: 2,}, Node{id: 3,}, Node{id: 0,} , Node{id: 1,} ].into_boxed_slice();
    /// let relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// relation.print();
    /// let sel_relation = *(relation.select(Node{id: 1,}, "col2"));
    /// sel_relation.print();
    /// assert!(sel_relation.get_row(0) == [Node{id: 0,} , Node{id: 1,}]);
    /// ```
    ///
    /// # Additional Notes
    /// [binary_search()] is most likely preferred, since it does the same as this function without consuming the relation.
    pub fn select(self, sel_node: Node, column: &str) -> Box<impl Relation> {
        let len = self.data.len() / self.column_names.len();
        let num_col = self.column_names.len();
        let col_id = self.column_names.iter().position(|s| s == column).unwrap();

        // Compare each row with sel_node in corresponding column, if comparision is true, add to new relation.
        let mut new_data = Vec::new();
        for i in 0..len {
            let mut row: Vec<Node> = vec![Node { id: 0 }; num_col];
            row.clone_from_slice(&self.data[num_col * i..num_col * (i + 1)]);
            if row[col_id] == sel_node {
                new_data.append(&mut row);
            }
        }

        Box::new(MaterializedRelation {
            column_names: self.column_names.clone(),
            sorted_after: self.sorted_after.clone(),
            path_to_meta: None,
            data: Data::Owned(new_data),
        })
    }

    /// Compares two nodes of the same column in two different rows in the same materialized_relation
    /// Return codes for SortingOrder::Ascending:
    /// Returns -1, if row row_idx_1 <  row row_idx_2 in column col_id
    /// Returns  0, if row row_idx_1 == row row_idx_2 in column col_id
    /// Returns  1, if row row_idx_1 >  row row_idx_2 in column col_id
    fn compare_rows(
        &self,
        row_idx_1: usize,
        row_idx_2: usize,
        col_id: usize,
        sorting_order: SortingOrder,
    ) -> i8 {
        let num_cols = self.get_column_count();
        let mut first_larger =
            self.data[row_idx_1 * num_cols + col_id] > self.data[row_idx_2 * num_cols + col_id];
        let second_larger =
            self.data[row_idx_1 * num_cols + col_id] < self.data[row_idx_2 * num_cols + col_id];

        if !first_larger && !second_larger {
            return 0;
        };
        if sorting_order == SortingOrder::Descending {
            first_larger = !first_larger;
            // second_larger = !second_larger;
        };
        if first_larger {
            return 1;
        } else {
            return -1;
        }
    }

    /// Compares two nodes in different relations
    fn compare_rows_to_other(
        &self,
        self_row_idx: usize,
        other: &Box<dyn Relation<'a> + 'a>,
        other_row_idx: usize,
        shared_cols: &[(usize,usize)]
    ) -> Ordering {
        for (col_idx_1, col_idx_2) in shared_cols {
            let other_node = other[(other_row_idx,*col_idx_2)];
            let cmp = self[(self_row_idx,*col_idx_1)].cmp(&other_node);
            if cmp.is_ne() {
                return cmp;
            };
        }
        
        Ordering::Equal
    }


    /// Sorts the relation inplace by given columns.
    ///
    /// The relation is primarly sorted after the given column in first position etc.
    ///
    /// # Panics
    /// Panics when trying to sort a materialized_relation, which is a reference.
    /// TODO: Does FileVec::new_named panics?
    ///
    /// # Example
    /// ```
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["col1".to_string(), "col2".to_string()];
    /// let relation_data = vec![ Node{id: 2,}, Node{id: 3,}, Node{id: 0,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// relation.print();
    /// relation.merge_sort(vec![(1, SortingOrder::Ascending)]);
    /// relation.print();
    /// assert!(relation.get_row(0) == [Node{id: 0,} , Node{id: 1,}]);
    /// assert!(relation.get_row(1) == [Node{id: 2,} , Node{id: 3,}]);
    /// ```
    // This function handles handling of sorted_after and meta file, while single_merge_sort is the true sort.
    pub fn merge_sort(&mut self, columns: Vec<(usize, SortingOrder)>) {
        println!("Starting a merge sort...");
        // Check this relation is already sorted after given column to sort
        if self.is_sorted_after(&columns) {
            println!("...and stop it, since the relation was already sorted.");
            return;
        }
        println!("...and continue sorting it...");
        // Sort relation to wished sort order
        for (column, sorting_order) in columns
            .iter()
            .rev()
            .collect::<Vec<&(usize, SortingOrder)>>()
        {
            self.single_merge_sort(*column, *sorting_order);
            // Save the new sort order in sorted_after
            update_sort_order_of_column(&mut self.sorted_after, (*column, *sorting_order));
            // To update sorted_after
            self.update_meta_file();
        }
        println!("...and sorted it!");
    }

    /// Sorts after one column without consulting the sorted_after vector or updating the meta file.
    fn single_merge_sort(&mut self, column: usize, sorting_order: SortingOrder) {
        // Index of the first row of the relation
        let start_index = 0;
        // Index of the final row of the relation
        let end_index = self.get_row_count();
        let num_cols = self.get_column_count();

        // Edge case: None or one element
        if end_index == 0 || start_index >= end_index - 1 {
            return;
        }
        // Create a secondary relation or file as sorting workspace
        let mut sort_to: Data = match self.data {
            Data::Ref(_) => panic!("Sorting of Ref-type relations is not supported."),
            Data::Owned(_) => Data::Owned(
                std::iter::repeat(Node { id: 0 })
                    .take(self.data.len())
                    .collect(),
            ),
            Data::FileOwned(ref file_vec) => Data::FileOwned(unsafe {
                let mut to = FileVec::new_named(Path::new(PATH_TMP_RELATION), file_vec.capacity());
                to.set_len(file_vec.len());
                to
            }),
        };
        // This gives the size of the current blocks. A block contains n correctly sorted nodes.
        let mut arr_size = 1_usize;
        // Sort until the array is made of one sorted block
        // One Iteration of this loop sorts the whole relation in 2 * arr_size-d blocks
        loop {
            // Index of the next unfilled Node in the sorting workspace
            let mut to_idx = 0_usize;
            // Index of the next to be sorted element of the 1. block
            let mut cur_idx_1 = start_index;
            // This while_loop sorts the next 2 blocks in the relation and saved the result in the sorting workspace
            while cur_idx_1 < end_index {
                // End of the 1. block
                let cur_idx_1_end = std::cmp::min(cur_idx_1 + arr_size, end_index);
                // Of the second block
                let mut cur_idx_2 = cur_idx_1_end;
                let cur_idx_2_end = std::cmp::min(cur_idx_2 + arr_size, end_index);

                // Sort 1. & 2. block into sorting workspace until one block is copied completely
                while cur_idx_1 < cur_idx_1_end && cur_idx_2 < cur_idx_2_end {
                    let comparision =
                        self.compare_rows(cur_idx_1, cur_idx_2, column, sorting_order);
                    if comparision <= 0 {
                        for i in 0..num_cols {
                            sort_to[to_idx * num_cols + i] = self.data[cur_idx_1 * num_cols + i];
                        }
                        to_idx += 1;
                        cur_idx_1 += 1;
                    } else {
                        for i in 0..num_cols {
                            sort_to[to_idx * num_cols + i] = self.data[cur_idx_2 * num_cols + i];
                        }
                        to_idx += 1;
                        cur_idx_2 += 1;
                    }
                }

                // Add remaining 1. or 2. block to sorting workspace
                while cur_idx_1 < cur_idx_1_end {
                    for i in 0..num_cols {
                        sort_to[to_idx * num_cols + i] = self.data[cur_idx_1 * num_cols + i];
                    }
                    to_idx += 1;
                    cur_idx_1 += 1;
                }
                while cur_idx_2 < cur_idx_2_end {
                    for i in 0..num_cols {
                        sort_to[to_idx * num_cols + i] = self.data[cur_idx_2 * num_cols + i];
                    }
                    to_idx += 1;
                    cur_idx_2 += 1;
                }

                // Prepare for next merge by starting next 2 blocks from the end of the 2. block
                cur_idx_1 = cur_idx_2_end;
            }

            // Everything has been sorted in sort_to in 2*arr_size-d blocks on sort_to.
            // Swap out the previously created sorting workspace with the data of the relation.
            match &mut self.data {
                Data::Ref(_) => unreachable!(),
                Data::Owned(_) => {
                    std::mem::swap(&mut self.data, &mut sort_to);
                }
                Data::FileOwned(file_vec) => {
                    if let Data::FileOwned(file_vec_to) = &mut sort_to {
                        file_vec.swap(file_vec_to)
                    }
                }
            }
            arr_size *= 2;
            if arr_size >= end_index {
                // The whole relation consists of one sorted block, which means that the relation is sorted now.
                break;
            }
        } // End of loop {}
    }

    /// Returns a 3-tuple of:
    /// - Pairs of identically named columns with syntax (<own_column_id>, <other_column_id>),
    /// - the column names of the merged relation
    /// - column ids from the other relation, whose columns have to be copied to the merged relation
    fn find_shared_columns_for_join(
        &self,
        other: &Box<dyn Relation + 'a>,
    ) -> (Vec<(usize, usize)>, Vec<String>, Vec<usize>) {
        

        let mut shared_cols: Vec<(usize, usize)> = Vec::new();
        let s_num_col = self.get_column_count();
        let o_num_col = other.get_column_count();
        let s_cols = self.get_attributes();
        let o_cols = other.get_attributes();

        // Find pairs of identically named columns in both relations and enter pairs in shared_rows
        for i in 0..s_num_col {
            if s_cols[i] == "ID" {
                continue;
            }
            for j in 0..o_num_col {
                if s_cols[i as usize] == o_cols[j as usize] {
                    shared_cols.push((i, j));
                }
            }
        }

        let mut new_columns: Vec<String> = self.get_attributes().to_vec().clone();
        let mut included_o_col: Vec<usize> = Vec::new();
        // loop over columns of other relation and see, if it has a identically named pair in self.
        for j in 0..o_num_col {
            let mut merged = false;
            for i in 0..shared_cols.len() {
                if j == shared_cols[i].1 {
                    // column can be dropped, since there is a same named column in self
                    merged = true;
                    break;
                }
            }
            if !merged {
                // column has to be retained in merged relation
                new_columns.push(o_cols[j].clone());
                included_o_col.push(j);
            }
        }
        (shared_cols, new_columns, included_o_col)
    }

    pub fn get_column_index_by_name(&self, name: &str) -> Option<usize>{
        self.column_names.iter().position( |x| {x == name})
    }

    /// Inner joins this relation inplace using O(n^2) loop join.
    /// Attemps to calculate cartesian product of the relation
    /// and only returns the rows, whose shared column pairs have the same value.
    /// Resulting relations contains all columns from original relation and
    /// all columns from the other relation, which don't share name with a own column.
    #[allow(dead_code)]
    fn loop_join(&mut self, other: Box<dyn Relation + 'a>) {
        let s_num_col = self.get_column_count();
        // let o_num_col = other.get_column_count();
        let s_num_row = self.get_row_count();
        let o_num_row = other.get_row_count();
        // let o_cols = other.get_attributes();
        // find shared rows, calculate new columns names and get columns of other to be copied into merged
        let (shared_rows, new_columns, included_o_col) = self.find_shared_columns_for_join(&other);
        // let num_new_col = s_num_col + o_num_row - shared_rows.len();

        // join over cartesian product
        let mut new_data: Vec<Node> = Vec::new();
        for i in 0..s_num_row {
            for j in 0..o_num_row {
                let mut equal = true;
                for x in 0..shared_rows.len() {
                    let s_key = &self.get_row(i)[shared_rows[x as usize].0];
                    let o_key = &other.get_row(j)[shared_rows[x as usize].1];
                    if s_key != o_key {
                        equal = false;
                        break;
                    }
                }
                if equal {
                    let mut row: Vec<Node> = vec![Node { id: 0 }; s_num_col];
                    row.clone_from_slice(&self.data[(i * s_num_col)..((i + 1) * s_num_col)]);
                    for col_id in &included_o_col {
                        row.push(other.get_row(j)[*col_id].clone())
                    }
                    new_data.append(&mut row);
                }
            }
        }
        self.column_names = new_columns;
        self.data = Data::Owned(new_data);
    }

    /// Joins this relation using O(n*log(n)) sort-merge join and returns a new relation.
    /// Resulting relations contains all columns from original relation and
    /// all columns from the other relation, which don't share name with a own column.
    /// Sorts the own relation, if it isn't already sorted.
    #[allow(dead_code)]
    fn merge_join(
        self: Box<Self>,
        other_sorted: Box<dyn Relation<'a> + 'a>,
        do_left: bool,
    ) -> Box<dyn Relation<'a> + 'a> {
        let s_num_col = self.get_column_count();
        // let o_num_col = other.get_column_count();
        let s_num_row = self.get_row_count();
        let o_num_row = other_sorted.get_row_count();
        // find shared columns
        let (shared_cols, new_columns, included_o_col) =
            self.find_shared_columns_for_join(&other_sorted);

        println!("Merge join with shared cols: {:?}", shared_cols);
        // Edge case: No identical column names
        if shared_cols.len() == 0 {
            todo!("Compute cartesian product")
        }

        let mut new_data: Vec<Node> = Vec::new();
        let mut s_idx = 0 as usize;
        let mut o_idx = 0 as usize;
        // progress through both both relations and write pairs in new_data
        while s_idx < s_num_row {
            // Special case. Adding rows from left relationship in the left join when right relationship is at the end (or empty)
            if do_left && o_idx >= o_num_row {
                // add row to new data
                new_data.extend_from_slice(
                    &self.data[(s_idx * s_num_col)..((s_idx + 1) * s_num_col)],
                );
                // Extend with NULL columns for the missing row in the other relation
                new_data.extend(included_o_col.iter().map(|_| Node { id: 0 }));
                s_idx += 1;
                continue;
            }
            // Lexically sort rows by multiple columns
            let comparision = self.compare_rows_to_other(
                s_idx,
                &other_sorted,
                o_idx,
                &shared_cols,
            );
            // get next left row, if left row is smaller
            if comparision.is_lt() && s_idx < s_num_row {
                if do_left {
                    // add row to new data
                    new_data.extend_from_slice(
                        &self.data[(s_idx * s_num_col)..((s_idx + 1) * s_num_col)],
                    );
                    // Extend with NULL columns for the missing row in the other relation
                    new_data.extend(included_o_col.iter().map(|_| Node { id: 0 }));
                }
                s_idx += 1;
            }
            // same for right row
            else if comparision.is_gt() && o_idx < o_num_row {
                o_idx += 1;
            }
            // if both rows are the same, multiple rows with identical value can now follow
            else if comparision.is_eq() {
                // Loop over all right rows with the same value, enter the pair in new_data,
                // then restore previous right row and increment left row
                let mut inner_o_idx = o_idx;
                loop {
                    // add row to new data
                    new_data.extend_from_slice(
                        &self.data[(s_idx * s_num_col)..((s_idx + 1) * s_num_col)],
                    );
                    for col_id in &included_o_col {
                        new_data.push(other_sorted.get_row(inner_o_idx)[*col_id].clone())
                    }

                    inner_o_idx += 1;
                    if inner_o_idx == o_num_row {
                        break;
                    }
                    let comparision = self.compare_rows_to_other(
                        s_idx,
                        &other_sorted,
                        inner_o_idx,
                        &shared_cols,
                    );
                    if comparision.is_ne() {
                        break;
                    }
                }
                s_idx += 1;
            }
        }
        Box::new(MaterializedRelation {
            column_names: new_columns,
            path_to_meta: None,
            data: Data::Owned(new_data),
            // Was sorted primarly after self columns
            sorted_after: self.sorted_after.clone(),
        })
    }

    /// Returns Relation that contains all rows that have the target node in the specified column by reference.
    ///
    /// The relation must be primarly sorted by that column, otherwise undefined behaviour will occur.
    /// When the search fails or node of value target is found, then returns an empty relation with the same column names as self.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let column_name = vec!["subject".to_string(), "id".to_string()];
    /// let relation_data = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// relation.merge_sort(vec![(0, SortingOrder::Ascending)]);
    /// let ref_relation = relation.binary_search(&[0 as usize], &[Node{id: 42,}]);
    /// assert!(ref_relation.get_row(0) == [Node{id: 42,} , Node{id: 1,}]);
    /// ```
    /// TODO: Return Error or panic!()?
    pub fn binary_search(
        &'a self,
        columns: &[usize],
        targets: &[Node],
    ) -> Box<MaterializedRelation<'a>> {
        // Edge-case: No rows
        if self.get_row_count() == 0 {
            return Box::new(MaterializedRelation {
                column_names: self.column_names.clone(),
                sorted_after: vec![(0 as usize, SortingOrder::Ascending); 0],
                path_to_meta: None,
                data: Data::Ref(&self.data[0..0]),
            });
        }
        let col_len = self.get_column_count();
        let mut min = 0;
        let mut max = self.get_row_count() - 1;
        for i in 0..columns.len() {
            let column = columns[i];
            let target = targets[i];
            if target < self.get_row(min)[column] || target > self.get_row(max)[column] {
                //if value is not between first and last return
                return Box::new(MaterializedRelation::create_mat_relation(
                    self.column_names.to_vec(),
                    &[] as &[Node; 0],
                ));
            }
            let mut low = min;
            let mut high = max;
            let mut found = false;
            // Find a row that matches search
            while low <= high {
                let m = (low + high) / 2;
                let n = self.get_row(m)[column];
                if target < n {
                    // Continue search in lower half
                    high = m - 1;
                } else if target > n {
                    // Continue search in upper half
                    low = m + 1;
                } else {
                    found = true;
                    break;
                }
            }
            if !found {
                //if node is not found return empty relation
                return Box::new(MaterializedRelation::create_mat_relation(
                    self.column_names.to_vec(),
                    &[] as &[Node; 0],
                ));
            }
            // Find start of block
            let mut low_start = low;
            let mut high_start = high;
            let mut start = (low_start + high_start) / 2;
            while low_start <= high_start {
                start = (low_start + high_start) / 2;
                let n = self.get_row(start)[column];
                if target < n {
                    // Continue search in lower half
                    high_start = start - 1;
                } else if target > n {
                    // Continue search in upper half
                    low_start = start + 1;
                } else if start > min && n == self.get_row(start - 1)[column] {
                    // If node matches but is not first node that matches, search in lower half
                    high_start = start - 1;
                } else {
                    break;
                }
            }
            // Find end of block
            let mut low_end = low;
            let mut high_end = high;
            let mut end = (low_end + high_end) / 2;
            while low_end <= high_end {
                end = (low_end + high_end) / 2;
                let n = self.get_row(end)[column];
                if target < n {
                    // Continue search in lower half
                    high_end = end - 1;
                } else if target > n {
                    // Continue search in upper half
                    low_end = end + 1;
                } else if end < max && n == self.get_row(end + 1)[column] {
                    // If node matches but is not last node that matches, search in upper half
                    low_end = end + 1;
                } else {
                    break;
                }
            }
            min = start;
            max = end;
        }
        // Sorted_after
        let mut new_sorted_after = self.sorted_after.clone();
        for const_column_id in columns {
            update_sort_order_of_column(
                &mut new_sorted_after,
                (*const_column_id, SortingOrder::Constant),
            );
        }
        Box::new(MaterializedRelation {
            column_names: self.column_names.clone(),
            sorted_after: new_sorted_after,
            path_to_meta: None,
            data: Data::Ref(&self.data[min * col_len..max * col_len + col_len]),
        })
    }

    /// Clones this relation into a owned relation.
    ///
    /// Works on referencing or normal relations, not on file-backed relation.
    ///
    /// # Panics
    /// Panics when exceuted on a file-backed relation.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let column_name = vec!["subject".to_string(), "id".to_string()];
    /// let relation_data = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// let copy_relation = relation.clone_to_owned();
    /// ```
    /// TODO: Change to clone()?
    pub fn clone_to_owned(&'a self) -> Box<MaterializedRelation<'static>> {
        let new_data = match &self.data {
            //this should be removed when deref mut is implemnted for Data::Ref
            Data::Ref(x) => *x,
            Data::Owned(x) => x,
            Data::FileOwned(_) => panic!(),
        };
        let copied_data = new_data.to_owned();
        let new_rel =
            Self::create_mat_relation(self.column_names.to_vec(), &copied_data.as_slice());
        Box::new(new_rel)
    }

    /// Creates reference relation to the whole data of relation.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let column_name = vec!["subject".to_string(), "id".to_string()];
    /// let relation_data = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// let copy_relation = relation.create_reference();
    /// ```
    pub fn create_reference(&'a self) -> Box<MaterializedRelation<'a>> {
        Box::new(MaterializedRelation {
            column_names: self.column_names.clone(),
            sorted_after: self.sorted_after.clone(),
            path_to_meta: None,
            data: Data::Ref(&self.data),
        })
    }

    /// Update the meta file or write new meta file if no one exists.
    /// Does nothing for not FileVec Relations
    fn update_meta_file(&mut self) {
        let file_vec = match &self.data {
            FileOwned(fv) => fv,
            Owned(..) => return,
            Ref(..) => return,
        };
        let path_meta = match &self.path_to_meta {
            Some(val) => val,
            None => return,
        };
        let meta_output = match File::create(&path_meta).map_err(|err| err.to_string()) {
            Ok(val) => val,
            Err(e) => {
                println!(
                    "Failed to open meta file to {0}, error message: {1}",
                    path_meta.display(),
                    e
                );
                return;
            }
        };
        match serde_json::to_writer(
            &meta_output,
            &(&self.column_names, &self.sorted_after, file_vec.len()),
        )
        .map_err(|err| err.to_string())
        {
            Ok(_val) => (),
            Err(e) => {
                println!(
                    "Failed to write meta file to {0}, error message: {1}",
                    path_meta.display(),
                    e
                );
                return;
            }
        };
    }

    /// Returns sorted_after-vector, which lists the columns the relation is currently sorted after
    ///
    /// # Example
    /// ```
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["subject".to_string(), "id".to_string()];
    /// let relation_data = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// relation.merge_sort(vec![(1, SortingOrder::Descending)]);
    /// println!("Sort order of relation: {:?}", relation.get_sorted_after());
    /// ```
    pub fn get_sorted_after(&'a self) -> &'a Vec<(usize, SortingOrder)> {
        &self.sorted_after
    }

    /// Return true, if the relation is already sorted after given sort order, resulting into no sort at merge_join or merge_sort.
    ///
    /// Ascending columns of asked_sort_order requires, that the respective column of the own relation is also ascending or constant.
    /// This also applied to descending columns.
    /// Constant columns of asked_sort_order requires, that the column of the own relation is also constant.
    ///
    /// Doesn't check for out of bounds array access
    ///
    /// # Example
    /// ```
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    /// use graphdb::storage_engine::{Node};
    ///
    /// let column_name = vec!["subject".to_string(), "id".to_string()];
    /// let relation_data = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
    /// relation.merge_sort(vec![(1, SortingOrder::Descending)]);
    /// assert!(relation.is_sorted_after(&vec![(1, SortingOrder::Descending)]));
    /// ```
    pub fn is_sorted_after(&self, asked_sort_order: &Vec<(usize, SortingOrder)>) -> bool {
        // Get own constant columns
        let mut own_const_col = vec![0 as usize; 0];
        for (col_id, so) in &self.sorted_after {
            if *so == SortingOrder::Constant {
                own_const_col.push(*col_id);
            }
        }
        let mut own_index: usize = 0;
        if self.sorted_after.len() >= asked_sort_order.len() {
            // Iterate each entry of asked sort order and check if they are fulfilled
            for (oth_col_id, oth_so) in asked_sort_order {
                // This entry is immediately fulfilled if own column is const
                if own_const_col.contains(oth_col_id) {
                    continue;
                }
                // Skip constant columns in own sort order
                while own_index < self.sorted_after.len()
                    && self.sorted_after[own_index].1 == SortingOrder::Constant
                {
                    own_index += 1;
                }
                // Stop if own list runs out
                if own_index >= self.sorted_after.len() {
                    return false;
                }
                // Check if current entry in asked_sort_order can be fulfilled
                let requirement_fulfilled = match oth_so {
                    SortingOrder::Ascending | SortingOrder::Descending => {
                        self.sorted_after[own_index] == (*oth_col_id, *oth_so)
                    }
                    SortingOrder::Constant => return false,
                };
                if !requirement_fulfilled {
                    return false;
                }
                own_index += 1;
            }
            return true;
        }
        return false;
    }

    pub fn into_vec_iter(self) -> RelationVecIterator<'a> {
        RelationVecIterator::new(self)
    }

    /// Sorts the relation lexicographically.
    pub fn sort_lexicographic(&mut self) {
        if matches!(self.data, Data::FileOwned{..}) && self.get_row_count()<9 {
            // The dance with temp_data and new_data is to move FileVec temporarily 
            // out of data, which is not possible behind a reference.
            let mut temp_data = Data::Owned(Vec::new());
            let mut new_data;
            std::mem::swap(&mut temp_data, &mut self.data);
            if let Data::FileOwned(filevec) = temp_data {
                new_data = match self.get_row_count() {
                    1 => Data::FileOwned(sort_filevec_lexicographic::<1>(filevec)),
                    2 => Data::FileOwned(sort_filevec_lexicographic::<2>(filevec)),
                    3 => Data::FileOwned(sort_filevec_lexicographic::<3>(filevec)),
                    4 => Data::FileOwned(sort_filevec_lexicographic::<4>(filevec)),
                    5 => Data::FileOwned(sort_filevec_lexicographic::<5>(filevec)),
                    6 => Data::FileOwned(sort_filevec_lexicographic::<6>(filevec)),
                    7 => Data::FileOwned(sort_filevec_lexicographic::<7>(filevec)),
                    8 => Data::FileOwned(sort_filevec_lexicographic::<8>(filevec)),
                    _ => unreachable!()
                }
            } else {
                unreachable!()
            }
            std::mem::swap(&mut new_data, &mut self.data);
        }

        let mut columns = Vec::with_capacity(self.get_column_count());
        for i in 0..self.get_column_count() {
            columns.push((i, SortingOrder::Ascending));
        }
        self.merge_sort(columns);
    }
}

fn sort_filevec_lexicographic<const K: usize>(filevec: FileVec<Node>) -> FileVec<Node> {
    let mut sliced_filevec: FileVec<[Node;K]> = filevec.into();
    sliced_filevec.parallel_sort();

    sliced_filevec.into()
}


/// Updates sorting order of column
/// Constant columns remain unchanged
fn update_sort_order_of_column(
    sorted_after: &mut Vec<(usize, SortingOrder)>,
    (column, sorting_order): (usize, SortingOrder),
) {
    // Remove entry of column, if it already is in sorted_after
    let idx_s_a = sorted_after.iter().position(|x| x.0 == column);
    match idx_s_a {
        Some(idx) => {
            if sorted_after[idx].1 == SortingOrder::Constant {
                // Stop if constant
                return;
            }
            drop(sorted_after.remove(idx))
        }
        _ => (),
    };
    // Add new column id to front
    sorted_after.push((column, sorting_order));
    sorted_after.rotate_right(1);
}

impl<'a> Relation<'a> for MaterializedRelation<'a> {
    /// Consumes two relations and joins them on same named rows.
    ///
    /// If there is no common column, then create cartesian product.
    /// The identity of this operation is the inputted relation.
    /// Columns named 'id' (or any variation on case like 'iD' or 'ID) are ignored in join.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let cm1 = vec!["subject".to_string(), "_id".to_string()];
    /// let rd1 = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// let cm2 = vec!["_id".to_string(), "object".to_string()];
    /// let rd2 = vec![ Node{id: 0,}, Node{id: 14,}, Node{id: 1,} , Node{id: 43,} ].into_boxed_slice();
    /// let mut rel2 = MaterializedRelation::create_mat_relation(cm2, &*rd2);
    /// rel1.print();
    /// rel2.print();
    /// let result = Box::new(rel1).join(Box::new(rel2), false);
    /// result.print();
    /// assert!(result.get_row(0) == [Node{id: 13,} , Node{id: 0,}, Node{id: 14, }]);
    /// assert!(result.get_row(1) == [Node{id: 42,} , Node{id: 1,}, Node{id: 43, }]);
    /// ```
    fn join(self: Box<Self>, other: Box<dyn Relation<'a> + 'a>, do_left: bool) -> Box<dyn Relation<'a> + 'a> {
        let o_cols = other.get_attributes().to_owned();
        let (shared_rows, _new_columns, _included_o_col) = self.find_shared_columns_for_join(&other);
        let own_sort_order = shared_rows
            .iter()
            .map(|elem| (elem.0, SortingOrder::Ascending))
            .collect();
        let other_sort_order = shared_rows
            .iter()
            .map(|elem| ((o_cols[elem.1]).as_str(), SortingOrder::Ascending))
            .collect();
        println!("=== STARTING MERGE JOIN ==");
        println!("= Expected own sort order: {:?}", own_sort_order);
        println!("= Expected other sort order: {:?}", other_sort_order);
        println!("= Given own sort order: {:?}", self.sorted_after);
        // Should only clone relation if it's unsorted
        let other_sorted = other.sort(other_sort_order);
        // Should only clone if necessary
        if !self.is_sorted_after(&own_sort_order) {
            println!("= Sorting self...");
            let mut self_sorted = self.clone_to_owned();
            self_sorted.merge_sort(own_sort_order);
            let sort_result = self_sorted.merge_join(other_sorted, do_left);
            println!("=== ENDING MERGE JOIN ==");
            return sort_result;
        } else {
            println!("= Self already sorted...");
            let self_sorted = self;
            println!("=== ENDING MERGE JOIN ==");
            return self_sorted.merge_join(other_sorted, do_left);
        }
    }

    /// Consume relation and returns sorted relation.
    ///
    /// Accepts a vector of pairs of column names and sorting order.
    /// Returned relation will be primarly sorted by first element in list, secondarly by second element etc.
    ///
    /// # Panics
    /// Panics, when aksed_sort_order contains a tuple with [SortingOrder::Constant].
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let cm1 = vec!["subject".to_string(), "_id".to_string()];
    /// let rd1 = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// rel1.print();
    /// let result = Box::new(rel1).sort(vec![("subject", SortingOrder::Descending)]);
    /// result.print();
    /// assert!(result.get_row(0) == [Node{id: 42,} , Node{id: 1,}]);
    /// assert!(result.get_row(1) == [Node{id: 13,} , Node{id: 0,}]);
    /// ```
    fn sort(
        self: Box<Self>,
        asked_sort_order: Vec<(&str, SortingOrder)>,
    ) -> Box<dyn Relation<'a> + 'a> {
        println!("=== STARTING SORT ==");
        let mut merge_sort_para = Vec::new();
        // convert column names to col_ids
        for column in asked_sort_order.iter() {
            if column.1 == SortingOrder::Constant {
                panic!("sort() of materialized_relation doesn't accept sort orders with SortingOrder::Constant");
            }
            let col_id = self
                .column_names
                .iter()
                .position(|s| s == column.0)
                .unwrap();
            merge_sort_para.push((col_id, column.1));
        }
        // Return relation, if it already is sorted
        if self.is_sorted_after(&merge_sort_para) {
            println!("=== ENDING SORT, already sorted ==");
            return self;
        }
        let mut new_rel = self.clone_to_owned();
        new_rel.merge_sort(merge_sort_para);
        println!("=== ENDING SORT, normal ==");
        new_rel
    }

    /// Consume the relation and return relation with only given rows.
    ///
    /// # Panics
    /// Panics, when columns contains duplicate string, to prevent relation with duplicate column names.
    /// Panics, when columns contains a name, for which a column in self doesn't exist.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let cm1 = vec!["subject".to_string(), "_id".to_string()];
    /// let rd1 = vec![ Node{id: 13,}, Node{id: 0,}, Node{id: 42,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// rel1.print();
    /// let result = Box::new(rel1).project(vec!["subject"]);
    /// result.print();
    /// assert!(result.get_row(0) == [Node{id: 13,}]);
    /// assert!(result.get_row(1) == [Node{id: 42,}]);
    /// ```
    fn project(self: Box<Self>, columns: Vec<&str>) -> Box<dyn Relation<'a> + 'a> {
        let num_row = self.get_row_count();
        let num_col = self.column_names.len();
        let mut col_ids = Vec::new();
        let mut new_columns = Vec::new();
        for column in &columns {
            let col_id = match self.column_names.iter().position(|s| s == column){
                Some(id) => id, 
                None => panic!("tried to project non-extistent column: {} ", column),
            };
            if col_ids.contains(&col_id) {
                panic!(
                    "Trying to project the same column '{0}' twice in a materialized_relation!",
                    *column
                );
            }
            col_ids.push(col_id);
            new_columns.push(self.column_names[col_id].clone());
            println!("Select column id {}", col_id);
        }
        // Compare each row : row[index] with sel_node, if yes, add to new relation.
        let mut new_data = Vec::new();
        // let num_new_col = col_ids.len();
        for i in 0..num_row {
            let mut row: Vec<Node> = Vec::new();
            for col_id in &col_ids {
                row.push(self.data[i as usize * num_col + col_id].clone());
            }
            new_data.append(&mut row);
        }
        // Filter out ids in sorted_after, which don't exist anymore
        let mut new_sorted_after = Vec::new();
        for x in self.sorted_after.iter() {
            if col_ids.contains(&x.0) {
                new_sorted_after.push(*x);
            }
        }
        Box::new(MaterializedRelation {
            column_names: new_columns,
            sorted_after: new_sorted_after,
            path_to_meta: None,
            data: Data::Owned(new_data),
        })
    }

    /// Consume this relation and return relation without duplicate rows and preserved ordering
    ///
    /// Has a quadratic runtime.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let cm1 = vec!["_id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let mut rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// rel1.print();
    /// let result = Box::new(rel1).remove_duplicate_indices();
    /// result.print();
    /// assert!(result.get_row_count() == 3);
    /// ```
    // TODO: Improve runtime by sorting relation and then removing entries.
    fn remove_duplicate_indices(self: Box<Self>) -> Box<dyn Relation<'a> + 'a> {
        let num_row = self.get_row_count();
        let num_col = self.column_names.len();
        let mut new_data = Vec::new();
        for i in 0..num_row {
            let mut duplicate = false;
            let s_row = self.get_row(i);
            for j in 0..i {
                let o_row = self.get_row(j);
                if s_row == o_row {
                    duplicate = true;
                    break;
                }
            }
            if !duplicate {
                let mut row: Vec<Node> = vec![Node { id: 0 }; num_col];
                row.clone_from_slice(&self.get_row(i));
                new_data.append(&mut row);
            }
        }
        Box::new(MaterializedRelation {
            column_names: self.column_names.clone(),
            sorted_after: self.sorted_after.clone(),
            path_to_meta: None,
            data: Data::Owned(new_data),
        })
    }

    /// Returns name of columns
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let cm1 = vec!["_id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// assert!(rel1.get_attributes() == vec!["_id".to_owned()]);
    /// ```
    fn get_attributes(&self) -> &[String] {
        &self.column_names
    }

    /// Return relation with renamed columns
    ///
    /// # Panic
    /// When the amount of strings of given vector differs from column count.
    /// When trying to rename a fileOwned relation
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation, SortingOrder};
    ///
    /// let cm1 = vec!["_id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// let ren_rel1 = Box::new(rel1).rename_columns(vec!["id_"]);
    /// assert!(ren_rel1.get_attributes() == vec!["id_".to_owned()]);
    /// ```
    fn rename_columns(self: Box<Self>, new_columns_names: Vec<&str>) -> Box<dyn Relation<'a> + 'a> {
        if new_columns_names.len() != self.get_column_count() {
            panic!(
                "Trying to rename {0} columns of a relation with {1} given column names.",
                self.get_column_count(),
                new_columns_names.len()
            );
        }
        match &self.data {
            Data::FileOwned(_) => panic!("May not rename FileOwned Relation"),
            _ => (),
        }
        let renamed_rel = Box::new(MaterializedRelation {
            column_names: new_columns_names.iter().map(|&x| x.into()).collect(),
            sorted_after: self.sorted_after.clone(),
            path_to_meta: self.path_to_meta,
            data: self.data,
        });
        renamed_rel
    }

    /// Returns number of rows
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation};
    ///
    /// let cm1 = vec!["subject".to_string(), "id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// assert!(rel1.get_row_count() == 3);
    /// ```
    fn get_row_count(&self) -> usize {
        if self.column_names.len() == 0 {
            return 0;
        }
        self.data.len() / self.column_names.len()
    }

    /// Returns number of columns
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation};
    ///
    /// let cm1 = vec!["subject".to_string(), "id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// assert!(rel1.get_column_count() == 2);
    /// ```
    fn get_column_count(&self) -> usize {
        self.column_names.len()
    }

    /// Returns slice contain the (row_id + 1)-th row by order numbering.
    ///
    /// # Example
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation};
    ///
    /// let cm1 = vec!["subject".to_string(), "id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// assert!(rel1.get_row(1) == &[Node{id: 3,} , Node{id: 1,}]);
    /// ```
    fn get_row(&self, row_id: usize) -> &[Node] {
        let num_col = self.column_names.len();
        &self.data[num_col * row_id..num_col * (row_id + 1)]
    }

    /// Prints out column and rows of this relation on console
    ///
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::materialized_relation::MaterializedRelation;
    /// use graphdb::relation::{Relation};
    ///
    /// let cm1 = vec!["subject".to_string(), "id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// rel1.print();
    /// ```
    fn print(&self) {
        let num_row = self.get_row_count();
        let num_col = self.column_names.len();
        println!("==PRINTING RELATION==");
        println!("Contains columns : {:?}", self.column_names);
        match &self.data {
            Data::Owned(_) => println!("Saved in RAM"),
            Data::FileOwned(_) => println!("Saved in {:?}", self.path_to_meta),
            Data::Ref(_) => println!("Referencing another relation"),
        }
        if self.sorted_after.len() > 0 {
            println!("Is sorted after : {:?}", self.sorted_after);
        } else {
            println!("Is unsorted")
        }
        if num_row > 0 {
            println!("This relation contains {} rows: ", num_row);
            for i in 0..num_row {
                for j in 0..num_col {
                    print!(
                        "{:#018x}, ",
                        self.data[i as usize * num_col + j as usize].id
                    );
                }
                println!("");
                if i > 10 {
                    println!("Cutting of print...");
                    break;
                }
            }
        } else {
            println!("This relation contains no rows.");
        }
        println!("==END OF PRINTING==");
    }
    

    fn group_by_and_aggregates(self: Box<Self>, new_cols: Vec<Pattern>, proj_cols: Vec<&str>, new_values: Vec<Vec<Node>>) -> Box<dyn Relation<'a> + 'a>{
        let path_to_meta = self.path_to_meta.clone();
        let sorted_after = self.sorted_after.clone();
        // only proj_cols left
        let rel_proj = self.project(proj_cols);

        if rel_proj.get_column_count() != 0 {
            //removing duplicates
            let mut single_rel = rel_proj.remove_duplicate_indices();

            let mut index = 0;  

            for col in new_cols{
                single_rel = single_rel.extend(col, new_values[index].clone());
                index = index + 1;
            }
        
            return single_rel;
        }
        else{
            let mut values: Vec<Node> = Vec::new();
            for row in new_values{
                for val in row{
                    values.push(val);
                }
            }

            let mut new_columns: Vec<String> = Vec::new();
            for col in new_cols{
                match col{
                    Pattern::Variable { name } => new_columns.push(name),
                    _ => ()
                }
            }

            return Box::new(MaterializedRelation {
                column_names: new_columns,
                sorted_after: sorted_after,
                path_to_meta: path_to_meta,
                data: Data::Owned(values),
            });
        }
    }


    ///creates new relation that consists of the old and a new column 
    ///with variabel as name and new_values as values
    fn extend(self: Box<Self>, variable: Pattern, new_values: Vec<Node>)-> Box<dyn Relation<'a> + 'a> {
        let column: String;
        match variable{
            Pattern::Variable { name } => column = name,
            _ => column = String::from(" "),
        }

        let mut new_columns = self.column_names.clone();
        new_columns.push(column);

        let sorted_after = self.sorted_after.clone();

        fn create_new_row(index: usize,row: Vec<Node>, new_values: &Vec<Node>) -> Vec<Node> {
            //index starts at 1
            let new_node: Node;

            //vec for new row filled with old values
            let mut new_row:Vec<Node> = Vec::new();
            new_row.extend_from_slice(&row);
            match new_values.get(index-1){
                Some(value) => {
                    new_node = *value;
                }
                None => {
                    new_node = Node{ id: 0};
                }
            }
            //calculated value added
            new_row.push(new_node);

            return new_row
        }

        //creates new relation with new rows
        let mut index = 0;
        let mut relation: MaterializedRelation =
            self.into_vec_iter().map(|row|{index += 1; create_new_row(index,row,&new_values)}).collect();

        relation.column_names = new_columns;
        relation.sorted_after = sorted_after;

        return Box::new(relation)
    }

    // Creates a new relation with the same column as self, but with the supplied data
    fn new_with_data(&self, new_data: Vec<Node>) -> Box<dyn Relation<'a> + 'a> {
        return Box::new(MaterializedRelation {
            column_names: self.column_names.clone(),
            sorted_after: self.sorted_after.clone(),
            path_to_meta: None,
            data: Data::Owned(new_data),
        });
    }


    /// Return relation with renamed attributes
    /// 
    /// # Example
    /// The example does not work any more.
    /// ```
    /// use graphdb::storage_engine::{Node};
    /// use graphdb::relation::{Relation, SortingOrder, materialized_relation::MaterializedRelation};
    /// use std::collections::HashMap;
    /// 
    /// let cm1 = vec!["_id".to_string()];
    /// let rd1 = vec![ Node{id: 0,}, Node{id: 1,}, Node{id: 3,} , Node{id: 1,} ].into_boxed_slice();
    /// let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
    /// let mut map = HashMap::new();
    /// map.insert("_id","id_");
    /// let ren_rel1 = Box::new(rel1).rename_attributes(&map);
    /// assert!(ren_rel1.get_attributes() == vec!["id_".to_owned()]);
    /// ```
    fn rename_attributes(mut self: Box<Self>, rename: &std::collections::HashMap<&str,&str>) -> Box<dyn Relation<'a> +'a> {
        self.column_names.iter_mut().for_each(|col| {
            if let Some(&new_name) = rename.get(col.as_str()) {
                *col = new_name.to_owned();
            }
        });

        self
    }


    fn to_materialized(self: Box<Self>) -> MaterializedRelation<'a> {
        *self
    }
}

impl<'a> IntoIterator for &'a MaterializedRelation<'a> {
    type Item = &'a [Node];
    type IntoIter = RelationIterator<'a>;

    fn into_iter(self) -> RelationIterator<'a> {
        RelationIterator::new(self)
    }
}

pub struct RelationVecIterator<'a> {
    relation: MaterializedRelation<'a>,
    row_count: usize,
    current_row: usize,
}

impl<'a> RelationVecIterator<'a> {
    fn new(relation: MaterializedRelation<'a>) -> RelationVecIterator<'a> {
        let row_count_temp = relation.get_row_count();
        Self {
            relation,
            row_count: row_count_temp,
            current_row: 0,
        }
    }

    pub fn get_column_names(self) -> Vec<String> {
        self.relation.column_names.clone()
    }

    pub fn len(self) -> usize {
        self.row_count
    }

    pub fn get_sorted_after(self) -> Vec<(usize, SortingOrder)> {
        self.relation.sorted_after.clone()
    }
}

impl<'a> Iterator for RelationVecIterator<'a> {
    type Item = Vec<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row < self.row_count {
            self.current_row += 1;
            Some(Vec::from(self.relation.get_row(self.current_row - 1)))
        } else {
            None
        }
    }
}

impl<'a> FromIterator<Vec<Node>> for MaterializedRelation<'a> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Vec<Node>>,
    {
        let column_names: Vec<String>;
        let mut iter = iter.into_iter();
        let mut result = Vec::with_capacity(iter.size_hint().0);
        match iter.next() {
            None => {
                return MaterializedRelation {
                    column_names: Vec::new(),
                    sorted_after: Vec::new(),
                    path_to_meta: None,
                    data: Data::Owned(Vec::new()),
                };
            }

            Some(row) => {
                column_names = (0..row.len()).map(|x| x.to_string()).collect();
                result.extend(row);
            }
        }
        for row in iter {
            result.extend(row);
        }

        MaterializedRelation {
            column_names,
            sorted_after: Vec::new(),
            path_to_meta: None,
            data: Data::Owned(result),
        }
    }
}

impl Index<(usize,usize)> for MaterializedRelation<'_> {
    type Output=Node;

    fn index(&self, index: (usize,usize)) -> &Self::Output {
        &self.get_row(index.0)[index.1]
    }
}

#[cfg(test)]
mod test {
    // TODO: Reuse the test somehow for all implementations of Relation.

    use super::{MaterializedRelation, Relation, SortingOrder};
    use crate::storage_engine::Node;
    use crate::relation::Pattern;

    fn debug_create_relation(id: u8) -> Box<MaterializedRelation<'static>> {
        let n0 = Node { id: 0 };
        let q1 = Node { id: 11 };
        let q2 = Node { id: 12 };
        let q3 = Node { id: 13 };
        let q4 = Node { id: 14 };
        let p1 = Node { id: 21 };
        let p2 = Node { id: 22 };
        let rel = match id {
            1 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                ],
                &[q1, p1, q2, q2, p2, q3, q3, p1, q4, q4, p2, q1],
            ),
            2 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject2".to_string(),
                    "predicat2".to_string(),
                    "subject1".to_string(),
                ],
                &[q1, p1, q2, q2, p2, q3, q3, p1, q4, q4, p2, q1],
            ),
            3 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject2".to_string(),
                    "predicat2".to_string(),
                    "object2".to_string(),
                ],
                &[q1, p1, q2, q1, p2, q1, q1, p2, q1, q4, p2, q1],
            ),
            4 => MaterializedRelation::create_mat_relation(vec![], &[]),
            5 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                ],
                &[q1, p1, q2, q3, p1, q4, q2, p2, q3, q4, p2, q1],
            ),
            6 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                ],
                &[q1, p1, q2, q3, p1, q4, q2, p2, q3, q3, p1, q4],
            ),
            7 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                ],
                &[q2, p2, q3, q4, p2, q1, q1, p1, q2, q3, p1, q4],
            ),
            8 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                ],
                &[
                    q4, p2, q3, q3, p2, q1, q2, p1, q2, q1, p1, q4, n0, p2, n0, q1, n0, n0, n0, n0,
                    q1,
                ],
            ),
            9 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                ],
                &[
                    n0, p2, n0, q1, n0, n0, q3, p2, q1, n0, n0, q1, q2, p1, q2, q4, p2, q3, q1, p1,
                    q4,
                ],
            ),
            10 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                    "object1".to_string(),
                    "subject2".to_string(),
                    "predicat2".to_string(),
                ],
                &[
                    q1, p1, q2, q4, p2, q2, p2, q3, q1, p1, q3, p1, q4, q2, p2, q4, p2, q1, q3, p1,
                ],
            ),
            11 => MaterializedRelation::create_mat_relation(
                vec![
                    "subject1".to_string(),
                    "predicat1".to_string(),
                ],
                &[q1, q2, q4, p2, q1, q3, n0, q4, q4, n0, q1, q2],
            ),
            _ => panic!(),
        };
        Box::new(rel)
    }

    #[test]
    fn select_test() {
        let column_name = vec!["col1".to_string(), "col2".to_string()];
        let relation_data = vec![
            Node { id: 2 },
            Node { id: 3 },
            Node { id: 0 },
            Node { id: 1 },
        ]
        .into_boxed_slice();
        let relation = MaterializedRelation::create_mat_relation(column_name, &*relation_data);
        relation.print();
        let sel_relation = *(relation.select(Node { id: 1 }, "col2"));
        sel_relation.print();
        assert!(sel_relation.get_row(0) == [Node { id: 0 }, Node { id: 1 }]);
    }

    #[test]
    fn join_test_1() {
        let mut rel = debug_create_relation(1);
        let other = debug_create_relation(2);
        let result = rel.join(other, false);
        rel = debug_create_relation(10);
        rel.print();
        result.print();
        for i in 0..rel.get_row_count() {
            assert!(result.get_row(i) == rel.get_row(i));
        }
    }

    #[test]
    fn join_test_2() {
        let mut rel = debug_create_relation(1);
        let other = debug_create_relation(1);
        let result = rel.join(other, false);
        rel = debug_create_relation(1);
        rel.print();
        result.print();
        for i in 0..rel.get_column_count() {
            assert!(result.get_row(i) == rel.get_row(i));
        }
    }

    #[test]
    fn join_test_3() {
        let cm1 = vec!["subject".to_string(), "_id".to_string()];
        let rd1 = vec![
            Node { id: 13 },
            Node { id: 0 },
            Node { id: 42 },
            Node { id: 1 },
        ];
        let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
        let cm2 = vec!["_id".to_string(), "object".to_string()];
        let rd2 = vec![
            Node { id: 0 },
            Node { id: 14 },
            Node { id: 1 },
            Node { id: 43 },
        ]
        .into_boxed_slice();
        let rel2 = MaterializedRelation::create_mat_relation(cm2, &*rd2);
        rel1.print();
        rel2.print();
        let result = Box::new(rel1).join(Box::new(rel2), false);
        result.print();
        assert!(result.get_row(0) == [Node { id: 13 }, Node { id: 0 }, Node { id: 14 }]);
        assert!(result.get_row(1) == [Node { id: 42 }, Node { id: 1 }, Node { id: 43 }]);
    }

    //#[test]  is not valid test 
    fn join_test_4() {
        let rel1 = debug_create_relation(1);
        let rel2 = debug_create_relation(3);
        // let joined_rel = rel1.join(rel2);
        // joined_rel.print();
    }

    #[test]
    fn left_join_test_1() {
        let mut rel = debug_create_relation(1);
        let other = debug_create_relation(2);
        let result = rel.join(other, true);
        rel = debug_create_relation(10);
        rel.print();
        result.print();
        for i in 0..rel.get_row_count() {
            assert!(result.get_row(i) == rel.get_row(i));
        }
    }

    #[test]
    fn left_join_test_2() {
        let mut rel = debug_create_relation(1);
        let other = debug_create_relation(1);
        let result = rel.join(other, true);
        rel = debug_create_relation(1);
        rel.print();
        result.print();
        for i in 0..rel.get_column_count() {
            assert!(result.get_row(i) == rel.get_row(i));
        }
    }

    #[test]
    fn left_join_test_3() {
        let cm1 = vec!["subject".to_string(), "_id".to_string()];
        let rd1 = vec![
            Node { id: 13 },
            Node { id: 0 },
            Node { id: 42 },
            Node { id: 1 },
            Node { id: 44 },
            Node { id: 2 },
        ];
        let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
        let cm2 = vec!["_id".to_string(), "object".to_string()];
        let rd2 = vec![
            Node { id: 0 },
            Node { id: 14 },
            Node { id: 1 },
            Node { id: 43 },
            Node { id: 3 },
            Node { id: 45 },
        ]
        .into_boxed_slice();
        let rel2 = MaterializedRelation::create_mat_relation(cm2, &*rd2);
        rel1.print();
        rel2.print();
        let result = Box::new(rel1).join(Box::new(rel2), true);
        result.print();
        assert!(result.get_row(0) == [Node { id: 13 }, Node { id: 0 }, Node { id: 14 }]);
        assert!(result.get_row(1) == [Node { id: 42 }, Node { id: 1 }, Node { id: 43 }]);
        assert!(result.get_row(2) == [Node { id: 44 }, Node { id: 2 }, Node { id: 0 }]);
    }

    #[test]
    fn left_join_test_4() {
        let cm1 = vec!["subject".to_string(), "_id".to_string()];
        let rd1 = vec![
            Node { id: 13 },
            Node { id: 0 },
            Node { id: 42 },
            Node { id: 1 },
            Node { id: 44 },
            Node { id: 2 },
        ];
        let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
        let cm2 = vec!["_id".to_string(), "object".to_string()];
        let rd2 = vec![
            Node { id: 0 },
            Node { id: 14 },
            Node { id: 1 },
            Node { id: 43 },
        ]
        .into_boxed_slice();
        let rel2 = MaterializedRelation::create_mat_relation(cm2, &*rd2);
        rel1.print();
        rel2.print();
        let result = Box::new(rel1).join(Box::new(rel2), true);
        result.print();
        assert!(result.get_row(0) == [Node { id: 13 }, Node { id: 0 }, Node { id: 14 }]);
        assert!(result.get_row(1) == [Node { id: 42 }, Node { id: 1 }, Node { id: 43 }]);
        assert!(result.get_row(2) == [Node { id: 44 }, Node { id: 2 }, Node { id: 0 }]);
    }

    #[test]
    fn left_join_test_5() {
        let cm1 = vec!["subject".to_string(), "_id".to_string()];
        let rd1 = vec![
            Node { id: 13 },
            Node { id: 0 },
            Node { id: 42 },
            Node { id: 1 },
            Node { id: 44 },
            Node { id: 2 },
        ];
        let rel1 = MaterializedRelation::create_mat_relation(cm1, &*rd1);
        let cm2 = vec!["_id".to_string(), "object".to_string()];
        let rd2 = vec![
        ]
        .into_boxed_slice();
        let rel2 = MaterializedRelation::create_mat_relation(cm2, &*rd2);
        rel1.print();
        rel2.print();
        let result = Box::new(rel1).join(Box::new(rel2), true);
        result.print();
        assert!(result.get_row(0) == [Node { id: 13 }, Node { id: 0 }, Node { id: 0 }]);
        assert!(result.get_row(1) == [Node { id: 42 }, Node { id: 1 }, Node { id: 0 }]);
        assert!(result.get_row(2) == [Node { id: 44 }, Node { id: 2 }, Node { id: 0 }]);
    }

    #[test]
    fn rename_test_1() {
        // Successfull rename
        let rel = debug_create_relation(1);
        let new_col_names = vec!["a", "b", "c"];
        let ren_rel = rel.rename_columns(new_col_names);
        assert!(ren_rel.get_attributes() == vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
    }

    #[test]
    #[should_panic(
        expected = "Trying to rename 3 columns of a relation with 4 given column names."
    )]
    fn rename_test_2() {
        // Test error message
        let rel = debug_create_relation(1);
        rel.rename_columns(vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn sort_test_1() {
        let mut rel = debug_create_relation(1);
        rel.print();
        let sorting_order = vec![(1, SortingOrder::Ascending)];
        rel.merge_sort(sorting_order.clone());
        rel.print();
        assert!(rel.get_attributes() == debug_create_relation(1).get_attributes());
        assert!(*(rel.data) == *(debug_create_relation(5).data));
        assert!(*rel.get_sorted_after() == sorting_order);
    }

    #[test]
    fn sort_test_2() {
        let mut rel = debug_create_relation(1);
        rel.print();
        let sorting_order = vec![(1, SortingOrder::Descending)];
        rel.merge_sort(sorting_order.clone());
        rel.print();
        assert!(rel.get_attributes() == debug_create_relation(1).get_attributes());
        assert!(*(rel.data) == *(debug_create_relation(7).data));
        assert!(*rel.get_sorted_after() == sorting_order);
    }

    #[test]
    fn sort_test_3() {
        let mut rel = debug_create_relation(8);
        rel.print();
        let sorting_order = vec![(2, SortingOrder::Ascending)];
        rel.merge_sort(sorting_order.clone());
        rel.print();
        assert!(rel.get_attributes() == debug_create_relation(8).get_attributes());
        assert!(*(rel.data) == *(debug_create_relation(9).data));
        assert!(*rel.get_sorted_after() == sorting_order);
    }

    #[test]
    fn get_sorted_after_test_1() {
        let mut rel = debug_create_relation(8);
        rel.print();
        rel.merge_sort(vec![
            (2, SortingOrder::Ascending),
            (1, SortingOrder::Descending),
        ]);
        assert!(
            *rel.get_sorted_after()
                == vec![(2, SortingOrder::Ascending), (1, SortingOrder::Descending)]
        );
        rel.merge_sort(vec![(0, SortingOrder::Descending)]);
        assert!(
            *rel.get_sorted_after()
                == vec![
                    (0, SortingOrder::Descending),
                    (2, SortingOrder::Ascending),
                    (1, SortingOrder::Descending)
                ]
        );
        rel.merge_sort(vec![(2, SortingOrder::Descending)]);
        assert!(
            *rel.get_sorted_after()
                == vec![
                    (2, SortingOrder::Descending),
                    (0, SortingOrder::Descending),
                    (1, SortingOrder::Descending)
                ]
        );
    }

    #[test]
    fn is_sorted_after_test_1() {
        let mut rel = debug_create_relation(8);
        rel.print();
        rel.merge_sort(vec![
            (2, SortingOrder::Ascending),
            (1, SortingOrder::Descending),
        ]);
        assert!(rel.is_sorted_after(&vec![
            (2 as usize, SortingOrder::Ascending),
            (1 as usize, SortingOrder::Descending)
        ]));
        assert!(!rel.is_sorted_after(&vec![
            (0 as usize, SortingOrder::Constant),
            (2 as usize, SortingOrder::Ascending),
            (1 as usize, SortingOrder::Descending)
        ]));
        assert!(!rel.is_sorted_after(&vec![
            (2 as usize, SortingOrder::Ascending),
            (0 as usize, SortingOrder::Constant),
            (1 as usize, SortingOrder::Descending)
        ]));
        assert!(!rel.is_sorted_after(&vec![
            (2 as usize, SortingOrder::Ascending),
            (1 as usize, SortingOrder::Descending),
            (0 as usize, SortingOrder::Constant)
        ]));
    }

    #[test]
    #[should_panic(
        expected = "Trying to project the same column 'subject1' twice in a materialized_relation!"
    )]
    fn project_test_1() {
        let rel = debug_create_relation(1);
        let _result = rel.project(vec![&"subject1".to_owned(), &"subject1".to_owned()]);
    }

    #[test]
    fn project_test_2() {
        let rel = debug_create_relation(1);
        let result = rel.project(vec![]);
        assert!(result.get_attributes() == [] as [String; 0]);
        assert!(result.get_row_count() == 0);
    }

    #[test]
    fn remove_duplicate_indices_test_1() {
        let rel = debug_create_relation(1);
        let result = debug_create_relation(1).remove_duplicate_indices();
        for i in 0..rel.get_row_count() {
            assert!(result.get_row(i) == rel.get_row(i));
        }
    }

    #[test]
    fn remove_duplicate_indices_test_2() {
        let rel = debug_create_relation(6);
        let result = rel.remove_duplicate_indices();
        assert!(result.get_row(0) == [Node { id: 11 }, Node { id: 21 }, Node { id: 12 }]);
        assert!(result.get_row(1) == [Node { id: 13 }, Node { id: 21 }, Node { id: 14 }]);
        assert!(result.get_row(2) == [Node { id: 12 }, Node { id: 22 }, Node { id: 13 }]);
    }

    #[test]
    fn get_attributes_1() {
        let rel = debug_create_relation(1);
        assert!(rel.get_attributes() == ["subject1", "predicat1", "object1"])
    }

    #[test]
    fn get_attributes_empty_relation() {
        let rel = debug_create_relation(4);
        assert!(rel.get_attributes() == [] as [String; 0])
    }

    #[test]
    fn get_row_test_1() {
        let rel = debug_create_relation(1);
        assert!(rel.get_row(1) == [Node { id: 12 }, Node { id: 22 }, Node { id: 13 }]);
    }

    // Commented out since it probably costs performance to always check for out of bonds
    //#[test]
    #[allow(dead_code)]
    #[should_panic(expected = "out of bounds")]
    fn get_row_test_2() {
        let rel = debug_create_relation(1);
        let invalid_id = rel.get_row_count();
        rel.get_row(invalid_id);
    }

    // Commented out since it probably costs performance to always check for out of bonds
    //#[test]
    #[allow(dead_code)]
    #[should_panic(expected = "out of bounds")]
    fn get_row_test_empty_relation() {
        let rel = debug_create_relation(4);
        rel.get_row(0);
    }

    #[test]
    fn get_row_count_test_normal_relation() {
        let rel = debug_create_relation(1);
        assert!(rel.get_row_count() == 4);
    }

    #[test]
    fn get_row_count_test_empty_relation() {
        let rel = debug_create_relation(4);
        assert!(rel.get_row_count() == 0);
    }

    #[test]
    fn get_column_count_test_normal_relation() {
        let rel = debug_create_relation(1);
        assert!(rel.get_column_count() == 3);
    }

    #[test]
    fn get_column_count_test_empty_relation() {
        let rel = debug_create_relation(4);
        assert!(rel.get_column_count() == 0);
    }

    #[test]
    fn iterate_over_a_relation_by_row() {
        let relation = debug_create_relation(6);
        let mut rel_iter = relation.into_iter();
        assert!(rel_iter.next().unwrap() == [Node { id: 11 }, Node { id: 21 }, Node { id: 12 }]);
        assert!(rel_iter.next().unwrap() == [Node { id: 13 }, Node { id: 21 }, Node { id: 14 }]);
        assert!(rel_iter.next().unwrap() == [Node { id: 12 }, Node { id: 22 }, Node { id: 13 }]);
        assert!(rel_iter.next().unwrap() == [Node { id: 13 }, Node { id: 21 }, Node { id: 14 }]);
    }

    #[test]
    fn iterate_by_row_consuming_and_yielding_vecs() {
        let relation = debug_create_relation(6);
        let mut rel_iter = (*relation).into_iter();
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 11 }, Node { id: 21 }, Node { id: 12 }]
        );
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 13 }, Node { id: 21 }, Node { id: 14 }]
        );
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 12 }, Node { id: 22 }, Node { id: 13 }]
        );
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 13 }, Node { id: 21 }, Node { id: 14 }]
        );
    }

    #[test]
    fn use_iterator_map_and_collect_result() {
        let relation = debug_create_relation(6);
        fn change_13_to_15(row: Vec<Node>) -> Vec<Node> {
            row.into_iter()
                .map(|node| if node.id == 13 { Node { id: 15 } } else { node })
                .collect()
        }
        let relation: MaterializedRelation =
            relation.into_vec_iter().map(change_13_to_15).collect();
        let mut rel_iter = (&relation).into_iter();
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 11 }, Node { id: 21 }, Node { id: 12 }]
        );
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 15 }, Node { id: 21 }, Node { id: 14 }]
        );
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 12 }, Node { id: 22 }, Node { id: 15 }]
        );
        assert!(
            rel_iter.next().unwrap() == vec![Node { id: 15 }, Node { id: 21 }, Node { id: 14 }]
        );
    }

    #[test]
    fn use_extend(){
        let relation = debug_create_relation(10);
        let new_values: Vec<Node> = vec![Node { id: 33 }, Node { id: 34 }, Node { id: 35 }, Node { id: 36 }];
        let variable: Pattern = Pattern::Variable{name: String::from("new_column")};
        let result = relation.extend(variable, new_values);

        assert!(result.get_row(0) == [Node { id: 11 }, Node { id: 21 }, Node { id: 12 }, Node { id: 14 }, Node { id: 22 }, Node { id: 33 }]);
        assert!(result.get_row(1) == [Node { id: 12 }, Node { id: 22 }, Node { id: 13 }, Node { id: 11 }, Node { id: 21 }, Node { id: 34 }]);
        assert!(result.get_row(2) == [Node { id: 13 }, Node { id: 21 }, Node { id: 14 }, Node { id: 12 }, Node { id: 22 }, Node { id: 35 }]);
        assert!(result.get_row(3) == [Node { id: 14 }, Node { id: 22 }, Node { id: 11 }, Node { id: 13 }, Node { id: 21 }, Node { id: 36 }]);

        assert!(result.get_attributes() == vec!["subject1".to_string(), "predicat1".to_string(), "object1".to_string(), "subject2".to_string(), "predicat2".to_string(), String::from("new_column")]);
    }

    #[test]
    fn use_extend_with_missing_node(){
        let relation = debug_create_relation(10);
        let new_values: Vec<Node> = vec![Node { id: 33 }, Node { id: 34 }, Node { id: 35 }];
        let variable: Pattern = Pattern::Variable{name: String::from("new_column")};
        let result = relation.extend(variable, new_values);

        assert!(result.get_row(0) == [Node { id: 11 }, Node { id: 21 }, Node { id: 12 }, Node { id: 14 }, Node { id: 22 }, Node { id: 33 }]);
        assert!(result.get_row(1) == [Node { id: 12 }, Node { id: 22 }, Node { id: 13 }, Node { id: 11 }, Node { id: 21 }, Node { id: 34 }]);
        assert!(result.get_row(2) == [Node { id: 13 }, Node { id: 21 }, Node { id: 14 }, Node { id: 12 }, Node { id: 22 }, Node { id: 35 }]);
        assert!(result.get_row(3) == [Node { id: 14 }, Node { id: 22 }, Node { id: 11 }, Node { id: 13 }, Node { id: 21 }, Node { id: 0 }]);
    }

    #[test]
    fn group_by_one_column(){
        let relation = debug_create_relation(11);
        let mut proj_cols: Vec<&str> = Vec::new();
        let col = "subject1".to_string();
        proj_cols.push(col.as_str());
        let new_cols:Vec<Pattern> = Vec::new();
        let new_values: Vec<Vec<Node>> = Vec::new();

        let result = relation.group_by_and_aggregates(new_cols, proj_cols, new_values);

        assert!(result.get_row(0) == [Node { id: 11}]);
        assert!(result.get_row(1) == [Node { id: 14}]);
        assert!(result.get_row(2) == [Node { id: 0}]);
    }

    #[test]
    fn group_by_two_column(){
        let relation = debug_create_relation(11);
        let mut proj_cols: Vec<&str> = Vec::new();
        let col = "subject1".to_string();
        let col2 = "predicat1".to_string();
        proj_cols.push(col.as_str());
        proj_cols.push(col2.as_str());
        let new_cols:Vec<Pattern> = Vec::new();
        let new_values: Vec<Vec<Node>> = Vec::new();

        let result = relation.group_by_and_aggregates(new_cols, proj_cols, new_values);

        assert!(result.get_row(0) == [Node { id: 11}, Node { id: 12}]);
        assert!(result.get_row(2) == [Node { id: 11}, Node { id: 13}]);
        assert!(result.get_row(1) == [Node { id: 14}, Node { id: 22}]);
        assert!(result.get_row(4) == [Node { id: 14}, Node { id: 0}]);
        assert!(result.get_row(3) == [Node { id: 0}, Node { id: 14}]);
    }

    #[test]
    fn group_by_and_aggregate(){
        let relation = debug_create_relation(11);
        let mut proj_cols: Vec<&str> = Vec::new();
        let col = "subject1".to_string();
        let col2 = "predicat1".to_string();
        proj_cols.push(col.as_str());
        proj_cols.push(col2.as_str());
        let new_cols:Vec<Pattern> = vec![Pattern::Variable{name: String::from("new_column")}];
        let mut new_values: Vec<Vec<Node>> = Vec::new();
        let new_col = vec![Node { id: 11}, Node { id: 12},Node { id: 11}, Node { id: 12},Node { id: 11}];
        new_values.push(new_col);

        let result = relation.group_by_and_aggregates(new_cols, proj_cols, new_values);

        assert!(result.get_row(0) == [Node { id: 11}, Node { id: 12}, Node { id: 11}]);
        assert!(result.get_row(1) == [Node { id: 14}, Node { id: 22}, Node { id: 12}]);
        assert!(result.get_row(2) == [Node { id: 11}, Node { id: 13}, Node { id: 11}]);
        assert!(result.get_row(3) == [Node { id: 0}, Node { id: 14}, Node { id: 12}]);
        assert!(result.get_row(4) == [Node { id: 14}, Node { id: 0}, Node { id: 11}]);
    }

    #[test]
    fn group_by_and_two_aggregate(){
        let relation = debug_create_relation(11);
        let mut proj_cols: Vec<&str> = Vec::new();
        let col = "subject1".to_string();
        let col2 = "predicat1".to_string();
        proj_cols.push(col.as_str());
        proj_cols.push(col2.as_str());
        let new_cols:Vec<Pattern> = vec![Pattern::Variable{name: String::from("new_column")},Pattern::Variable{name: String::from("new_column2")}];
        let mut new_values: Vec<Vec<Node>> = Vec::new();
        let new_col = vec![Node { id: 11}, Node { id: 12},Node { id: 11}, Node { id: 12},Node { id: 11}];
        let new_col2 = vec![Node { id: 55}, Node { id: 55},Node { id: 55}, Node { id: 55},Node { id: 55}];
        new_values.push(new_col);
        new_values.push(new_col2);

        let result = relation.group_by_and_aggregates(new_cols, proj_cols, new_values);

        assert!(result.get_row(0) == [Node { id: 11}, Node { id: 12}, Node { id: 11}, Node { id: 55}]);
        assert!(result.get_row(1) == [Node { id: 14}, Node { id: 22}, Node { id: 12}, Node { id: 55}]);
        assert!(result.get_row(2) == [Node { id: 11}, Node { id: 13}, Node { id: 11}, Node { id: 55}]);
        assert!(result.get_row(3) == [Node { id: 0}, Node { id: 14}, Node { id: 12}, Node { id: 55}]);
        assert!(result.get_row(4) == [Node { id: 14}, Node { id: 0}, Node { id: 11}, Node { id: 55}]);
    }

    #[test]
    fn test_get_shared_attributes(){
        let names_a = vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()];
        let names_b = vec!["b".to_string(), "f".to_string(), "g".to_string(), "a".to_string()];
        let rel1 = MaterializedRelation::create_mat_relation(names_a, &[]);
        let rel2 = MaterializedRelation::create_mat_relation(names_b, &[]);
        let p_result1 = vec!["a".to_string(), "b".to_string()];
        let result = rel1.get_shared_attributes(&rel2);
        assert!((result.iter().eq(p_result1.iter())));
    }

    #[test]
    fn test_get_column_index_by_name(){
        let names_a = vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()];
        let rel = MaterializedRelation::create_mat_relation(names_a, &[]);
        assert!(rel.get_column_index_by_name("c").unwrap() == 2);
        assert!(rel.get_column_index_by_name("f").is_none());
    }

    #[test]
    fn test_new_with_data(){
        let relation = debug_create_relation(1);
        assert!(relation.get_row(0) == [Node { id: 11 }, Node { id: 21 }, Node { id: 12 }]);
        let new_values: Vec<Node> = vec![Node { id: 1 }, Node { id: 2 }, Node { id: 3 }];
        let result = relation.new_with_data(new_values);
        assert!(result.get_row(0) == [Node { id: 1 }, Node { id: 2 }, Node { id: 3 }]);
    }

    // exploratory test describing behaviour, not saying how it should be
    #[test]
    fn find_shared_columns_for_join_keeps_both_id_columns_from_self_and_other(){
        let names_self = vec!["a".to_string(), "ID".to_string()];
        let rel_self = MaterializedRelation::create_mat_relation(names_self,&[]);
        let names_other = vec!["a".to_string(), "ID".to_string()];
        let rel_other = Box::new(MaterializedRelation::create_mat_relation(names_other,&[]));
        let expected : (Vec<(usize, usize)>, Vec<String>, Vec<usize>) = 
            (vec![(0, 0)], vec!["a".to_string(), "ID".to_string(), "ID".to_string()], vec![1]);
        assert_eq!(rel_self.find_shared_columns_for_join(&(rel_other as Box<dyn Relation>)), expected);
    }

    // exploratory test describing behaviour, not saying how it should be
    #[test]
    fn what_happens_when_merging_two_relations_with_id_columns(){
        let names_self = vec!["a".to_string(), "ID".to_string()];
        let vals_self = vec![Node{ id: 1}, Node{ id: 2}, 
                             Node {id : 11}, Node {id : 12}];
        let rel_self = Box::new(MaterializedRelation::create_mat_relation(names_self, &vals_self));
        let names_other = vec!["a".to_string(), "ID".to_string()];
        let vals_other = vec![Node{ id: 1}, Node{ id: 9}, 
                             Node {id : 18}, Node {id : 19}];
        let rel_other = Box::new(MaterializedRelation::create_mat_relation(
            names_other,&vals_other));
        let joined = (rel_self as Box<dyn Relation>).join(rel_other as Box<dyn Relation>,true);
        let expected_names = vec!["a".to_string(), "ID".to_string(), "ID".to_string()];
        let expected_values = vec![Node { id: 1}, Node { id: 2}, Node { id : 9}];
        let expected = Box::new(MaterializedRelation::create_mat_relation(
            expected_names, &expected_values));
        assert_eq!(joined.get_row(0), expected.get_row(0));
        assert_eq!(joined.get_attributes(), expected.get_attributes());
    }

    #[test]
    fn to_materialized_relation_gets_right_type(){
        let names_self = vec!["a".to_string(), "ID".to_string()];
        let vals_self = vec![Node{ id: 1}, Node{ id: 2}, 
                             Node {id : 11}, Node {id : 12}];
        let box_self: Box<dyn Relation> = Box::new(
            MaterializedRelation::create_mat_relation(names_self, &vals_self));
        let mat_self : MaterializedRelation = box_self.to_materialized();
    }
    
}
