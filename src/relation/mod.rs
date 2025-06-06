//! Provides an relation interface for [storage_engine](crate::storage_engine), [calc_engine](crate::calc_engine) and [to_json_result](crate::to_json_result).
//! A implementation of the trait [Relation] called [materialized_relation] is also provided in that module.
pub mod materialized_relation;
use serde::{Deserialize, Serialize};
use crate::calc_data_types::Pattern;
use crate::data_types::DataValue;
use crate::storage_engine::Node;
use crate::relation::materialized_relation::MaterializedRelation;
use std::collections::btree_set::BTreeSet;
use std::collections::HashMap;
use std::ops::Index;
/// Stores nodes in a read-only mathematical Relation and implements some DB-Operations for Relations
///
/// Columns are indentified by their name alone, thus there can't be two identically named columns in a relation
/// Identical rows are possible.

pub trait Relation<'a> {
    /// Joins this relation with the other relation in an inner join.
    ///
    /// The returned relations contains each column name from self and each unmatched column name from other.
    /// If no columns match, an empty relation with the same column names as the self relation is returned
    fn join(self: Box<Self>, other: Box<dyn Relation<'a> + 'a>, do_left: bool) -> Box<dyn Relation<'a> + 'a>;

    /// Sorts this relation by the given asked_sort_order.
    ///
    /// The asked_sort_order contains the columns, after which the relation should be sorted.
    /// The first element of the vector is the primary sort criteria of the output, the second element the secondary sort criteria etc.
    ///
    /// # Panics
    /// Panics, when asked_sort_order contains a tuple with [SortingOrder::Constant]
    fn sort(
        self: Box<Self>,
        asked_sort_order: Vec<(&str, SortingOrder)>,
    ) -> Box<dyn Relation<'a> + 'a>;


    /// Projects specific columns of the relation to the output
    ///
    /// # Panics
    /// Panics, when columns contains duplicate string, to prevent relation with duplicate column names.
    /// Panics, when columns contains a name, for which a column in self doesn't exist.
    fn project(self: Box<Self>, columns: Vec<&str>) -> Box<dyn Relation<'a> + 'a>;

    /// Renames columns.
    ///
    /// # Panics
    /// Panics, when lenght of new_columns_names doesn't match up with number of own columns.
    fn rename_columns(self: Box<Self>, new_columns_names: Vec<&str>) -> Box<dyn Relation<'a> + 'a>;
    /// Removes identical rows.
    fn remove_duplicate_indices(self: Box<Self>) -> Box<dyn Relation<'a> + 'a>;

    /// Returns names of the column
    fn get_attributes(&self) -> &[String];
    /// Returns a specfic row of this relation
    fn get_row(&self, row_id: usize) -> &[Node];
    /// Returns number of rows of this relation
    fn get_row_count(&self) -> usize;
    /// Returns amount of columns of this relation
    fn get_column_count(&self) -> usize;
    /// Outputs the relation to console
    fn print(&self);
    ///returns only the proj_cols of the relation with addition of the new
    ///columns new_cols with the values new_values
    fn group_by_and_aggregates(self: Box<Self>, new_cols: Vec<Pattern>, proj_cols: Vec<&str>, new_values: Vec<Vec<Node>>) -> Box<dyn Relation<'a> + 'a>;
    ///creates new relation that consists of the old and a new column 
    ///with variabel as name and new_values as values
    fn extend(self: Box<Self>, variable: Pattern, new_values: Vec<Node>)-> Box<dyn Relation<'a> + 'a>;

    /// given another relation get the attribute names of both columns 
    /// returns a Vec<String>
    fn get_shared_attributes(&self, other: &dyn Relation<'a>) -> Vec<String>{
        let shared_names: BTreeSet<&String> = &BTreeSet::from_iter(
                                                self.get_attributes().iter())
            & //bitand gives an intersection
            &BTreeSet::from_iter(other.get_attributes().iter());
        shared_names.into_iter().map(String::from).collect()
    }
    // Creates a new relation with the same column as self, but with the supplied data
    fn new_with_data(&self, new_data: Vec<Node>) -> Box<dyn Relation<'a> + 'a>;
    
    /// given another relation find if there exist any common attribute names of both columns 
    /// returns a bool
    fn has_any_shared_attributes(&self, other: &Box<dyn Relation<'a> + 'a>) -> bool {
        let shared_names: BTreeSet<&String> = &BTreeSet::from_iter(self.get_attributes().iter())
            & //bitand gives an intersection
            &BTreeSet::from_iter(other.get_attributes().iter());
        !shared_names.is_empty()
    }
    /// Renames the attributes according to the map `rename`.
    fn rename_attributes(self: Box<Self>, rename: &HashMap<&str,&str>) -> Box<dyn Relation<'a> +'a>;
    /// convert unknown Relation to a MaterializedRelation
    fn to_materialized(self: Box<Self>) -> MaterializedRelation<'a>;

}

/// for sort function of trait [Relation]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SortingOrder {
    /// The first row of that column has the lowest value, the last row the highest value
    Ascending,
    /// The first row of that column has the highest value, the last row the lowest value
    Descending,
    /// All rows of that column contain an equal value
    Constant,
}

/// default iterator for &'a Relation
//#[derive(Debug)]  dunno how to derive Debug for a dyn
pub struct RelationIterator<'a> {
    relation: &'a dyn Relation<'a>,
    row_count: usize,
    current_row: usize,
}

impl<'a> RelationIterator<'a> {
    fn new(relation: &'a dyn Relation<'a>) -> RelationIterator<'a> {
        RelationIterator {
            relation,
            row_count: relation.get_row_count(),
            current_row: 0,
        }
    }
}

impl<'a> Iterator for RelationIterator<'a> {
    type Item = &'a [Node];

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row < self.row_count {
            self.current_row += 1;
            Some(self.relation.get_row(self.current_row - 1))
        } else {
            None
        }
    }
}

impl<'a> Index<(usize,usize)> for dyn Relation<'a> + 'a {
    type Output=Node;

    fn index(&self, index: (usize,usize)) -> &Self::Output {
        &self.get_row(index.0)[index.1]
    }
}

