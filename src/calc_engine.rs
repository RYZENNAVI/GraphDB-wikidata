use std::collections::HashMap;
use std::cmp::Ordering;
use crate::ext_string::StringStorage;

use crate::relation::materialized_relation::MaterializedRelation;
use crate::small_string::SmallString;
use crate::storage_engine::{Storage, StorageEngine, Node, prefix_str_to_datavalue_vec, 
                            Query, setup_test_storage, wikidataidp_to_wikidataidpstmt, 
                            NodeType};
use crate::data_types::{DataValue, MonolinText, RDFType, Quantity, Time};
use crate::parser::Data;
use crate::ext_string::RamStorage;

// for hash functions
use crypto::md5::Md5;
use crypto::digest::Digest;
use crypto::sha1::Sha1;
use crypto::sha2::Sha256;
use crypto::sha2::Sha384;
use crypto::sha2::Sha512;

// for RAND function
use rand::Rng;
use rand::prelude::ThreadRng;

use crate::relation::{Relation, SortingOrder};
use crate::to_json_result::relation_to_json;
// Added since to data move
use crate::calc_data_types::{Operator, Pattern, Expression, AE, Function};
use regex::Regex;

pub trait CalculationEngine<'a> 
{
    fn execute_query(&'a self,query: &mut Query <'a>);

    ///Executes qep and returns Relation
    fn recursive_qep_traversal(&'a self,rec_op:&Operator,query_id: u64)
                               ->Box<dyn Relation<'a> + 'a>;

    fn group_by(&'a self, inner: &Operator, variables: &[Pattern], aggregates: &[(Pattern, AE)], query_id: u64) 
                -> Box<dyn Relation<'a> + 'a>;

    fn calculate_aggregate_function(&'a self,rows: Vec<&[Node]>, aggregate: AE,
                                    attributes: &[String], query_id: u64) 
                                    -> DataValue <'a>;

    fn replace_variables(expression: Expression, cols: &[String]) 
                         -> Expression;

    fn equal(arg1: DataValue, arg2: DataValue) -> bool;

    fn calculate_expression(&'a self,expression: &Expression, row: &[Node], query_id: u64) 
                            -> DataValue<'a>;

    fn calculate_function_call(&'a self,func:Function, expressions: &[Expression], row: &[Node], query_id: u64) ->DataValue<'a>;

    fn calculation_on_numbers(arg1: DataValue, arg2: DataValue, kind: char) 
                              -> DataValue <'a>;

    fn type_of_calculation(arg1: f32, arg2: f32, kind: char) 
                           -> DataValue<'a>;

    fn pattern_to_str(pattern: &Pattern)->&str;

    fn pattern_to_string(pattern: &Pattern)->String;

    fn triplepattern_match(&'a self,s: &Pattern,p: &Pattern,o: &Pattern)
                           ->Box<dyn Relation<'a>+'a>;

    fn pattern_to_node(&self,pattern: &Pattern) -> Node;

    fn str_to_datavalue(input: SmallString) -> DataValue;

    fn literal_to_datavalue(input: SmallString) -> DataValue;

    fn to_json(&self, rel: Box<dyn Relation<'a> + 'a>) -> String;

    /// returns a vector of Datavalues from a Box<Relation>,
    /// with columns == None you get all Datavalues including alls the "ID" cols
    /// when specifying columns the relation will be projected accordingly
    fn relation_to_datavalue_vec(&'a self, 
                                 rel: Box<dyn Relation<'a> + 'a>,
                                 columns: Option<Vec<&str>>) -> Vec<DataValue>;

    /// given relation with columns of WikidataidP - Nodes and the names of those 
    /// columns return the same relation with the WikidataidP-Nodes replaced with
    /// equivalent WikidataidPstmt - Nodes
    fn direct_pred_to_stmt_pred(&self, 
                                rel: Box<dyn Relation<'a> + 'a>,
                                column_names: Vec<&str>) 
                                -> Box<dyn Relation<'a> + 'a>;


}

#[allow(unused_variables)]
#[allow(unused_mut)]
impl<'a,S> CalculationEngine<'a> for Calculation<'a,S>
where S:StringStorage
{
    fn execute_query(&'a self, query: & mut Query <'a>){
        query.relation = Some(Self::recursive_qep_traversal(self,&query.query_plan, query.id));
    }
    
    fn recursive_qep_traversal(&'a self, rec_op: &Operator, query_id: u64)
                               ->Box<dyn Relation<'a> + 'a>{
        match rec_op 
        {
            Operator::Bgp{inner}=> 
            { 
                let mut rel_vec: Vec<Box<dyn Relation>> = inner
                    .into_iter()
                    .map(|x|{self.recursive_qep_traversal(x, query_id)})
                    .collect();

                if rel_vec.len() > 2 {
                    // Sort by the size of the relations to start merging from the smallest first.
                    rel_vec.sort_by_key(|k| k.get_row_count());
                }
                println!("--DOING BGP--");
                
                if rel_vec.is_empty()
                {
                     todo!();
                }

                if rel_vec.len()==1
                {
                    rel_vec[0].print();
                    rel_vec.pop().unwrap()
                } else {
                    let mut merged_rel = rel_vec.remove(0);
                    println!("--FIRST BGP JOIN--");
                    merged_rel.print(); 
                    while !rel_vec.is_empty()
                    {
                        // Find the next smallest relation to merge with.
                        let next_mergeable_pos = rel_vec.iter()
                          .position(|r| merged_rel.has_any_shared_attributes(r))
                          .expect("Did not find any mergeable relation in a not-yet-empty vector");
                        let merge_with_rel = rel_vec.remove(next_mergeable_pos);

                        merged_rel = merged_rel.join(merge_with_rel, false);
                        println!("--FOLLOWING BGP JOIN--");
                        merged_rel.print(); 
                    }
                    println!("--FINISHED BGP JOIN--");
                    merged_rel
                }
                
            }

            Operator::Path{subject,path,object}=>
            {
                todo!();
            }
            
            Operator::Distinct{inner} =>{
                let mut rel = Self::recursive_qep_traversal(self,inner, query_id);
                return rel.remove_duplicate_indices();
            }

            Operator::MergeJoin{left_child,right_child}=> 
            { 
                let mut left_rel = Self::recursive_qep_traversal(self, left_child,query_id);
                let mut right_rel = Self::recursive_qep_traversal(self, right_child,query_id);
                return left_rel.join(right_rel, false);
            }

            Operator::LeftJoin{left_child,right_child,expression} =>
            {
                let mut left_rel: Box <dyn Relation> = Self::recursive_qep_traversal(self,left_child,query_id);
                let mut right_rel: Box <dyn Relation> = Self::recursive_qep_traversal(self,right_child,query_id);

                // First use the expression to filter the left relation
                if let Some(expression) = expression {
                    let expr = Self::replace_variables(expression.clone(), left_rel.get_attributes());
                    let mut new_values: Vec<Node> = Vec::new();
                    let row_count = left_rel.get_row_count();
                    // evaluates the expression for each row
                    for row_index in 0..row_count{
                        let row = left_rel.get_row(row_index);
                        let eval_result = Self::calculate_expression(self, &expr, row,query_id);
                        if eval_result.into_effective_boolean().unwrap_or(false) {
                            new_values.extend_from_slice(row);
                        }
                    }

                    left_rel = left_rel.new_with_data(new_values);
                }
                return left_rel.join(right_rel, true);
            }

            Operator::LateralJoin{left_child,right_child}=>
            {
                let mut left_rel: Box <dyn Relation> = Self::recursive_qep_traversal(self, 
                                                                                     left_child, query_id);
                let mut right_rel: Box <dyn Relation> = Self::recursive_qep_traversal(self,
                                                                                      right_child,query_id);
                return left_rel.join(right_rel, false);
            }

            Operator::Filter{expression,inner}=>
            {
                let mut rel = Self::recursive_qep_traversal(self,inner, query_id);
                //replace column names with numbers
                let expr = Self::replace_variables(expression.clone(), rel.get_attributes());
                let mut new_values: Vec<Node> = Vec::new();

                let row_count = rel.get_row_count();
                // evaluates the expression for each row
                for row_index in 0..row_count{
                    let row = rel.get_row(row_index);
                    let eval_result = Self::calculate_expression(self,&expr, row, query_id);
                    if eval_result.into_effective_boolean().unwrap_or(false) {
                        new_values.extend_from_slice(row);
                    }
                }

                rel.new_with_data(new_values)

            }

            Operator::Order{inner, order_attribute}=> 
            { 
                let mut rel: Box <dyn Relation> = Self::recursive_qep_traversal(self,inner, query_id);
                //get column: &str, sorting_order from order_attribute
                //rel.sort(column,sorting_order);
                rel
            }

            Operator::Projection{inner,attributes}=> 
            {
                let mut rel: Box <dyn Relation> = Self::recursive_qep_traversal(self, inner, query_id);
                let str_attributes = attributes.iter()
                    .map(Self::pattern_to_str)
                    .collect();

                rel = rel.project(str_attributes);
                rel
            }

            Operator::TriplePattern{s,p,o}=>
            {
                Self::triplepattern_match(self, &s, &p, &o)
            }

            Operator::Union{left,right}=>
            {
                let mut left_rel: Box<dyn Relation> = Self::recursive_qep_traversal(self,left, query_id);
                let mut right_rel: Box<dyn Relation> = Self::recursive_qep_traversal(self,right, query_id);
                todo!();
            }

            Operator::Graph{name,inner}=>
            {
                let mut rel = Self::recursive_qep_traversal(self,inner, query_id);
                todo!();
            }

            Operator::Extend{inner,variable,expression}=>
            {
                let mut rel = Self::recursive_qep_traversal(self, inner, query_id);
                //replace column names with numbers

                let expr = Self::replace_variables(expression.clone(), rel.get_attributes());
                let mut new_values: Vec<Node> = Vec::new();                
                let row_count = rel.get_row_count();

                //calculates new value for each row
                for row_index in 0..row_count{
                    let row = rel.get_row(row_index);
                    new_values.push(self.storage
                                    .get_node_from_datavalue(
                                        Self::calculate_expression(self,
                                                                   &expr,
                                                                   row, query_id), Some(query_id)));
                } 

                rel.extend(variable.clone(), new_values)
            }

            Operator::ProjectExtend { inner, expressions } =>
            {
                let mut rel = Self::recursive_qep_traversal(self, inner,query_id);
                let (vars, expressions): (Vec<_>, Vec<_>) = expressions.iter().map(|(variable,expr)| {
                    (Self::pattern_to_str(&variable).to_owned(), Self::replace_variables(expr.clone(), rel.get_attributes()))
                }).unzip();

                let nodes: Vec::<_> = rel.to_materialized().into_iter().flat_map(|tuple| {
                    expressions.iter().map(|expr| {
                        self.storage.get_node_from_datavalue(
                            Self::calculate_expression(self, &expr, tuple,query_id), Some(query_id)
                        )
                    })
                }).collect();

                Box::new(
                    MaterializedRelation::create_mat_relation(vars, &nodes)
                )
            }

            Operator::Minus{left,right}=>
            {
                let mut left_rel= Self::recursive_qep_traversal(self, left, query_id);
                let mut right_rel=Self::recursive_qep_traversal(self, right, query_id);
                todo!();
            }

            Operator::Values{variables,bindings}=>
            {
                todo!();
            }

            Operator::Reduced{inner}=>
            {
                let mut rel= Self::recursive_qep_traversal(self, inner, query_id);
                todo!();
            }

            Operator::Slice{inner,start,length}=>
            {
                // M := Slice(M, start, length)
                // start defaults to 0
                // length defaults to (size(M)-start).
                let mut rel= Self::recursive_qep_traversal(self,inner,query_id);
                let start = (*start).min(rel.get_row_count());
                let length = length.unwrap_or_else(|| rel.get_row_count() - start).min(rel.get_row_count() - start);
                if start == 0 && length >= rel.get_row_count() {
                    return rel;
                }
                let mut new_values: Vec<Node> = Vec::new();
                for row_index in start..start + length {
                    let row = rel.get_row(row_index);
                    new_values.extend_from_slice(row);
                }
                rel.new_with_data(new_values)
            }

            Operator::Service{name,inner,silent}=>
            {
                let mut rel = Self::recursive_qep_traversal(self,inner, query_id);
                todo!();
            }

            Operator::Group { inner, variables, aggregates } =>
            {
                return Self::group_by(self, inner, variables, aggregates, query_id)
            }
        }
    }

    fn group_by(&'a self, inner: &Operator, variables: &[Pattern], aggregates: &[(Pattern, AE)], query_id: u64) -> Box<dyn Relation<'a> + 'a>{
        let mut rel = Self::recursive_qep_traversal(self,inner, query_id);

                //only aggregates, no group by
                if variables.is_empty(){
                    //new values
                    let mut new_values_all: Vec<Vec<Node>> = Vec::new();
                    //new column names
                    let mut new_columns: Vec<Pattern> = Vec::new();

                    for aggregate in aggregates{
                        let mut new_vals: Vec<Node> = Vec::new(); 
                        //new column
                        new_columns.push(aggregate.0.clone());

                        let mut rows: Vec<&[Node]> = Vec::new();

                        for i in 0..rel.get_row_count(){
                            rows.push(rel.get_row(i));
                        }

                        new_vals.push(self.storage.get_node_from_datavalue(Self::calculate_aggregate_function(self, rows, aggregate.1.clone(),rel.get_attributes(), query_id),Some(query_id)));
                        new_values_all.push(new_vals);
                    }

                        //nur einen Wert -> nur eine Zeile

                    let proj_cols: Vec<&str> = Vec::new();

                    return rel.group_by_and_aggregates(new_columns, proj_cols, new_values_all);
                }
                //aggregates and group by
                else{

                //sorts relation
                let asked_sort_order = variables.iter().map(|pattern|(Self::pattern_to_str(pattern), SortingOrder::Ascending)).collect();

                rel = rel.sort(asked_sort_order);

               
                let str_attributes: Vec<&str> = variables.iter().map(Self::pattern_to_str).collect();

                //Vec of variables as numbers
                let mut cols: Vec<usize> = Vec::new();

                for col in &str_attributes{
                    let column = rel.get_attributes().iter().position(|s| s == col);

                    match column{
                        Some(value) => cols.push(value),
                        _ => panic!("Not a column"),
                    }
                }

                //new values
                let mut new_values_all: Vec<Vec<Node>> = Vec::new();
                //new column names
                let mut new_columns: Vec<Pattern> = Vec::new();

                let mut rows: Vec<&[Node]> = Vec::new();
                
                let mut  is_first_unequal:bool;

                for aggregate in aggregates{
                    //new column
                    new_columns.push(aggregate.0.clone());
                    //contains new values for aggregate
                    let mut new_values: Vec<Node> = Vec::new();

                    is_first_unequal = true;

                    //get all rows that have the same values for all variables
                    //and calculate for each group one DataValue
                    for i in 0..rel.get_row_count(){

                        if  is_first_unequal {
                            //push first new row
                            rows.push(rel.get_row(i));
                        }
                        
                        if i != rel.get_row_count()-1 {
                            for column in &cols{
                                if rel.get_row(i)[*column] == rel.get_row(i+1)[*column]{
                                    if column == &cols[cols.len()-1]{
                                        rows.push(rel.get_row(i+1));
                                        is_first_unequal = false;
                                    }
                                    else{
                                        continue;
                                    }
                                }
                                else{
                                    is_first_unequal = true;
                                    break;
                                }
                            }
                        }

                        //found a group
                        if  is_first_unequal {
                            new_values.push(self.storage.get_node_from_datavalue(self.calculate_aggregate_function(rows.clone(), aggregate.1.clone(),rel.get_attributes(), query_id), Some(query_id)));
                            rows.clear();
                        }

                    }
                    new_values_all.push(new_values);
 
                }
                return rel.group_by_and_aggregates(new_columns, str_attributes, new_values_all);
            }   
    }

    fn calculate_aggregate_function(&'a self, rows: Vec<&[Node]>, aggregate: AE, attributes: &[String], query_id: u64) -> DataValue<'a>{
            match aggregate{
                AE::Count {expr, distinct} => {
                    if ! distinct {
                        return DataValue::NumberInt(rows.len().try_into().unwrap())
                    }
                    else{
                        match expr{
                            //counts not Null values
                            Some(expr) => {
                                let expression = Self::replace_variables(*expr, attributes);
                                let mut added_values: Vec<DataValue> = Vec::new();
                                let mut sum = 0;

                                for row in rows{
                                    let value = Self::calculate_expression(&self, &expression, row, query_id);

                                    if added_values.contains(&value) == false && value != DataValue::Null(){
                                        sum = sum + 1;
                                        added_values.push(value);
                                    }
                                }

                                return DataValue::NumberInt(sum);
                            },
                            //counts distinct rows
                            None => {
                                let mut added_values: Vec<&[Node]> = Vec::new();
                                let mut sum = 0;

                                for row in rows{
                                    if ! added_values.contains(&row) {
                                        added_values.push(row);
                                        sum = sum + 1;
                                    }
                                }

                                return DataValue::NumberInt(sum);
                            }, 
                        }
                    }
                },
                AE::Sum {expr, distinct} => {
                    let expression = Self::replace_variables(*expr, attributes);

                    if ! distinct {
                        
                        let mut sum: f32 = 0.0;
                        for row in rows{
                            let value = Self::calculate_expression(self, &expression, row, query_id);
                            match value{
                                DataValue::NumberFloat(number) => sum = sum + number,
                                DataValue::NumberInt(number) => sum = sum + number as f32,
                                _ => (),
                            }
                        }
                        return DataValue::NumberFloat(sum)
                    }
                    else{
                        let mut added_values: Vec<DataValue> = Vec::new();

                        let mut sum: f32 = 0.0;
                        for row in rows{
                            let value = Self::calculate_expression(&self, &expression, row, query_id);

                            if ! added_values.contains(&value) {

                                added_values.push(value.clone());

                                match value{
                                    DataValue::NumberFloat(number) => sum = sum + number,
                                    DataValue::NumberInt(number) => sum = sum + number as f32,
                                    _ => (),
                                }
                            }
                        }
                        return DataValue::NumberFloat(sum)
                    }
                },
                AE::Avg {expr, distinct} => {
                    let mut values: Vec<f32> = Vec::new();
                        let expression = Self::replace_variables(*expr, attributes);


                        for row in rows{
                            let value = Self::calculate_expression(self, &expression, row, query_id);
                            match value{
                                DataValue::NumberFloat(number) => {
                                    if distinct == false{
                                        values.push(number)
                                    }
                                    else{
                                        if ! values.contains(&number) {
                                            values.push(number)
                                        }
                                    }
                                },
                                DataValue::NumberInt(number) => {
                                    if distinct == false{
                                        values.push(number as f32)
                                    }
                                    else{
                                        if ! values.contains(&(number as f32)) {
                                            values.push(number as f32)
                                        }
                                    }
                                },
                                _ => (),
                            }
                        }
                                
                        if values.is_empty() {
                            return DataValue::Null()
                        }
                        else{
                            let mut sum = 0.0;

                            for value in &values{
                                sum = sum + value;
                            }

                            return DataValue::NumberFloat(sum / (values.len() as f32))
                        }
                },
                AE::Min {expr, distinct} => {
                    let expression = Self::replace_variables(*expr, attributes);
                    let mut new_value = Self::calculate_expression(self, &expression, rows[0], query_id);

                    for row in &rows{
                        let calc_value = Self::calculate_expression(self, &expression, row, query_id);

                        if *row == rows[0]{
                            ()
                        }
                        else{
                            let comparison = new_value.partial_cmp(&calc_value);
                            match comparison{
                                Some(value) => {
                                    match value{
                                        Ordering::Greater => new_value = calc_value,
                                        _ => (),
                                    }
                                },
                                None => (),
                            }
                        }
                    }
                    
                    new_value
                },
                AE::Max {expr, distinct} => {
                    let expression = Self::replace_variables(*expr, attributes);
                    let mut new_value = Self::calculate_expression(&self, &expression, rows[0], query_id);

                    for row in &rows{
                        let calc_value = Self::calculate_expression(&self, &expression, row, query_id);

                        if *row == rows[0]{
                            ()
                        }
                        else{
                            let comparison = new_value.partial_cmp(&calc_value);
                            match comparison{
                                Some(value) => {
                                    match value{
                                        Ordering::Less => new_value = calc_value,
                                        _ => (),
                                    }
                                },
                                None => (),
                            }
                        }
                    }
                    
                    new_value
                },
                AE::GroupConcat {expr, distinct, separator} => {
                    let mut new_value: String = String::from("");
                    let mut sep: String = String::from("");
                    let mut is_last: bool = false;

                    match separator{
                        Some(value) => sep = value,
                        None => (),
                    }

                    let expression = Self::replace_variables(*expr, attributes);

                    if distinct == false {

                        for row in &rows{
                            let calc_value = Self::calculate_expression(&self, &expression, row, query_id);
                            
                            if *row == rows[rows.len()-1]{
                                //last row -> no Separator
                                is_last = true;
                            }

                            match calc_value {
                                DataValue::WikidataidQ(value) |
                                DataValue::WikidataidP(value) |
                                DataValue::WikidataidL(value) |
                                DataValue::WikidataidPstmt(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.to_string().as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::String(value) | DataValue::Identifier(value) | 
                                DataValue::Url(value) | DataValue::Media(value) | DataValue::GeoShape(value) | 
                                DataValue::Label(value) | DataValue::Description(value) | DataValue::Alias(value) |
                                DataValue::Tabular(value) | DataValue::MathExp(value) | DataValue::MusicNotation(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.as_str();
                                    }
                                },
                                DataValue::Quantity(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.amount.as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.amount.as_str();
                                    }
                                },
                                DataValue::Time(value) => {
                                    if is_last == false{
                                        new_value = new_value + format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", value.year, value.month, value.day, value.hour, value.minute, value.second).as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", value.year, value.month, value.day, value.hour, value.minute, value.second).as_str();
                                    }
                                },
                                DataValue::Coord(value) => {
                                    
                                    if is_last == false{
                                        new_value = new_value + format!("Point({} {})", value.longitude, value.latitude).as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + format!("Point({} {})", value.longitude, value.latitude).as_str();
                                    }
                                },
                                DataValue::Null() => (),
                                DataValue::NumberInt(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.to_string().as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::NumberFloat(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.to_string().as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::Boolean(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.to_string().as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::MonolinText(value) => {
                                    if is_last == false{
                                        new_value = new_value + value.text.as_str() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.text.to_string().as_str();
                                    }
                                },
                                DataValue::Edge(value) => {
                                    if is_last == false{
                                        new_value = new_value + &value.to_string() + sep.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + &value.to_string();
                                    }
                                },
                                DataValue::NamedEdge(_) => {
                                    todo!()
                                }
                            }

                        }

                    }
                    else{
                        let mut added_values:Vec<DataValue> = Vec::new();
                        let mut is_first:bool;

                        for row in &rows{
                            let calc_value = Self::calculate_expression(self, &expression, row, query_id);
                            is_first = false;

                            if *row == rows[0]{
                                //last row -> no Separator
                                is_first = true;
                            }

                            if ! added_values.contains(&calc_value) {

                                added_values.push(calc_value.clone());

                            match calc_value{
                                DataValue::WikidataidQ(value) |
                                DataValue::WikidataidP(value) |
                                DataValue::WikidataidL(value) |
                                DataValue::WikidataidPstmt(value) |
                                DataValue::Edge(value)=> {
                                    if ! is_first {
                                        new_value = new_value  + sep.as_str() + value.to_string().as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::String(value) | DataValue::Identifier(value) | 
                                DataValue::Url(value) | DataValue::Media(value) | DataValue::GeoShape(value) | DataValue::NamedEdge(value) |
                                DataValue::Label(value) | DataValue::Description(value) | DataValue::Alias(value) |
                                DataValue::Tabular(value) | DataValue::MathExp(value) | DataValue::MusicNotation(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + value.as_str();
                                    }
                                    else{
                                        new_value = new_value + value.as_str();
                                    }
                                },
                                DataValue::Quantity(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + value.amount.as_str();
                                    }
                                    else{
                                        new_value = new_value + value.amount.as_str();
                                    }
                                },
                                DataValue::Time(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", value.year, value.month, value.day, value.hour, value.minute, value.second).as_str() ;
                                    }
                                    else{
                                        new_value = new_value + format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", value.year, value.month, value.day, value.hour, value.minute, value.second).as_str();
                                    }
                                },
                                DataValue::Coord(value) => {
                                    
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + format!("Point({} {})", value.longitude, value.latitude).as_str();
                                    }
                                    else{
                                        new_value = new_value + format!("Point({} {})", value.longitude, value.latitude).as_str();
                                    }
                                },
                                DataValue::Null() => (),
                                DataValue::NumberInt(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + value.to_string().as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::NumberFloat(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + value.to_string().as_str();
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::Boolean(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + value.to_string().as_str();
                                    }
                                    else{
                                        new_value = new_value + value.to_string().as_str();
                                    }
                                },
                                DataValue::MonolinText(value) => {
                                    if ! is_first {
                                        new_value = new_value + sep.as_str() + value.text.as_str() ;
                                    }
                                    else{
                                        new_value = new_value + value.text.to_string().as_str();
                                    }
                                },
                            }
                            }
                        }
                    }

                    return DataValue::String(new_value.into())
                },
                AE::Sample {expr, distinct} => {
                    let expression = Self::replace_variables(*expr, attributes);
                    match expression{
                        Expression::ColumnNumber(col) => {
                            return self.storage.retrieve_data(rows[0][col])
                        },
                        _ => {
                            return Self::calculate_expression(self, &expression, rows[0], query_id);
                        },
                    }
                },
                AE::Custom {name, expr, distinct} => {
                    todo!()
                },
            }
    }


    fn replace_variables(expression: Expression, cols: &[String]) -> Expression {
        match expression{
            Expression::NamedNode(pattern) => Expression::NamedNode(pattern),
            Expression::Literal(pattern) => Expression::Literal(pattern),
            Expression::Variable(pattern) => {
                match pattern{
                    Pattern::Variable { name } => {

                        let column = cols.iter().position(|s| s == &name);

                        match column{
                            Some(value) => Expression::ColumnNumber(value),
                            _ => Expression::ColumnNumber(usize::MAX),
                        }
                    }
                    _ => panic!("No column"),
                }
            },
            Expression::ColumnNumber(number) => Expression::ColumnNumber(number),
            Expression::Or(arg1, arg2) => Expression::Or(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2,cols))),
            Expression::And(arg1, arg2) => Expression::And(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::Equal(arg1, arg2) => Expression::Equal(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::SameTerm(arg1, arg2) => Expression::SameTerm(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::Greater(arg1, arg2) => Expression::Greater(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::GreaterOrEqual(arg1, arg2) => Expression::GreaterOrEqual(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::Less(arg1, arg2) => Expression::Less(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::LessOrEqual(arg1, arg2) => Expression::LessOrEqual(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::In(arg, vec) => {
                let mut vector: Vec<Expression> = Vec::new();

                for expr in vec{
                    vector.push(Self::replace_variables(expr.clone(), cols));
                }

                Expression::In(Box::new(Self::replace_variables(*arg, cols)), vector)
            },
            Expression::Add(arg1, arg2) => Expression::Add(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::Subtract(arg1, arg2) => Expression::Subtract(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::Multiply(arg1, arg2) => Expression::Multiply(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::Divide(arg1, arg2) => Expression::Divide(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols))),
            Expression::UnaryPlus(arg) => Expression::UnaryPlus(Box::new(Self::replace_variables(*arg, cols))),
            Expression::UnaryMinus(arg) => Expression::UnaryMinus(Box::new(Self::replace_variables(*arg, cols))),
            Expression::Not(arg) => Expression::Not(Box::new(Self::replace_variables(*arg, cols))),
            Expression::Exists(operator) => {
                match *operator {
                    Operator::Filter { expression, inner } => {
                        Expression::Exists(Box::new(Operator::Filter {
                            expression: Self::replace_variables(expression, cols),
                            inner,
                        }))
                    },
                    Operator::Extend { inner, variable, expression } => {
                        Expression::Exists(Box::new(Operator::Extend {
                            inner,
                            variable,
                            expression: Self::replace_variables(expression, cols)
                        }))
                    },
                    Operator::LeftJoin { left_child, right_child, expression: Some(expression) } => {
                        Expression::Exists(Box::new(Operator::LeftJoin {
                            left_child,
                            right_child,
                            expression: Some(Self::replace_variables(expression, cols))
                        }))
                    },
                    _ => Expression::Exists(operator)
                }
            },
            Expression::Bound(pattern) => {
                match pattern{
                    Pattern::BlankNode { name } => Expression::Bound(Pattern::BlankNode { name }),
                    Pattern::Literal { name } => Expression::Bound(Pattern::Literal { name }),
                    Pattern::NamedNode { name } => Expression::Bound(Pattern::NamedNode { name }),
                    Pattern::Variable { name } => {
                        let column = cols.iter().position(|s| s == &name);

                        match column{
                            Some(value) => Expression::BoundVariable(value),
                            _ => Expression::BoundVariable(usize::MAX),
                        }
                    },
                }
            },
            Expression::BoundVariable(number) => Expression::BoundVariable(number),
            Expression::If(arg1, arg2, arg3) =>  Expression::If(Box::new(Self::replace_variables(*arg1, cols)),Box::new(Self::replace_variables(*arg2, cols)),Box::new(Self::replace_variables(*arg3, cols))),
            //returns the first non-null value
            Expression::Coalesce(vec) => {
                let mut vector: Vec<Expression> = Vec::new();

                for expr in vec{
                    vector.push(Self::replace_variables(expr.clone(), cols));
                }

                Expression::Coalesce(vector)
            },
            Expression::FunctionCall(string, vec) => {
                let mut vector: Vec<Expression> = Vec::new();

                for expr in vec{
                    vector.push(Self::replace_variables(expr.clone(), cols));
                }

                Expression::FunctionCall(string,vector)
            },
        }
    }
    


    fn calculate_expression(&'a self,expression: &Expression, row: &[Node], query_id: u64) -> DataValue<'a>{
        match expression{
            Expression::NamedNode(pattern)| Expression::Literal(pattern) | Expression::Variable(pattern) => {
                match pattern{
                    Pattern::Literal{name} => Self::literal_to_datavalue(SmallString::new(name)),
                    Pattern::NamedNode {name} | Pattern::BlankNode {name} => Self::str_to_datavalue(SmallString::new(name)),
                    _ => panic!(),
                }
            },
            Expression::ColumnNumber(number) => {
                if *number < row.len(){
                    let cell = row[*number];
                    self.storage.retrieve_data(cell)
                }
                else{
                    DataValue::Null()
                }
            },
            Expression::Or(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                match (arg1,arg2){
                    (DataValue::Boolean(value1), DataValue::Boolean(value2)) => {
                                DataValue::Boolean(value1 | value2)
                    },
                    _ => DataValue::Null(),
                }
            },
            Expression::And(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                match (arg1,arg2){
                    (DataValue::Boolean(value1), DataValue::Boolean(value2)) => {
                                DataValue::Boolean(value1 & value2)
                    },
                    _ => DataValue::Null(),
                }
            },
            Expression::Equal(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                DataValue::Boolean(Self::equal(arg1, arg2))
            },
            Expression::SameTerm(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self, arg1, row, query_id);
                let arg2 = Self::calculate_expression(self, arg2, row, query_id);

                DataValue::Boolean(arg1 == arg2)
            },
            Expression::Greater(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                arg1.gt(&arg2).into()
            },
            Expression::GreaterOrEqual(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                arg1.ge(&arg2).into()
                
            },
            Expression::Less(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                arg1.lt(&arg2).into()
            },
            Expression::LessOrEqual(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);

                arg1.le(&arg2).into()
            },
            Expression::In(arg, vec) => {
                let arg = Self::calculate_expression(self,arg, row, query_id);
                let mut all: Vec<DataValue> = Vec::new();
                // create a new mutable array "all", it is a vec<DataValue> array
                for expr in vec{
                    all.push(Self::calculate_expression(self,&expr, row, query_id));
                }
                // push every element from "vec" to the new array "all"
                // can be seen as a transformation from vec<Expression> to vec<DataValue>
                DataValue::Boolean(all.contains(&arg))
                // test if arg in this new array, if true, means arg is in vec
                // more details from In is here: https://www.w3.org/TR/sparql11-query/#func-in
            },
            Expression::Add(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);
                Self::calculation_on_numbers(arg1, arg2,'A')                
            },
            Expression::Subtract(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);
                Self::calculation_on_numbers(arg1, arg2,'S')                
            },
            Expression::Multiply(arg1, arg2) =>{
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);
                Self::calculation_on_numbers(arg1, arg2,'M')                
            },
            Expression::Divide(arg1, arg2) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);
                let arg2 = Self::calculate_expression(self,arg2, row, query_id);
                Self::calculation_on_numbers(arg1, arg2,'D')                
            },
            Expression::UnaryPlus(arg) => {
                let arg = Self::calculate_expression(self,arg, row, query_id);
                Self::calculation_on_numbers(arg, DataValue::NumberInt(0), 'P')
            },
            Expression::UnaryMinus(arg) => {
                let arg = Self::calculate_expression(self,arg, row, query_id);
                Self::calculation_on_numbers(arg, DataValue::NumberInt(0), 'I')
            },
            Expression::Not(arg) => {
                let arg = Self::calculate_expression(self,arg, row, query_id);
                match arg{
                    DataValue::Boolean(value) => DataValue::Boolean(!value),
                    _ => DataValue::Null()
                }
            },
            Expression::Exists(operator) => {
                DataValue::Boolean(self.recursive_qep_traversal(operator, query_id).get_row_count() > 0)
                // Should be optimized during the query optimization phase
            },
            //returns true if variable is bound to a value
            Expression::Bound(pattern) => {
                match pattern{
                    Pattern::BlankNode { name } => DataValue::Boolean(false),
                    Pattern::Literal { name } => {
                        match Self::literal_to_datavalue(name.into()){
                            DataValue::Null() => DataValue::Boolean(false),
                            _ => DataValue::Boolean(true),
                        }
                    },
                    Pattern::NamedNode { name } => todo!(),
                    Pattern::Variable { name } => DataValue::Boolean(false),
                }
            },
            Expression::BoundVariable(number) => {
                if *number < row.len(){
                    let cell = row[*number];
                    let value = self.storage.retrieve_data(cell);
                    match value{
                        DataValue::Null() => DataValue::Boolean(false),
                        _ => DataValue::Boolean(true),
                    }
                }
                else{
                    DataValue::Boolean(false)
                }
            }
            Expression::If(arg1, arg2, arg3) => {
                let arg1 = Self::calculate_expression(self,arg1, row, query_id);

                match arg1{
                    DataValue::Boolean(value) => {
                        if value{
                            Self::calculate_expression(self,arg2, row, query_id)
                        }
                        else{
                            Self::calculate_expression(self,arg3, row, query_id)
                        }
                    },
                    _ => DataValue::Null(),
                }
                
            },
            //returns the first non-null value
            Expression::Coalesce(vec) => {

                for expr in vec{
                    let value = Self::calculate_expression(self,&expr, row, query_id);
                    match value{
                        DataValue::Null() => continue,
                        _ => return value
                    }
                }

                DataValue::Null()
            },
            Expression::FunctionCall(func, vec) => {
                Self::calculate_function_call(self,func.clone(),&vec,row, query_id)

            },
        }
    }
    
    // This function is the explizit implement of case FunctionCall in calculate_expression
    fn calculate_function_call(&'a self,func:Function, expressions: &[Expression], row: &[Node], query_id: u64) ->DataValue<'a>{
        let mut all: Vec<DataValue> = Vec::new();
        // create a new mutable array "all", it is a vec<DataValue> array, which has all parameters from current function
        for expr in expressions {
            all.push(Self::calculate_expression(self,expr, row, query_id));
        }
        // push every element from "vec" to the new array "all"
        // can be seen as a transformation from vec<Expression> to vec<DataValue>
        // Although almost all functions here have only one parameter, 
        // the purpose of this is to deal with functions with multiple parameters that may be encountered or added later
        match func
        {
            // RDF Terms functions
            Function::IsIri =>
            {
                // Details: https://www.w3.org/TR/sparql11-query/#func-isIRI
                (all[0].get_rdf_type() == RDFType::IRI).into()
            }
            Function::IsBlank =>
            {
                // Details: https://www.w3.org/TR/sparql11-query/#func-isBlank
                (all[0].get_rdf_type() == RDFType::BNode).into()
            }
            Function::IsLiteral =>
            {
                (all[0].get_rdf_type() == RDFType::Literal).into()
            }
            Function::IsNumeric =>
            {
                // match the only parameter DataValue::NumberInt/NumberFloat in function IsNumeric
                // Details: https://www.w3.org/TR/sparql11-query/#func-isLiteral
                match &all[0]
                {
                    DataValue::NumberInt(number) =>
                    {
                        return DataValue::Boolean(true);
                    }
                    DataValue::NumberFloat(number) =>
                    {
                        return DataValue::Boolean(true);
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }
            }
            Function::Str =>
            {
                // convert the datavalue to string
                // Details: https://www.w3.org/TR/sparql11-query/#func-str

                // convert datatype to Option<SmallString>
                let type_of_data = all[0].get_datatype();
                match type_of_data
                {
                    Some(type_string) =>
                    {
                        let type_string_local = type_string.to_string();
                        return DataValue::String(type_string_local.into());
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }
                }                    
            }
            Function::Lang =>
            {
                // match the only parameter MonolinText and return its language tag
                // Details: https://www.w3.org/TR/sparql11-query/#func-lang
                match &all[0]
                {
                    DataValue::MonolinText(MonoText) =>
                    {
                        let language_tag = MonoText.language.as_str();
                        return DataValue::String(language_tag.to_string().into());
                    }
                    _=> // has no language tag, return empty string
                    {
                        let string_empty = String::new();
                        return DataValue::String(string_empty.into());       
                    }
                }
            }
            Function::Datatype =>
            {
                // we have no Datavalue::IRI as return value, so todo here
                // Details: https://www.w3.org/TR/sparql11-query/#func-datatype
                todo!()            
            }
            Function::Iri =>
            {
                // we have no Datavalue::IRI as return value, it can be added later
                // The return value of IRI is here temporarily Null
                // Details: https://www.w3.org/TR/sparql11-query/#func-iri

                let mut the_str = String::new();
                match &all[0]
                {
                    DataValue::String(this_string) =>
                    {
                        the_str = this_string.to_string();
                    }
                    _=>
                    {
                        return DataValue::Null();       
                    }
                }
                let type_of_data = Self::str_to_datavalue(the_str.into());
                match type_of_data
                {
                    DataValue::WikidataidQ(number) | 
                    DataValue::WikidataidP(number) |
                    DataValue::WikidataidL(number) |
                    DataValue::WikidataidPstmt(number) 
                    =>
                    {
                        //return DataValue::IRI(number);
                        // IRI is not implemented, no replace it temporarily with Null
                        return DataValue::Null();
                    }
                    DataValue::NamedEdge(edge)
                    =>
                    {
                        return DataValue::Null();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::BNode =>
            {
                // we have no Datavalue::blank as return value, so todo here
                // Datavalue::Null will return nothing, it is different from BNode
                // Details: https://www.w3.org/TR/sparql11-query/#func-bnode
                todo!();
            }
            Function::StrDt =>
            {
                // we have no Datavalue::IRI as parameter, so todo here
                // Details: https://www.w3.org/TR/sparql11-query/#func-strdt
                todo!();
            }
            Function::StrLang =>
            {
                // match two parameters Datavalue::string (lexicalForm and langTag)
                // Details: https://www.w3.org/TR/sparql11-query/#func-strlang

                // Initialization
                let mut lexicalForm = String::new();
                let mut langTag = String::new();

                // process the first parameter
                match &all[0]
                {
                    DataValue::String(this_string) =>
                    {
                        lexicalForm = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Null();       
                    }
                }

                // process the second parameter
                match &all[1]
                {
                    DataValue::String(this_string) =>
                    {
                        langTag = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Null();       
                    }
                }
                
                let MonoText = MonolinText {text:lexicalForm.into(), language:langTag.into()};
                return DataValue::MonolinText(Box::new(MonoText));       

            }
            Function::Uuid =>
            {
                // we have no Datavalue::IRI as return value, so todo here
                // Details: https://www.w3.org/TR/sparql11-query/#func-uuid
                todo!()            
            }
            Function::StrUuid =>
            {
                // we have no Datavalue::IRI as return value, so todo here
                // Details: https://www.w3.org/TR/sparql11-query/#func-uuid
                todo!()            
            }
            // String functions
            Function::StrLen =>
            {
                match &all[0] // match the only parameter DataValue::NumberInt/Float in function StrLen
                // Details: https://www.w3.org/TR/sparql11-query/#func-strlen
                {
                    DataValue::String(this_string) =>
                    {
                        let this_str = this_string.as_str();
                        return DataValue::NumberInt((this_str.len()) as i32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::SubStr =>
            {
                // match the only parameter DataValue::String in function SubStr
                // Details: https://www.w3.org/TR/sparql11-query/#func-substr

                // Number of parameters indicator
                // all is made of push, so if second element exists, the first one must exist
                // so it is enough, if only test second and third element exists or not 
                let mut indicator_2_parameter = 0;
                let mut indicator_3_parameter = 0;

                // initialization the whole string, start location, number of substring
                let mut this_str = String::new();
                let mut start_loc = 0 as usize;
                let mut number_of_substr = 0 as usize;

                // process the first argument
                match &all[0]
                {
                    DataValue::String(this_string) =>
                    {
                        this_str = this_string.to_string(); // get whole string             
                    }
                    DataValue::MonolinText(this_string) =>
                    {
                        this_str = this_string.text.to_string(); // get whole string             
                    }
                    _=> 
                    {
                        return DataValue::Null(); // second parameter not exists
                    }
                }
                // process the second argument
                match all[1]
                {
                    DataValue::NumberInt(start) =>
                    {
                        start_loc = start as usize; // get start location
                        indicator_2_parameter = 1; // second parameter exists
                    }
                    _=> 
                    {
                        return DataValue::Null(); // second parameter not exists
                    }
                }
                // process the third argument
                if all.len() == 3
                {
                    match all[2]
                    {
                        DataValue::NumberInt(number) =>
                        {
                            number_of_substr = number as usize; // get number of substring
                            indicator_3_parameter = 1; // third parameter exists
                        }
                        _=> 
                        {
                            // has two parameters
                            return DataValue::Null(); // second parameter not exists
                        }
                    }
                }

                // case distinction
                if (indicator_2_parameter == 0 && indicator_3_parameter == 0) // only one parameter or no parameter
                || (indicator_2_parameter == 0 && indicator_3_parameter == 1) // this case exists not
                {
                    return DataValue::Null();
                }
                else if indicator_2_parameter == 1 && indicator_3_parameter == 0 // has two parameters
                {
                    let start = (start_loc - 1) as usize; // index of start location is (second parameter - 1)
                    let end = this_str.len() as usize; 
                    let sub_str = &this_str[start..end];
                    return DataValue::String(sub_str.to_string().into());
                }
                else // has three parameters
                {
                    let start = start_loc - 1; // index of start location is (second parameter - 1)
                    let end = (start_loc - 1) + number_of_substr; 
                    let sub_str = &this_str[start..end];
                    return DataValue::String(sub_str.to_string().into());
                }
            }
            Function::UCase =>
            {
                match &all[0] // match the only parameter DataValue::String in function Ucase
                // Details: https://www.w3.org/TR/sparql11-query/#func-ucase
                {
                    DataValue::String(this_string) =>
                    {
                        let this_str = this_string.as_str();
                        let this_upper_str = this_str.to_ascii_uppercase();
                        return DataValue::String(this_upper_str.to_string().into());
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::LCase =>
            {
                match &all[0] // match the only parameter DataValue::String in function Lcase
                // Details: https://www.w3.org/TR/sparql11-query/#func-lcase
                {
                    DataValue::String(this_string) =>
                    {
                        let this_str = this_string.as_str();
                        let this_lower_str = this_str.to_ascii_lowercase();
                        return DataValue::String(this_lower_str.to_string().into());
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::StrStarts =>
            {
                // match two parameters DataValue::String in function StrStarts
                // Details: https://www.w3.org/TR/sparql11-query/#func-strstarts

                // Initialization two string
                let mut str1 = String::new();
                let mut str2 = String::new();

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str1 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }
                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str2 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }

                // if str2 is longer than str1
                if str2.len() > str1.len()
                {
                    return DataValue::Boolean(false);
                }

                // get the "head" of str1, it has same length as str2
                let str1_head = &str1[0..str2.len()];
                
                if str1_head == str2
                {
                    return DataValue::Boolean(true);
                }
                else 
                {
                    return DataValue::Boolean(false);
                }
            }
            Function::StrEnds =>
            {
                // match two parameters DataValue::String in function StrEnds
                // Details: https://www.w3.org/TR/sparql11-query/#func-strends

                // Initialization two string
                let mut str1 = String::new();
                let mut str2 = String::new();

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str1 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }
                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str2 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }

                // if str2 is longer than str1
                if str2.len() > str1.len()
                {
                    return DataValue::Boolean(false);
                }

                // get the "tail" of str1, it has same length as str2
                let start_loc = (str1.len() - str2.len()) as usize;
                let end_loc = str1.len() as usize;
                let str1_tail = &str1[start_loc..end_loc];
                
                if str1_tail == str2
                {
                    return DataValue::Boolean(true);
                }
                else 
                {
                    return DataValue::Boolean(false);
                }            
            }
            Function::Contains =>
            {
                // match two parameters DataValue::String in function Contains
                // Details: https://www.w3.org/TR/sparql11-query/#func-contains

                // Initialization two string
                let mut str1 = String::new();
                let mut str2 = String::new();

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str1 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }
                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str2 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }

                // use contains function
                if str1.contains(&str2)
                {
                    return DataValue::Boolean(true);
                }
                else 
                {
                    return DataValue::Boolean(false);
                }            
            }
            Function::StrBefore =>
            {
                // match two parameters DataValue::String in function StrBefore
                // Details: https://www.w3.org/TR/sparql11-query/#func-contains

                // Initialization two string
                let mut str1 = String::new();
                let mut str2 = String::new();

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str1 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str2 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }

                // if str2 empty, return empty string
                if str2.is_empty()
                {
                    let string_empty = String::new();
                    return DataValue::String(string_empty.into());       
                }

                if str1.contains(&str2) // str1 contains str2
                {
                    let start_loc = str1.find(&str2); 
                    match start_loc
                    {
                        Some(i) => //i is the index of the first character of s2 in s1
                        {
                            let str1_before = &str1[0..i]; // get strBefore
                            return DataValue::String(str1_before.to_string().into());       
                        }
                        _=>
                        {
                            return DataValue::Null();
                        }
                    }
                }
                else // str1 doesn't contain str2, return empty string
                {
                    let string_empty = String::new();
                    return DataValue::String(string_empty.into());       
                }
            }
            Function::StrAfter =>
            {
                // match two parameters DataValue::String in function StrAfter
                // Details: https://www.w3.org/TR/sparql11-query/#func-strafter

                // Initialization two string
                let mut str1 = String::new();
                let mut str2 = String::new();

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str1 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str2 = this_string.to_string();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }

                // if str2 empty, return empty string
                if str2.is_empty()
                {
                    let string_empty = String::new();
                    return DataValue::String(string_empty.into());       
                }

                if str1.contains(&str2) // str1 contains str2
                {
                    let start_loc = str1.find(&str2); 
                    match start_loc
                    {
                        Some(i) => //i is the index of the first character of s2 in s1
                        {
                            let last_index_of_str2_in_str1 = i+str2.len(); 
                            //get the index of the last character of s2 in s1
                            let str1_after = &str1[last_index_of_str2_in_str1..str1.len()]; // get strAfter
                            return DataValue::String(str1_after.to_string().into());       
                        }
                        _=>
                        {
                            return DataValue::Null();
                        }
                    }
                }
                else // str1 doesn't contain str2, return empty string
                {
                    let string_empty = String::new();
                    return DataValue::String(string_empty.into());       
                }
            }
            Function::EncodeForUri =>
            {
                // match the only parameters DataValue::String in function ENCODE_FOR_URI
                // Details: https://www.w3.org/TR/sparql11-query/#func-encode

                // encode url to hex string, but skip A-Z and a-z
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        // Initialization str1
                        let mut str1 = this_string.to_string();

                        let str1_chars = str1.chars();
                        let mut vec_u8:Vec<u8> = vec![];
                        
                        for ch in str1_chars
                        {
                            //println!("{}",ch);
                            vec_u8.push(ch as u8);
                        }
                                            
                        // convert url to hex string
                        // test for this function: input "A b", return "A%20b" 
                        pub fn hex_push(blob: &[u8]) -> String
                        {
                            //let blob_string = blob.as_str().chars();
                    
                            let mut buffer = String::new();
                    
                            for ch in blob 
                            {
                                fn hex_from_digit(num: u8) -> char {
                                    if num < 10 {
                                        (b'0' + num) as char
                                    } else {
                                        (b'A' + num - 10) as char
                                    }
                                }
                                if ch >= &65 && ch <= &90 // A - Z, push the same alphabet to buffer
                                {
                                    buffer.push((b'A' + ch - 65) as char);
                                }
                                else if ch >= &97 && ch <= &122 // a - z, push the same alphabet to buffer
                                {
                                    buffer.push((b'a' + ch - 97) as char);
                                }
                                else // convert the other chars to %hex string
                                {
                                    buffer.push('%');
                                    buffer.push(hex_from_digit(ch / 16));
                                    buffer.push(hex_from_digit(ch % 16));
                                }
                    
                            }
                            return buffer;
                        }
                        return DataValue::String(hex_push(&vec_u8).into());
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Concat =>
            {
                // match and concat the every parameter DataValue::String in function Concat
                // Details: https://www.w3.org/TR/sparql11-query/#func-concat

                // Initialization of total string
                let mut sum_string = String::new();

                for string in all
                {
                    match string
                    {
                        DataValue::String(this_string) =>
                        {
                            let this_str = this_string.to_string();
                            sum_string += &this_str;
                        }
                        _=> 
                        {
                            return DataValue::Null();
                        }
                    }
                }
                return DataValue::String(sum_string.into());

            }
            Function::LangMatches =>
            {
                // match two parameters language tag and language range
                // Details: https://www.w3.org/TR/sparql11-query/#func-concat

                // Initialization
                let mut langtag_lower = String::new();
                let mut langrange_lower = String::new();

                // process the first parameter
                match &all[0]
                {
                    DataValue::String(this_string) =>
                    {
                        langtag_lower = this_string.as_str().to_ascii_lowercase();
                    }
                    _=> 
                    {
                        return DataValue::Null();       
                    }
                }

                // process the second parameter
                match &all[1]
                {
                    DataValue::String(this_string) =>
                    {
                        langrange_lower = this_string.as_str().to_ascii_lowercase();
                        // language rang is always in upper case, so convert it to lower case 
                    }
                    _=> 
                    {
                        return DataValue::Null();       
                    }
                }
                let langtag_lower = langtag_lower.as_str();
                let langrange_lower = langrange_lower.as_str();
                if langtag_lower.contains(langrange_lower) // example "fr" and "fr-be" contain "fr"
                {
                    return DataValue::Boolean(true);
                }
                else 
                {
                    return DataValue::Boolean(false);
                }

        }
            Function::Regex =>
            {
                // two parameters DataValue::String (text and pattern)
                // or three parameters DataValue::String (text, pattern and flag)
                // Details: https://www.w3.org/TR/sparql11-query/#func-regex
                // Details for flags: https://www.w3.org/TR/xpath-functions/#flags
                
                // Initialization three parameters
                let mut str_text = "";
                let mut str_pattern = "";
                let mut str_flag = "";

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str_text = this_string.as_str();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }

                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str_pattern = this_string.as_str();
                    }
                    _=> 
                    {
                        return DataValue::Boolean(false);
                    }
                }

                // process third parameter
                if all.len() == 3
                {
                    match &all[2] 
                    {
                        DataValue::String(this_string) =>
                        {
                            str_flag = this_string.as_str();
                        }
                        _=> 
                        {
                            return DataValue::Boolean(false);
                        }
                    }
                }

                // match the flag
                // use the regex flag, details: https://docs.rs/regex/latest/regex/#grouping-and-flags
                match str_flag
                {
                    "i" => // case-insensitive mode 
                    {
                        let mut pattern_i = String::from("(?i)");
                        pattern_i += str_pattern;
                        let re = Regex::new(&pattern_i).unwrap();
                        return DataValue::Boolean(re.is_match(&str_text));
                    }

                    "q" | "qm" | "mq" | "qs" | "sq" | "qx" | "xq" => 
                    // all characters in the regular expression are treated as representing themselves
                    // not as metacharacters
                    // If q is used together with the m, s, or x flag, that flag has no effect.
                    {
                        if str_text.contains(str_pattern)
                        {
                            return DataValue::Boolean(true);    
                        }
                        else 
                        {
                            return DataValue::Boolean(false);    
                        }
                    }
                    
                    "iq" | "qi" => // q flag can be used in conjunction with the i flag
                    {
                        if str_text.contains(&str_pattern) // q has been satisfied
                        {
                            let mut pattern_iq = String::from("(?i)");
                            pattern_iq += str_pattern;    
                            let re = Regex::new(&str_pattern).unwrap();
                            return DataValue::Boolean(re.is_match(&str_text));   
                            // q, i have been satisfied 
                        }
                        else 
                        {
                            return DataValue::Boolean(false);    
                        }
                    }

                    "x" => 
                    // If present, whitespace characters (#x9, #xA, #xD and #x20 (in hex)) in the regular expression 
                    // are removed prior to matching with one exception: 
                    // whitespace characters within character class expressions (charClassExpr) (\s)are not removed. 
                    {
                        let mut pattern_x = String::from("(?x)");
                        pattern_x += str_pattern;
                        let re = Regex::new(&pattern_x).unwrap();
                        return DataValue::Boolean(re.is_match(str_text));

                    }

                    "s" => // "dot-all" mode
                    // In dot-all mode, the meta-character . matches any character whatsoever.
                    // In normal mode, . matches any character except #x0A (newline) and #x0D (return)
                    {
                        let mut pattern_s = String::from("(?s)");
                        pattern_s += str_pattern;
                        let re = Regex::new(&pattern_s).unwrap();
                        return DataValue::Boolean(re.is_match(&str_text));
                    }

                    "m" => // "multi-line mode"
                    // By default, the meta-character ^ matches the start of the entire string, 
                    // while $ matches the end of the entire string. 
                    // In multi-line mode, ^ matches the start of any line and $ matches the end of the any line
                    {
                        let mut pattern_m = String::from("(?m)");
                        pattern_m += str_pattern;
                        let re = Regex::new(&pattern_m).unwrap();
                        return DataValue::Boolean(re.is_match(&str_text));
                    }
                    _=> // no flag, it has two parameters, so use standard Regex function
                    {
                        let re = Regex::new(str_pattern).unwrap();
                        return DataValue::Boolean(re.is_match(str_text));
                    }
                }       
            }
            Function::Replace =>
            {
                // Details: https://www.w3.org/TR/sparql11-query/#func-replace
                // Details for Xpath function: https://www.w3.org/TR/xpath-functions/#func-replace
                // three parameters DataValue::String text, pattern and replacement
                // or four parameters DataValue::Stringtext text, pattern, replacement and flag
                // If the input string contains no substring that matches the regular expression, 
                // the result of the function is a single string identical to the input string.

                // Initialization four parameters
                let mut str_text = "";
                let mut str_pattern = "";
                let mut str_replacement = "";
                let mut str_flag = "";

                // process first parameter 
                match &all[0] 
                {
                    DataValue::String(this_string) =>
                    {
                        str_text = this_string.as_str();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }

                // process second parameter
                match &all[1] 
                {
                    DataValue::String(this_string) =>
                    {
                        str_pattern = this_string.as_str();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }

                // process third parameter
                match &all[2] 
                {
                    DataValue::String(this_string) =>
                    {
                        str_replacement = this_string.as_str();
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }

                // process fourth parameter
                if all.len() == 4
                {
                    match &all[3] 
                    {
                        DataValue::String(this_string) =>
                        {
                            str_flag = this_string.as_str();
                        }
                        _=> 
                        {
                            // fourth parameter does not exist, here do nothing
                        }
                    }
                }

                // match the flag
                // use the regex flag, details: https://docs.rs/regex/latest/regex/#grouping-and-flags
                match str_flag
                {
                    "i" => // case-insensitive mode 
                    {
                        let mut pattern_i = String::from("(?i)");
                        pattern_i += str_pattern;
                        let re = Regex::new(&pattern_i).unwrap();
                        if re.is_match(str_text) // if match
                        {
                            // find and store all matched substring from text into array matches
                            let matches: Vec<_> = re.find_iter(str_text).map(|m| m.as_str()).collect();
                            let mut replaced_text = str_text.to_string();
                            // replace all matched substring to replacement
                            for substr in matches
                            {
                                replaced_text = replaced_text.replace(substr, str_replacement);
                            }
                            return DataValue::String(replaced_text.to_string().into());
                        }
                        else // does not match, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }
                    }

                    "q" | "qm" | "mq" | "qs" | "sq" | "qx" | "xq" => 
                    // all characters in the regular expression are treated as representing themselves
                    // not as metacharacters
                    // If q is used together with the m, s, or x flag, that flag has no effect.
                    // If the q flag is present, the replacement string is used as is.
                    {
                        if str_text.contains(str_pattern) // q has been satisfied
                        {
                            let replaced_text = str_text.replace(str_pattern, str_replacement);
                            return DataValue::String(replaced_text.to_string().into());
                        }
                        else // does not contain, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }
                    }
                    
                    "iq" | "qi" => // q flag can be used in conjunction with the i flag
                    {
                        if str_text.contains(&str_pattern) // q has been satisfied
                        {
                            let mut pattern_i = String::from("(?i)");
                            pattern_i += str_pattern;
                            let re = Regex::new(&pattern_i).unwrap();
                            if re.is_match(str_text) // if match
                            {
                                // find and store all matched substring from text into array matches
                                let matches: Vec<_> = re.find_iter(str_text).map(|m| m.as_str()).collect();
                                let mut replaced_text = str_text.to_string();
                                // replace all matched substring to replacement
                                for substr in matches
                                {
                                    replaced_text = replaced_text.replace(substr, str_replacement);
                                }
                                return DataValue::String(replaced_text.to_string().into());
                            }
                            else // does not match, return text
                            {
                                return DataValue::String(str_text.to_string().into());
                            }
                        }
                        else // does not contain, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }

                    }

                    "x" => 
                    // If present, whitespace characters (#x9, #xA, #xD and #x20 (in hex)) in the regular expression 
                    // are removed prior to matching with one exception: 
                    // whitespace characters within character class expressions (charClassExpr) (\s)are not removed. 
                    {
                        let mut pattern_x = String::from("(?x)");
                        pattern_x += str_pattern;
                        let re = Regex::new(&pattern_x).unwrap();
                        if re.is_match(str_text) // if match
                        {
                            // find and store all matched substring from text into array matches
                            let matches: Vec<_> = re.find_iter(str_text).map(|m| m.as_str()).collect();
                            let mut replaced_text = str_text.to_string();
                            // replace all matched substring to replacement
                            for substr in matches
                            {
                                replaced_text = replaced_text.replace(substr, str_replacement);
                            }
                            return DataValue::String(replaced_text.to_string().into());
                        }
                        else // does not match, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }
                    }

                    "s" => // "dot-all" mode
                    // In dot-all mode, the meta-character . matches any character whatsoever.
                    // In normal mode, . matches any character except #x0A (newline) and #x0D (return)
                    {
                        let mut pattern_s = String::from("(?s)");
                        pattern_s += str_pattern;
                        let re = Regex::new(&pattern_s).unwrap();
                        if re.is_match(str_text) // if match
                        {
                            // find and store all matched substring from text into array matches
                            let matches: Vec<_> = re.find_iter(str_text).map(|m| m.as_str()).collect();
                            let mut replaced_text = str_text.to_string();
                            // replace all matched substring to replacement
                            for substr in matches
                            {
                                replaced_text = replaced_text.replace(substr, str_replacement);
                            }
                            return DataValue::String(replaced_text.to_string().into());
                        }
                        else // does not match, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }
                    }

                    "m" => // "multi-line mode"
                    // By default, the meta-character ^ matches the start of the entire string, 
                    // while $ matches the end of the entire string. 
                    // In multi-line mode, ^ matches the start of any line and $ matches the end of the any line
                    {
                        let mut pattern_m = String::from("(?m)");
                        pattern_m += str_pattern;
                        let re = Regex::new(&pattern_m).unwrap();
                        if re.is_match(str_text) // if match
                        {
                            // find and store all matched substring from text into array matches
                            let matches: Vec<_> = re.find_iter(str_text).map(|m| m.as_str()).collect();
                            let mut replaced_text = str_text.to_string();
                            // replace all matched substring to replacement
                            for substr in matches
                            {
                                replaced_text = replaced_text.replace(substr, str_replacement);
                            }
                            return DataValue::String(replaced_text.to_string().into());
                        }
                        else // does not match, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }
                    }
                    _=> // no flag, use str.replace function
                    {
                        let re = Regex::new(&str_pattern).unwrap();
                        if re.is_match(str_text) // if match
                        {
                            // find and store all matched substring from text into array matches
                            let matches: Vec<_> = re.find_iter(str_text).map(|m| m.as_str()).collect();
                            let mut replaced_text = str_text.to_string();
                            // replace all matched substring to replacement
                            for substr in matches
                            {
                                replaced_text = replaced_text.replace(substr, str_replacement);
                            }
                            return DataValue::String(replaced_text.to_string().into());
                        }
                        else // does not match, return text
                        {
                            return DataValue::String(str_text.to_string().into());
                        }
                    }
                }       
            }
            // numerics functions
            Function::Abs =>
            {
                match all[0] // match the only parameter DataValue::NumberInt/Float in function Abs
                // Details: https://www.w3.org/TR/sparql11-query/#func-abs
                {
                    DataValue::NumberInt(number) =>
                    {
                        let number_abs = number.abs();
                        return DataValue::NumberInt(number_abs as i32);
                    }
                    DataValue::NumberFloat(number) =>
                    {
                        let number_abs = number.abs();
                        return DataValue::NumberFloat(number_abs as f32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Round =>
            {
                match all[0] // match the only parameter DataValue::NumberInt/Float in function Round
                // Details: https://www.w3.org/TR/sparql11-query/#func-round
                {
                    DataValue::NumberInt(number) =>
                    {
                        return DataValue::NumberFloat(number as f32);
                    }
                    DataValue::NumberFloat(number) =>
                    {
                        let number_int = number as i32; 
                        // get the integer part of this float, example: number = 2.9, number_int = 2
                        if number > 0.0 
                        {
                            if number < (number_int as f32 + 0.5)  // example: number = 2.49 < 2.5
                            {
                                return DataValue::NumberFloat(number_int as f32); // return 2.0
                            }
                            else // example: number = 2.5 >= 2.5
                            {
                                return DataValue::NumberFloat((number_int + 1) as f32); // return 3.0 
                            }
                        }
                        else if number < 0.0
                        {
                            if number < (number_int as f32 - 0.5) // example: number = -2.6 < -2.5
                            {
                                return DataValue::NumberFloat((number_int - 1) as f32); // return -3.0
                            }
                            else // example: number = -2.5 >= -2.5
                            {
                                return DataValue::NumberFloat(number_int as f32); // return -2.0
                            }
                        }
                        else // 0
                        {
                            return DataValue::NumberFloat(number_int as f32);
                        }
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Ceil =>
            {
                match all[0] // match the only parameter DataValue::NumberInt/Float in function Ceil
                // Details: https://www.w3.org/TR/sparql11-query/#func-ceil
                {
                    DataValue::NumberInt(number) =>
                    {
                        return DataValue::NumberInt(number as i32);
                    }
                    DataValue::NumberFloat(number) =>
                    {
                        let number_int = number as i32;
                        if number > 0.0 // example: number = 2.1, number_int = 2
                        {
                            return DataValue::NumberFloat((number_int + 1) as f32); // return 3.0
                        }
                        else if number < 0.0 // example: number = -2.1, number_int = -2
                        {
                            return DataValue::NumberFloat(number_int as f32); // return -2.0
                        }
                        else // 0
                        {
                            return DataValue::NumberFloat(number as f32);
                        }
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Floor =>
            {
                match all[0] // match the only parameter DataValue::NumberInt/Float in function Floor
                // Details: https://www.w3.org/TR/sparql11-query/#func-floor
                {
                    DataValue::NumberInt(number) =>
                    {
                        return DataValue::NumberInt(number as i32);
                    }
                    DataValue::NumberFloat(number) =>
                    {
                        let number_int = number as i32;
                        if number > 0.0 // example: number = 2.1, number_int = 2
                        {
                            return DataValue::NumberFloat(number_int as f32); // return 2.0
                        }
                        else if number < 0.0 // example: number = -2.1, number_int = -2
                        {
                            return DataValue::NumberFloat((number_int - 1) as f32); // return -3.0
                        }
                        else // 0
                        {
                            return DataValue::NumberFloat(number as f32);
                        }
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Rand =>
            {
                // Details: https://www.w3.org/TR/sparql11-query/#idp2130040
                // Returns a pseudo-random number between 0 (inclusive) and 1.0e0 (exclusive)
                let mut random: ThreadRng = rand::thread_rng();
                let x0_1: f32 = random.gen_range(0.0,1.0e0);
                return DataValue::NumberFloat(x0_1);
            }

            // Time functions 
            Function::Now =>
            {
                // Details: https://www.w3.org/TR/sparql11-query/#func-now   
                // function Now has no parameter, direct return the time of current query
                // but xsd:dataTime has not implemented yet, So temporarily write it as todo
                todo!();
            }
            Function::Year =>
            {
                match all[0] // match the only parameter DataValue::Time in function year
                // Details: https://www.w3.org/TR/sparql11-query/#func-year
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::NumberInt(time.year as i32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Month =>
            {
                match all[0] // match the only parameter DataValue::Time in function Month
                // Details: https://www.w3.org/TR/sparql11-query/#func-month
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::NumberInt(time.month as i32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Day =>
            {
                match all[0] // match the only parameter DataValue::Time in function Day
                // Details: https://www.w3.org/TR/sparql11-query/#func-day                
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::NumberInt(time.day as i32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Hours =>
            {
                match all[0] // match the only parameter DataValue::Time in function Hours
                // Details: https://www.w3.org/TR/sparql11-query/#func-hours  
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::NumberInt(time.hour as i32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
          
            }
            Function::Minutes =>
            {
                match all[0] // match the only parameter DataValue::Time in function Minutes
                // Details: https://www.w3.org/TR/sparql11-query/#func-minutes                
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::NumberInt(time.minute as i32);
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
          
            }
            Function::Seconds =>
            {
                match all[0] // match the only parameter DataValue::Time in function Seconds
                // Details: https://www.w3.org/TR/sparql11-query/#func-seconds              
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::NumberInt(time.second as i32); 
                        // Datavalue has no decimal typ, but second in time is a u8, so i still use i32 here
                    }
                    _=> 
                    {
                        return DataValue::Null();
                    }
                }
            }
            Function::Timezone =>
            {
                // Details: https://www.w3.org/TR/sparql11-query/#func-timezone      
                // Timezone is xsd:dayTimeDuration, but not implemented yet, So temporarily write it as todo
                todo!();
            }
            Function::Tz =>
            {
                match all[0] // match the only parameter DataValue::Time in function Tz
                // Details: https://www.w3.org/TR/sparql11-query/#func-tz             
                {
                    DataValue::Time(time) =>
                    {
                        return DataValue::String(time.timezone.to_string().into());
                        // convert timezone to Datavalue::string
                    }
                    _=> 
                    {
                        let timezone_string = String::new();
                        return DataValue::String(timezone_string.into());
                        // convert empty string to Datavalue::string
                    }
                }            
            }
            // Hash functions
            Function::Md5 =>
            {
                let test_null = 0;
                let test_has = 1;
                match &all[0] // match the only parameter DataValue::String in function md5
                // Details: https://www.w3.org/TR/sparql11-query/#func-md5   
                // use md5 module        
                {
                    DataValue::String(original_str)=>
                    {                        
                        let s_str = original_str.to_string();
                        let mut hasher = Md5::new();
                        hasher.input_str(&s_str);   
                        let hasher_small_string = hasher.result_str().into();
                        return DataValue::String(hasher_small_string);
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }

                }            
            }
            Function::Sha1 =>
            {
                match &all[0] // match the only parameter DataValue::String in function Sha1
                // Details: https://www.w3.org/TR/sparql11-query/#func-sha1   
                // use sha1 module        
                {
                    DataValue::String(original_str)=>
                    {
                        let s_str = original_str.to_string();
                        let mut hasher = Sha1::new();
                        hasher.input_str(&s_str);   
                        let hasher_small_string = hasher.result_str().into();
                        return DataValue::String(hasher_small_string);
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }
                }            
            }
            Function::Sha256 =>
            {
                match &all[0] // match the only parameter DataValue::String in function Sha256
                // Details: https://www.w3.org/TR/sparql11-query/#func-sha256   
                // use Sha2::Sha256 module        
                {
                    DataValue::String(original_str)=>
                    {
                        let s_str = original_str.to_string();
                        let mut hasher = Sha256::new();
                        hasher.input_str(&s_str);   
                        let hasher_small_string = hasher.result_str().into();
                        return DataValue::String(hasher_small_string);
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }

                }            
            }
            Function::Sha384 =>
            {
                match &all[0] // match the only parameter DataValue::String in function Sha384
                // Details: https://www.w3.org/TR/sparql11-query/#func-sha384
                // use Sha2::Sha384 module        
                {
                    DataValue::String(original_str)=>
                    {
                        let s_str = original_str.to_string();
                        let mut hasher = Sha384::new();
                        hasher.input_str(&s_str);   
                        let hasher_small_string = hasher.result_str().into();
                        return DataValue::String(hasher_small_string);
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }

                }            
            }
            Function::Sha512 =>
            {
                match &all[0] // match the only parameter DataValue::String in function Sha512
                // Details: https://www.w3.org/TR/sparql11-query/#func-sha512
                // use Sha2::Sha512 module        
                {
                    DataValue::String(original_str)=>
                    {
                        let s_str = original_str.to_string();
                        let mut hasher = Sha512::new();
                        hasher.input_str(&s_str);   
                        let hasher_small_string = hasher.result_str().into();
                        return DataValue::String(hasher_small_string);
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }

                }              
            }
            // this function supports the label service 
            Function::getLabel =>
            {
                // This function will return the first object that meets the conditions
                // It has only one return value, so only the first founded object will be returned
                // The object has first parameter(WikidataId) as subject and one of the other parameters(language label) as predicate

                // Initialization of nodes
                let mut wikidata_id = Node { id: 0 };
                let mut lang_tag = Node {id: 0};

                let mut found = false;
                let mut id_value:u64 = 0;
                // create object node
                let mut object_node = Node{id:0};

                match all[0] // match the first parameter wikidataid     
                {
                    DataValue::WikidataidQ(number) | DataValue::WikidataidP(number)=>
                    {
                        // convert the Datavalue to node
                        wikidata_id = StorageEngine::get_node_from_datavalue(&self.storage, all[0].clone(),Some(query_id));
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }
                }

                // match the second until the last parameter, they should be language tag
                for i in 1..all.len()
                {
                    lang_tag = Node {id: 0}; // reset the lang_tag at the begin of each iteration
                    match &all[i]
                    {
                        DataValue::String(langtag) =>
                        {
                            // convert string to Datavalue::Label
                            let langtag_label = DataValue::Label(langtag.clone());
                            // convert the Datavalue to node
                            lang_tag = StorageEngine::get_node_from_datavalue(&self.storage, langtag_label,Some(query_id));

                            // find if there is a edge, which has first parameter as subject and lang_tag as predicate. 
                            let find_relation = StorageEngine::get_all_object_edges_of(&self.storage, wikidata_id, lang_tag);
                            // get the number of row
                            let row_number = find_relation.get_row_count();

                            // get this edge from relation
                            if row_number == 0 
                            {
                                continue; // no founded edge in this language, then try the next
                            }
                            else 
                            {
                                found = true; // edge founded
                                // choice the third element in first row as object
                                // the struct of row/edge: each row has 4 nodes: subject predicate object id
                                // here get the third element of row, the object
                                object_node = find_relation.get_row(0).to_vec()[2]; 
                                break; // object founded, so end the loop
                            }
                        }
                        _=> // not language label
                        {
                            return DataValue::Null();
                        }
                    }
                }

                if found
                {
                    // convert the node to datavalue
                    return StorageEngine::retrieve_data(&self.storage, object_node);
                }
                else 
                {
                    return DataValue::Null();
                }
            }
            Function::getDescription =>
            {
                // Initialization of nodes
                let mut wikidata_id = Node { id: 0 };
                let mut lang_tag = Node {id: 0};

                let mut found = false;
                let mut id_value:u64 = 0;
                // create object node
                let mut object_node = Node{id:0};

                match all[0] // match the first parameter wikidataid     
                {
                    DataValue::WikidataidQ(number) | DataValue::WikidataidP(number)=>
                    {
                        // convert the Datavalue to node
                        wikidata_id = StorageEngine::get_node_from_datavalue(&self.storage, all[0].clone(),Some(query_id));
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }
                }

                // match the second until the last parameter, they should be language tag
                for i in 1..all.len()
                {
                    lang_tag = Node {id: 0}; // reset the lang_tag at the begin of each iteration
                    match &all[i]
                    {
                        DataValue::String(langtag) =>
                        {
                            // convert string to Datavalue::Description
                            let langtag_label = DataValue::Description(langtag.clone());
                            // convert the Datavalue to node
                            lang_tag = StorageEngine::get_node_from_datavalue(&self.storage, langtag_label,Some(query_id));

                            // find if there is a edge, which has first parameter as subject and lang_tag as predicate. 
                            let find_relation = StorageEngine::get_all_object_edges_of(&self.storage, wikidata_id, lang_tag);
                            // get the number of row
                            let row_number = find_relation.get_row_count();

                            // get this edge from relation
                            if row_number == 0 
                            {
                                continue; // no founded edge in this language, then try the next
                            }
                            else 
                            {
                                found = true; // edge founded
                                // choice the third element in first row as object
                                // the struct of row/edge: each row has 4 nodes: subject predicate object id
                                // here get the third element of row, the object
                                object_node = find_relation.get_row(0).to_vec()[2]; 
                                break; // object founded, so end the loop
                            }
                        }
                        _=> // not language label
                        {
                            return DataValue::Null();
                        }
                    }
                }

                if found == true
                {
                    // convert the node to datavalue
                    return StorageEngine::retrieve_data(&self.storage, object_node);
                }
                else 
                {
                    return DataValue::Null();
                }
            }
            Function::getAlias =>
            {
                // Initialization of nodes
                let mut wikidata_id = Node { id: 0 };
                let mut lang_tag = Node {id: 0};

                let mut found = false;
                let mut id_value:u64 = 0;
                // create object node
                let mut object_node = Node{id:0};

                match all[0] // match the first parameter wikidataid     
                {
                    DataValue::WikidataidQ(number) | DataValue::WikidataidP(number)=>
                    {
                        // convert the Datavalue to node
                        wikidata_id = StorageEngine::get_node_from_datavalue(&self.storage, all[0].clone(),Some(query_id));
                    }
                    _=>
                    {
                        return DataValue::Null();
                    }
                }

                // create the string for all aliases
                let mut alias_str = String::new();
                // match the second until the last parameter, they should be language tag
                for i in 1..all.len()
                {
                    lang_tag = Node {id: 0}; // reset the lang_tag at the begin of each iteration
                    match &all[i]
                    {
                        DataValue::String(langtag) =>
                        {
                            // convert string to Datavalue::Alias
                            let langtag_label = DataValue::Alias(langtag.clone());
                            // convert the Datavalue to node
                            lang_tag = StorageEngine::get_node_from_datavalue(&self.storage, langtag_label,Some(query_id));

                            // find if there is a edge, which has first parameter as subject and lang_tag as predicate. 
                            let find_relation = StorageEngine::get_all_object_edges_of(&self.storage, wikidata_id, lang_tag);
                            // get the number of row
                            let row_number = find_relation.get_row_count();
                            // get this edge from relation
                            if row_number == 0 
                            {
                                continue; // no founded edge in this language, then try the next
                            }
                            else 
                            {
                                found = true; // edge founded
                                // choice the third element in first row as object
                                // the struct of row/edge: each row has 4 nodes: subject predicate object id
                                // here get the third element of row, the object
                                // get all alias from this language
                                for alias_index in 0..row_number
                                {
                                    object_node = find_relation.get_row(alias_index).to_vec()[2]; 
                                    // convert node to Datavalue.
                                    let object_datavalue = StorageEngine::retrieve_data(&self.storage, object_node);
                                    match object_datavalue
                                    {
                                        DataValue::String(this_string) =>
                                        {
                                            alias_str += this_string.as_str();                                        }
                                        _=>
                                        {
                                            alias_str += "";
                                        }
                                    }
                                    alias_str += ",";
                                }
                            }
                        }
                        _=> // not language label
                        {
                            return DataValue::Null();
                        }
                    }
                }
                if found == true
                {
                    // convert the node to datavalue
                    // return StorageEngine::retrieve_data(&self.storage, object_node);
                    return DataValue::String(alias_str.into());
                }
                else 
                {
                    return DataValue::Null();
                }

            }
            // the custom function
            Function::Custom(pattern) => 
            {
                todo!();
            }

        }
    }
    
    fn equal(arg1: DataValue, arg2: DataValue) -> bool{
        match (&arg1,&arg2) {
            (&DataValue::NumberFloat(value1),&DataValue::NumberInt(value2)) => value1 == value2 as f32,
            (&DataValue::NumberInt(value1), &DataValue::NumberFloat(value2)) => value2 == value1 as f32,
            _ => arg1 == arg2
        }
    }

    fn calculation_on_numbers(arg1: DataValue, arg2: DataValue, kind: char) -> DataValue<'a>{
        match arg1{
            DataValue::NumberFloat(value1)=> {
                match arg2{
                    DataValue::NumberFloat(value2) => return Self::type_of_calculation(value1, value2, kind),
                    DataValue::NumberInt(value2) => return Self::type_of_calculation(value1, value2 as f32, kind),
                    _ => return DataValue::Null(),
                }
            },
            DataValue::NumberInt(value1) => {
                match arg2{
                    DataValue::NumberFloat(value2) => return Self::type_of_calculation(value1 as f32, value2, kind),
                    DataValue::NumberInt(value2) => return Self::type_of_calculation(value1 as f32, value2 as f32, kind),
                    _ => return DataValue::Null(),
                }
            },
            _ => return DataValue::Null(),
        }
    }

    fn type_of_calculation(arg1: f32, arg2: f32, kind: char) -> DataValue<'a>{
        if kind == 'A'{
            return DataValue::NumberFloat(arg1 + arg2)
        }
        if kind == 'S'{
            return DataValue::NumberFloat(arg1 - arg2)
        }
        if kind == 'D'{
            return DataValue::NumberFloat(arg1 / arg2)
        }
        if kind == 'M'{
            return DataValue::NumberFloat(arg1 * arg2)
        }
        if kind == 'P'{
            return DataValue::NumberFloat(arg1.abs())
        }
        if kind == 'I'{
            return DataValue::NumberFloat(-arg1)
        }
        return DataValue::Null()
    }
    
    fn pattern_to_str(pattern: &Pattern)->&str {
        match pattern {
            Pattern::Variable { name }=>
            {
                return name.as_str();
            }
            Pattern::NamedNode { name }=>
            {
                return name.as_str();
            }
            Pattern::BlankNode { name  }=>
            {
                return name.as_str();
            }
            Pattern::Literal { name }=>
            {
                return name.as_str();
            }
        }
    }

    fn pattern_to_string(pattern: &Pattern)->String {
        match pattern {
            Pattern::Variable { name }=>
            {
                name.clone()
            }
            Pattern::NamedNode { name }=>
            {
                name.clone()
            }
            Pattern::BlankNode { name  }=>
            {
                name.clone()
            }
            Pattern::Literal { name }=>
            {
                name.clone()
            }
        }
    }


    fn str_to_datavalue(input : SmallString) -> DataValue {
        let input_str = input.to_string();
        let re_item  = Regex::new( r"^http://www.wikidata.org/entity/Q(\d+)").unwrap();
        if let Some(caps) = re_item.captures(&input_str){
            return DataValue::WikidataidQ(
                str::parse::<u64>(caps.get(1).unwrap().as_str()).unwrap());        
        }    

        let input_str = input.to_string();
        let re_item  = Regex::new( r"^http://www.wikidata.org/entity/L(\d+)").unwrap();
        if let Some(caps) = re_item.captures(&input_str) {
            return DataValue::WikidataidL(
                str::parse::<u64>(caps.get(1).unwrap().as_str()).unwrap());        
        }    


        let re_direct_prop  = Regex::new( r"^http://www.wikidata.org/prop/direct/P(\d+)").
            unwrap();
        if let Some(caps) = re_direct_prop.captures(&input_str){
            return DataValue::WikidataidP(
                str::parse::<u64>(caps.get(1).unwrap().as_str()).unwrap());        
        }    
        

        let re_stat_prop = Regex::new(r"http://www.wikidata.org/prop/P(\d+)").unwrap();
        if let Some(caps) = re_stat_prop.captures(&input_str){
            return DataValue::WikidataidPstmt(
                str::parse::<u64>(caps.get(1).unwrap().as_str()).unwrap());        
        }    

        let re_stat = Regex::new(r"http://www.wikidata.org/entity/statement/(\d+)")
            .unwrap();
        if let Some(caps) = re_stat.captures(&input_str){
            let stmt_id = SmallString::from(caps.get(1).unwrap().as_str().to_owned().replace('$', "-"));
            return DataValue::NamedEdge(stmt_id);
        }        
        // else
        DataValue::String(input)
    }




    fn triplepattern_match(&'a self, s: &Pattern, p: &Pattern, o: &Pattern) 
                           -> Box<dyn Relation<'a>+'a> {
        let empty_rel = Box::new(MaterializedRelation::create_mat_relation(
                        vec!["Subject".to_string(), "Predicate".to_string(),
                             "Object".to_string(), "ID".to_string()], &[]));


        let mut rename = HashMap::with_capacity(4);
        rename.insert("Subject", Self::pattern_to_str(s));
        rename.insert("Predicate", Self::pattern_to_str(p));

        // p_local always holds the WikidataIdP Variant, cause that is 
        // what is in the relations stored by the db
        // p can also refer to WikidataIdPstmt
        let mut p_local = p.clone();
        let mut stmt_pred = false; 
        if let Pattern::NamedNode{ name } = p {
            if let DataValue::WikidataidPstmt(pred_id) = Self::str_to_datavalue(
                                               SmallString::from(name.to_string())){
                stmt_pred = true; 
                p_local = Pattern::NamedNode { 
                    name: format!("http://www.wikidata.org/prop/direct/P{}", pred_id) };
            }
        }            


        let mut stmt_object = false;
        if let Pattern::NamedNode{ name } = o {
            if let DataValue::NamedEdge(_) = Self::str_to_datavalue(
                                               SmallString::from(name.to_string())){
                stmt_object = true; 
                }  
        }

        let result = match (s,p,o) {
            (Pattern::Variable {name: _ }, 
             Pattern::Variable {name: _ }, 
             Pattern::Variable {name: _ }) => {
                // this is not feasible already for modest sized databases
                self.storage.get_all_edges()
            },

            (Pattern::Variable {name: _ }, 
             Pattern::Variable {name: _ }, 
             _) => {
                if stmt_object {
                    rename.insert("ID", Self::pattern_to_str(o));
                    self.direct_pred_to_stmt_pred(
                        self.storage.get_nodes_from_edge_id(self.pattern_to_node(o)), 
                        vec!["Predicate"])
                        
                } else {
                    rename.insert("Object", Self::pattern_to_str(o));
                    self.storage.get_all_edges_using_object(self.pattern_to_node(o))
                }

            },

            (Pattern::Variable {name: _ }, 
             _, 
             Pattern::Variable {name: _ }) => {
                if stmt_pred {
                        rename.insert("ID", Self::pattern_to_str(o));
                } else {
                    rename.insert("Object", Self::pattern_to_str(o));
                }
                self.storage.get_all_edges_using_predicat(self.pattern_to_node(&p_local))
            },

            (Pattern::Variable {name: _ }, 
             _, 
             _) => {
                // predicate and object align
                if (stmt_object && stmt_pred) || (!stmt_object && !stmt_pred){      
                    if stmt_object {
                        rename.insert("ID", Self::pattern_to_str(o));
                        let edge = self.storage.get_nodes_from_edge_id(
                            self.pattern_to_node(o));
                        // query predicate aligns with stored predicate
                        // in edge sorted id, "Predicate" is column 2 
                        if (edge.get_row_count() > 0 && 
                            self.pattern_to_node(&p_local) == edge[(0, 2)]) 
                        {
                            self.direct_pred_to_stmt_pred(
                                edge, 
                                vec!["Predicate"])
                        } else {
                            empty_rel
                        }
                    } else {
                        rename.insert("Object", Self::pattern_to_str(o));
                        self.storage.get_all_subject_edges_of(
                            self.pattern_to_node(&p_local),
                            self.pattern_to_node(o))
                    }
                // stmt and object don't align:  searching for an impossible combination 
                } else {
                    if stmt_object {
                        rename.insert("ID", Self::pattern_to_str(o));
                    }
                    else {
                        rename.insert("Object", Self::pattern_to_str(o));
                    }
                    empty_rel
                }
                
            },

            (_, 
             Pattern::Variable {name: _ }, 
             Pattern::Variable {name: _ }) => {
                rename.insert("Object", Self::pattern_to_str(o));
                let direct_rel_iter = self.storage.get_all_edges_using_subject(
                    self.pattern_to_node(s)).to_materialized().into_vec_iter();
                let stmt_rel_iter = 
                    self.direct_pred_to_stmt_pred(
                        self.storage.get_all_edges_using_subject(
                            self.pattern_to_node(s)), 
                        vec!["Predicate"]).to_materialized().into_vec_iter().
                    map(|mut row_vec| -> Vec<Node> {
                        row_vec.swap(2,3);
                        row_vec});;
                let rename = HashMap::from([("0", "Subject"), 
                                            ("1", "Predicate"), 
                                            ("2", "Object"), 
                                            ("3", "ID")]);
                Box::new(MaterializedRelation::from_iter(
                    direct_rel_iter.chain(stmt_rel_iter))).
                    rename_attributes(&rename)
            },

            (_, 
             Pattern::Variable {name: _ }, 
             _) => {
                if stmt_object{
                    rename.insert("ID", Self::pattern_to_str(o));
                    let edge = self.storage.get_nodes_from_edge_id(
                        self.pattern_to_node(o));
                    // query subject aligns with stored subject
                    // in edge sorted id, "Subject" is column 1
                        if (edge.get_row_count() > 0 &&
                            self.pattern_to_node(s) == edge[(0, 1)]){
                            self.direct_pred_to_stmt_pred(
                                edge, 
                                vec!["Predicate"])
                        } else {
                            empty_rel
                        }
                   
                } else {
                    rename.insert("Object", Self::pattern_to_str(o));                    
                    self.storage.get_all_predicat_edges_of(
                        self.pattern_to_node(s), 
                        self.pattern_to_node(o))
                }
            },

            (_, 
             _,
             Pattern::Variable {name: _ }) => {
                let plain_match =  self.storage.get_all_object_edges_of(
                    self.pattern_to_node(s),
                    self.pattern_to_node(&p_local));
                if stmt_pred {
                    rename.insert("ID", Self::pattern_to_str(o));
                    plain_match
                } else {
                    rename.insert("Object", Self::pattern_to_str(o));
                    self.direct_pred_to_stmt_pred(plain_match, vec!["Predicate"])
                }

            },

            (_,_,_) => {                
                if stmt_object  {
                    rename.insert("ID", Self::pattern_to_str(o));
                    if ( stmt_pred ){
                        let edge = self.storage.get_nodes_from_edge_id(
                            self.pattern_to_node(o));
                        // query subject and pred align with stored subject and pred
                        // in edge sorted id, "Subject" is column 1
                        // and "Predicate" is column 2
                        if (edge.get_row_count() > 0 &&
                            self.pattern_to_node(s) == edge[(0, 1)] &&
                            self.pattern_to_node(&p_local) == edge[(0, 2)])
                        {
                            self.direct_pred_to_stmt_pred(
                                edge, 
                                vec!["Predicate"])
                        } else { // subject or predicate don't align with edge
                            empty_rel
                        }

                    } else {
                        empty_rel
                    }                    
                } else {
                    if !stmt_pred {
                        rename.insert("Object", Self::pattern_to_str(o));
                        self.storage.get_rows_from_subject_pred_object(
                            self.pattern_to_node(s),
                            self.pattern_to_node(&p_local), 
                            self.pattern_to_node(o))
                        } else {
                        empty_rel
                    }
                }
            },
        };
        result.rename_attributes(&rename)
    }

    fn pattern_to_node(&self,pattern: &Pattern)-> Node {
        match pattern {
            Pattern::Variable { name } |
            Pattern::NamedNode { name } | 
            Pattern::BlankNode { name } => {
                self.storage.get_node_from_datavalue(
                    Self::str_to_datavalue(name.as_str().into()), None) 
            },

            Pattern::Literal { name } =>{
                self.storage.get_node_from_datavalue(
                    Self::literal_to_datavalue(name.as_str().into()),None)
            },
        }
    }

    ///tries to convert the input into i32 or f32;
    /// if it doesn't work -> returns DataValue::Null
    fn literal_to_datavalue(input: SmallString) -> DataValue{
        //input number example: "10"^^http://www.w3.org/2001/XMLSchema#integer
        //input string example: "this is a string"

        let mut input_string = input.to_string();
        input_string.remove(0); //removes first "
        let index = input_string.find('"');
        
        match index{
            Some(value) => {
                let (literal,rest) = input_string.split_at(value);

                if rest.contains("integer"){
                    let int = literal.parse::<i32>();
                        match int{
                            Ok(value) => return DataValue::NumberInt(value),
                            Err(value) => return DataValue::Null(),
                        }
                }

                if rest.contains("decimal") {
                    let float = literal.parse::<f32>();
                        match float{
                            Ok(value) => return DataValue::NumberFloat(value),
                            Err(value) => return DataValue::Null(),
                        }
                }

                if rest.contains("boolean") {
                    let boolean = literal.parse::<bool>();
                        match boolean{
                            Ok(value) => return DataValue::Boolean(value),
                            Err(value) => return DataValue::Null(),
                        }
                }

                if rest.eq("\""){ //string
                    let string: SmallString = literal.to_string().into();
                    return DataValue::String(string)
                }

                DataValue::Null()
            },
            None => DataValue::Null(),
        }
    }

    fn to_json(&self, rel: Box<dyn Relation<'a> + 'a>) -> String{
        let res = serde_json::to_string(&relation_to_json(&rel, &self.storage)).unwrap();
        res
    }

    fn relation_to_datavalue_vec(&'a self, 
        mut rel: Box<dyn Relation<'a> + 'a>,
        columns: Option<Vec<&str>>) -> Vec<DataValue> {
        self.storage.relation_to_datavalue_vec(rel, columns)
    }
    
    fn direct_pred_to_stmt_pred(&self, 
                                rel: Box<dyn Relation<'a> + 'a>,
                                column_names: Vec<&str>)
->  Box<dyn Relation<'a>  + 'a> {
    let row_count = rel.get_row_count();
    let col_count = rel.get_column_count();
    let column_indices :  Vec<usize> = rel.get_attributes().iter().
        enumerate().filter_map(
            |(i, x)| -> Option<usize>{
                if column_names.contains(&(x.as_str())) {
                    Some(i)
                } else {
                    None
                }
        }).collect();
        let mut new_nodes:  Vec<Node> = Vec::new();
    for row_index in 0..row_count {
        let row = rel.get_row(row_index);
        for col_index in 0..col_count{
            if column_indices.contains(&col_index){                
                if let DataValue::WikidataidP(_) = self.storage.retrieve_data(
                    row[col_index]) 
                {                                
                    new_nodes.push(wikidataidp_to_wikidataidpstmt(row[col_index]));
                } else {
                    new_nodes.push(row[col_index]);                            
                }                
            } else {
                new_nodes.push(row[col_index]);                                           
            }
        }
    }         
    Box::new(MaterializedRelation::create_mat_relation(
        rel.get_attributes().to_vec(), 
        new_nodes.as_slice()))
    }

}

pub struct Calculation<'a,S>
where S:StringStorage {
    /// Is the storage module used to store the parsed data
    pub storage: Storage<'a,S>,
}

impl<'a,S> Calculation<'a,S> 
where S:StringStorage {
    pub fn new(storage: Storage<'a,S>) -> Self {
        Self { storage }
    }
}


/// setup small test database by giving a prefix_str
/// the string consists of comma seperated DataValue items like
/// "wd:Q1, wdt:P2, wd:Q3, wds:4" 
/// of those prefixes just used and also p:P2 for WikidataidPstmt type
/// it must have a multiple of 4 items (Subject, Predicate, Object, NamedEdge)
pub fn setup_calc_engine(prefix_str: &str) -> Calculation<RamStorage>{
    Calculation::new(setup_test_storage(prefix_str))
}


/// translate a column of WikidataidP-Nodes into a column of WikidataidPstatement nodes
/// 

#[cfg(test)]
mod tests{
    use crate::data_types::DataValue;
    use crate::small_string::SmallString;
    use crate::storage_engine::builder::{StorageBuilder, StorageEngineBuilder};
    use super::{CalculationEngine, Calculation, setup_calc_engine};
    use crate::storage_engine::{prefix_str_to_datavalue_vec, StorageEngine};
    use crate::ext_string::RamStorage;
    use crate::calc_data_types::Pattern;
    use crate::relation::Relation;

    

    // also tests relation_to_datavalue_vec
    #[test]
    fn setup_calc_engine_creates_a_calculation_with_a_small_ram_storage(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let my_calc = setup_calc_engine(prefix_str);
        let datavalues : Vec<DataValue> = my_calc.relation_to_datavalue_vec(
            my_calc.storage.get_all_edges(),
            Some(vec!["Subject", "Predicate", "Object", "ID"]));
        let expected = prefix_str_to_datavalue_vec(prefix_str);
        assert_eq!(datavalues, expected);
    }


    #[test]
    fn convert_strings_to_datavalue(){
        let item =    SmallString::from("http://www.wikidata.org/entity/Q12");
        let parsed = <Calculation<RamStorage> as CalculationEngine>::str_to_datavalue(
            item);
        assert_eq!(parsed, DataValue::WikidataidQ(12));

        
        let direct_p = SmallString::from("http://www.wikidata.org/prop/direct/P13");
        let parsed = <Calculation<RamStorage> as CalculationEngine>::str_to_datavalue(
            direct_p);
        assert_eq!(parsed, DataValue::WikidataidP(13));
    
        let statement_p = SmallString::from("http://www.wikidata.org/prop/P14");
        let parsed = <Calculation<RamStorage> as CalculationEngine>::str_to_datavalue(
            statement_p);
        assert_eq!(parsed,  DataValue::WikidataidPstmt(14));
    
        let statement = SmallString::from("http://www.wikidata.org/entity/statement/15");
        let parsed = <Calculation<RamStorage> as CalculationEngine>::str_to_datavalue(
            statement);
        assert_eq!(parsed, DataValue::NamedEdge(SmallString::from("15")));        
    }

    #[test]
    fn triplepattern_match_for_an_object_by_WikidataidP_given_subject_pred(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p = Pattern::NamedNode{
            name: "http://www.wikidata.org/prop/direct/P2".to_string()};
        let o = Pattern::Variable{ name: "?an_object".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result,
                                                         Some(vec!["?an_object"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q3");
        assert_eq!(datavalues, expected);
    }

    #[test]
    fn triplepatterrn_match_for_a_statement_by_WikidataidPstmt_given_subject_pred(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::NamedNode{ name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p = Pattern::NamedNode{ name: "http://www.wikidata.org/prop/P2".to_string()};
        let o = Pattern::Variable{ name: "?a_statement".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result, 
                                                         Some(vec!["?a_statement"]));
        let expected = prefix_str_to_datavalue_vec("wds:4");
        assert_eq!(datavalues, expected);
    }

    #[test]
    fn triplepattern_match_for_a_subject_by_WikidataidP_given_object_pred(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/prop/direct/P2".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q3".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result,
                                                         Some(vec!["?a_subject"]));
       let expected = prefix_str_to_datavalue_vec("wd:Q1");
        assert_eq!(datavalues, expected);
    }


    #[test]
    fn triplepatterrn_match_for_a_subject_by_WikidataidPstmt_given_pred_object(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);
                let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::NamedNode{ name: "http://www.wikidata.org/prop/P2".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q3".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result, 
                                                         Some(vec!["?a_subject"]));
        let expected : Vec<DataValue> = vec![];
        assert_eq!(datavalues, expected);
    }
    
        #[test]
    fn triplepatterrn_match_for_a_subject_given_predStmt_and_stmt(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);
                let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::NamedNode{ name: "http://www.wikidata.org/prop/P2".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/statement/4".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(
            result, 
            Some(vec!["?a_subject",
                      "http://www.wikidata.org/prop/P2",
                      "http://www.wikidata.org/entity/statement/4"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, p:P2, wds:4");
        assert_eq!(datavalues, expected);
    }


    #[test]
    fn triplepatterrn_match_for_a_subject_given_predP_stmt(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);
                let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::NamedNode{ name: "http://www.wikidata.org/prop/direct/P2".
                                    to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/statement/4".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result, 
                                                         Some(vec!["?a_subject"]));
        let expected : Vec<DataValue> = vec![];
        assert_eq!(datavalues, expected);
    }


    #[test]
    fn triplepatterrn_match_for_subject_stmt_given_predStmt(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);
        let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::NamedNode{ name: "http://www.wikidata.org/prop/P2".to_string()};
        let o = Pattern::Variable{ name: "?a_statement".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result, 
                                   Some(vec!["?a_subject", "?a_statement"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, wds:4");
        assert_eq!(datavalues, expected);
    }

    #[test]
    fn triplepattern_match_for_subject_object_given_predP(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/prop/direct/P2".to_string()};
        let o = Pattern::Variable{ name: "?an_object".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result,
                                          Some(vec!["?a_subject", "?an_object"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, wd:Q3");
        assert_eq!(datavalues, expected);
    }
    
    #[test]
    fn triplepattern_match_for_subj_pred_given_object(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::Variable{ 
            name: "?a_subject".to_string()};
        let p = Pattern::Variable{ name: "?a_predicate".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q3".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(result,
                                         Some(vec!["?a_subject", "?a_predicate"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, wdt:P2");
        assert_eq!(datavalues, expected);
    }
    
    
    #[test]
    fn triplepattern_match_for_subj_pred_by_given_stmt(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::Variable{ name: "?a_subject".to_string()};
        let p = Pattern::Variable{ name: "?a_predicate".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/statement/4".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(
            result,
           Some(vec!["?a_subject", "?a_predicate",
                     "http://www.wikidata.org/entity/statement/4"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, p:P2, wds:4");
        assert_eq!(datavalues, expected);
    }

    #[test]
    fn triplepattern_match_for_pred_given_subject_object(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::NamedNode {
        name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p = Pattern::Variable{ name: "?a_predicate".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q3".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(
            result, Some(vec!["?a_predicate"]));
        let expected = prefix_str_to_datavalue_vec("wdt:P2");
        assert_eq!(datavalues, expected);
    }
    
    #[test]
    fn triplepattern_match_for_pred_by_given_subject_stmt(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::NamedNode{ 
                name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p = Pattern::Variable{ name: "?a_predicate".to_string()};
        let o = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/statement/4".to_string()};
        let result = calc.triplepattern_match(&s, &p, &o);
        let datavalues =  calc.relation_to_datavalue_vec(
            result,
           Some(vec!["http://www.wikidata.org/entity/Q1", 
                     "?a_predicate",
                     "http://www.wikidata.org/entity/statement/4"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, p:P2, wds:4");
        assert_eq!(datavalues, expected);
    }

    #[test]
    fn triplepattern_match_with_all_given(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        // correct combo predP
        let s1 = Pattern::NamedNode{ 
                name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p1 = Pattern::NamedNode { 
            name: "http://www.wikidata.org/prop/direct/P2".to_string()};
        let o1 = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q3".to_string()};
        let result1 = calc.triplepattern_match(&s1, &p1, &o1);
        let datavalues1 =  calc.relation_to_datavalue_vec(
            result1,
           Some(vec!["http://www.wikidata.org/entity/Q1", 
                     "http://www.wikidata.org/prop/direct/P2",
                     "http://www.wikidata.org/entity/Q3"]));
        let expected1 = prefix_str_to_datavalue_vec("wd:Q1, wdt:P2, wd:Q3");
        assert_eq!(datavalues1, expected1);

        // correct combo predPstmt
        let s2 = Pattern::NamedNode{ 
                name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p2 = Pattern::NamedNode { 
            name: "http://www.wikidata.org/prop/P2".to_string()};
        let o2 = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/statement/4".to_string()};
        let result2 = calc.triplepattern_match(&s2, &p2, &o2);
        let datavalues2 =  calc.relation_to_datavalue_vec(
            result2,
           Some(vec!["http://www.wikidata.org/entity/Q1", 
                     "http://www.wikidata.org/prop/P2",
                     "http://www.wikidata.org/entity/statement/4"]));
        let expected2 = prefix_str_to_datavalue_vec("wd:Q1, p:P2, wds:4");
        assert_eq!(datavalues2, expected2);

        // incorrect combo predP
        let s3 = Pattern::NamedNode{ 
                name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p3 = Pattern::NamedNode { 
            name: "http://www.wikidata.org/prop/direct/P2".to_string()};
        let o3 = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/Q18".to_string()};
        let result3 = calc.triplepattern_match(&s3, &p3, &o3);
        let datavalues3 =  calc.relation_to_datavalue_vec(result3, None);
        assert_eq!(datavalues3, vec![]);

        //  incorrect combo predPstmt
        let s4 = Pattern::NamedNode{ 
                name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p4 = Pattern::NamedNode { 
            name: "http://www.wikidata.org/prop/P3".to_string()};
        let o4 = Pattern::NamedNode{ 
            name: "http://www.wikidata.org/entity/statement/4".to_string()};
        let result4 = calc.triplepattern_match(&s4, &p4, &o4);
        let datavalues4 =  calc.relation_to_datavalue_vec(result4, None);
        assert_eq!(datavalues4, vec![]);
        
    }

    #[test]
    fn triplepattern_match_given_subject(){
        let prefix_str = "wd:Q1, wdt:P2, wd:Q3, wds:4";
        let calc = setup_calc_engine(prefix_str);        
        let s = Pattern::NamedNode {
            name: "http://www.wikidata.org/entity/Q1".to_string()};
        let p = Pattern::Variable{ name: "?a_predicate".to_string()};
        let o = Pattern::Variable{ name: "?an_object".to_string()}; 
        let result = calc.triplepattern_match(&s, &p, &o);
        println!("{:?}", result.get_attributes());
        let datavalues =  calc.relation_to_datavalue_vec(
            result,
           Some(vec!["http://www.wikidata.org/entity/Q1", 
                     "?a_predicate",
                     "?an_object"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, wdt:P2, wd:Q3,
                                                     wd:Q1, p:P2, wds:4");        
        assert_eq!(datavalues, expected);
    }


    #[test]
    fn convert_a_column_of_wikidataidp_nodes_into_equivalent_wikidataidpstmt_nodes(){
        let prefix_str = " wd:Q1,  wdt:P2,  wd:Q3,  wds:4, 
                          wd:Q11, wdt:P12, wd:Q13, wds:14";
        let calc = setup_calc_engine(prefix_str);
        let rel : Box<dyn Relation> = Box::new(calc.storage.get_all_edges().
                                               to_materialized());
        let operation_result = calc.direct_pred_to_stmt_pred(rel, vec!["Predicate"]); 
        assert_eq!(operation_result.get_row_count(), 2);
        assert_eq!(operation_result.get_column_count(), 4);
        let datavalues = calc.relation_to_datavalue_vec(
            operation_result,
            Some(vec!["Subject", "Predicate"]));
        let expected = prefix_str_to_datavalue_vec("wd:Q1, p:P2, wd:Q11, p:P12");
        assert_eq!(datavalues, expected);                                                
    }
}
    



