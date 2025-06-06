use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
extern crate graphdb;
use graphdb::relation::{Relation, SortingOrder};
use graphdb::relation::materialized_relation::{create_mat_relation, MaterializedRelation};
use graphdb::storage_engine::StorageBuilder;
use graphdb::parser::{parse_file};

fn benchmark_mergesort_on_file(c: &mut Criterion, id : &str, filepath : &str) {
    let mut seb = StorageBuilder::new();
    parse_file(filepath, &mut seb, false, None);
    let col = vec!["Subject".to_string(), "Predicate".to_string(), "Object".to_string(), "ID".to_string()];
    c.bench_function(
        id, 
        |b| 
        b.iter_batched(
            || create_mat_relation(col.clone(), &seb.edges),
            |mut rel: MaterializedRelation| rel.merge_sort(0, SortingOrder::Ascending),
            BatchSize::LargeInput
        )
    );
}

fn benchmark_mergesort(c: &mut Criterion) {
    benchmark_mergesort_on_file(c, "mergesort_5", "./tests/data/first_5_lines.txt");
    //benchmark_mergesort_on_file(c, "mergesort_100", "./tests/data/first_100_lines.txt");
    //benchmark_mergesort_on_file(c, "mergesort_1000", "./tests/data/first_1000_lines.txt");
}

fn benchmark_join_on_file(c: &mut Criterion, id : &str, filepath : &str) {
    let mut seb = StorageBuilder::new();
    parse_file(filepath, &mut seb, false, None);
    let col = vec!["Subject".to_string(), "Predicate".to_string(), "Object".to_string(), "ID".to_string()];
    let col2 = vec!["Subject".to_string(), "Predicate2".to_string(), "Object2".to_string(), "ID2".to_string()];
    c.bench_function(
        id, 
        |b| 
        b.iter_batched(
            || 
            (
                Box::new(create_mat_relation(col.clone(), &seb.edges)), 
                Box::new(create_mat_relation(col2.clone(), &seb.edges))
            ),
            |rel: (Box<MaterializedRelation>, Box<MaterializedRelation>)| 
            rel.0.join(rel.1),
            BatchSize::LargeInput
        )
    );
}

fn benchmark_join(c : &mut Criterion) {
    benchmark_join_on_file(c, "join_5", "./tests/data/first_5_lines.txt");
    //benchmark_join_on_file(c, "join_100", "./tests/data/first_100_lines.txt");
}

criterion_group!(benches, benchmark_mergesort, benchmark_join);
criterion_main!(benches);