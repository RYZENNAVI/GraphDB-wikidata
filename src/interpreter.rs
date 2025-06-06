use std::collections::{HashMap, HashSet};
use spargebra::{
    algebra::{Function, GraphPattern},
    term::TriplePattern,
    Query,
};
// use std::io::stdin;
// use urlencoding::decode;
use crate::calc_data_types;
use crate::calc_data_types::{Expression, Operator, Pattern, AE, PPE, SO};
pub struct InterpreterStruct {}

/// TEST FUNCTION
/// Test the process of reading in HTML-Links of a query to then creating a query plan for executing the query
// #[test]
// #[ignore]
// fn test_interpreter(){
//     let mut line = "TestAbfrage".to_string();
//     stdin().read_line(&mut line).unwrap();
//     let abfrage = line.split("=");
//     let mut counter = 0;
//     let mut split = "Test";
//     for i in abfrage {
//         if counter == 1 {
//         split = i;
//         }
//         counter += 1;
//     }
//     println!("This is your Wikidata Query, {}", line);
//     let decoded = decode(&split).expect("UTF-8");
//     println!("{}", decoded);
//     let query = InterpreterStruct::str_to_query(&decoded);
//     match query {
//         Ok(query) => println!("This is your Wikidata Query, \n{:#?}", InterpreterStruct::query_to_pattern(&query)),
//         Err(_) => println!("\nWRONG 1\n"),
//     }
// }

// unused Prefixes
/*
        PREFIX bd:<http://www.bigdata.com/rdf#>
        PREFIX cc: <http://creativecommons.org/ns#>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#
        PREFIX schema: <http://schema.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>


*/

impl InterpreterStruct {
    ///Parses the given &str for all known indices and replaces them.
    fn prepend_prefixes(input: &str) -> String {
        "
        PREFIX bd: <http://www.bigdata.com/rdf#>
        PREFIX cc: <http://creativecommons.org/ns#>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX schema: <http://schema.org/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX p: <http://www.wikidata.org/prop/>
        PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
        PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
        PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
        PREFIX pr: <http://www.wikidata.org/prop/reference/>
        PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>
        PREFIX prv: <http://www.wikidata.org/prop/reference/value/>
        PREFIX ps: <http://www.wikidata.org/prop/statement/>
        PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
        PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
        PREFIX wd: <http://www.wikidata.org/entity/>
        Prefix wdata: <http://www.wikidata.org/wiki/Special:EntityData/>
        PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
        PREFIX wdref: <http://www.wikidata.org/reference/>
        PREFIX wds: <http://www.wikidata.org/entity/statement/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>
        PREFIX wdv: <http://www.wikidata.org/value/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 
        "
        .to_string()
            + input
    }

    ///Parses the ready &str to type Query
    pub fn str_to_query(input: &str) -> Result<Query, &str> {
        let query_str = Self::prepend_prefixes(input);
        if let Ok(query) = Query::parse(&query_str, None) {
            Ok(query)
        } else {
            Err("Input is not a correct SPARQL Query!")
        }
    }

    /// Get the Type of query and execute rec_pattern with the given pattern (-> GraphPattern)
    pub fn query_to_pattern(query: &Query) -> Operator {
        match query {
            Query::Select {
                dataset: _,
                pattern,
                base_iri: _,
            } => rewrite_label_service(Self::rec_pattern(pattern), &HashSet::new()),
            Query::Construct {
                template: _,
                dataset: _,
                pattern: _,
                base_iri: _,
            } => todo!(),
            Query::Describe {
                dataset: _,
                pattern: _,
                base_iri: _,
            } => todo!(),
            Query::Ask {
                dataset: _,
                pattern: _,
                base_iri: _,
            } => todo!(),
        }
    }

    /// Recursively go through the GraphPattern to create an Operator(-Tree) bottom-up
    /// Bgp is our leaf, due to the pattern thats left is a vec of triple-pattern(s)
    fn rec_pattern(pattern: &GraphPattern) -> Operator {
        match pattern {
            GraphPattern::Bgp { patterns } => Operator::Bgp {
                inner: (Self::triple_pattern_to_own_tp(patterns)),
            },
            GraphPattern::Path {
                subject,
                path,
                object,
            } => Operator::Path {
                subject: Box::new(Self::termpattern_to_pattern(subject)),
                path: Box::new(Self::property_path_expression_to_ppe(path)),
                object: Box::new(Self::termpattern_to_pattern(object)),
            },
            GraphPattern::Join { left, right } => Operator::MergeJoin {
                left_child: Box::new(Self::rec_pattern(left)),
                right_child: Box::new(Self::rec_pattern(right)),
            },
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => match expression {
                Some(expr) => Operator::LeftJoin {
                    left_child: (Box::new(Self::rec_pattern(left))),
                    right_child: (Box::new(Self::rec_pattern(right))),
                    expression: Some(Self::expression_to_expression(expr)),
                },
                None => Operator::LeftJoin {
                    left_child: (Box::new(Self::rec_pattern(left))),
                    right_child: (Box::new(Self::rec_pattern(right))),
                    expression: None,
                },
            },
            GraphPattern::Filter { expr, inner } => Operator::Filter {
                expression: (Self::expression_to_expression(expr)),
                inner: Box::new(Self::rec_pattern(inner)),
            },
            GraphPattern::Union { left, right } => Operator::Union {
                left: Box::new(Self::rec_pattern(left)),
                right: Box::new(Self::rec_pattern(right)),
            },
            GraphPattern::Graph { name, inner } => Operator::Graph {
                name: (Self::named_node_pattern_to_pattern(name)),
                inner: Box::new(Self::rec_pattern(inner)),
            },

            GraphPattern::Extend {
                inner,
                variable,
                expression,
            } => Operator::Extend {
                inner: Box::new(Self::rec_pattern(inner)),
                variable: (Pattern::Variable {
                    name: (variable.to_string()),
                }),
                expression: (Self::expression_to_expression(expression)),
            },
            GraphPattern::Minus { left, right } => Operator::Minus {
                left: Box::new(Self::rec_pattern(left)),
                right: Box::new(Self::rec_pattern(right)),
            },
            GraphPattern::Values {
                variables: _,
                bindings: _,
            } => {
                todo!()
            }
            GraphPattern::OrderBy { inner, expression } => Operator::Order {
                inner: (Box::new(Self::rec_pattern(inner))),
                order_attribute: expression
                    .iter()
                    .map(Self::order_expression_to_so)
                    .collect(),
            },
            GraphPattern::Project { inner, variables } => Operator::Projection {
                inner: (Box::new(Self::rec_pattern(inner))),
                attributes: variables.iter().map(Self::variable_mapper).collect(),
            },
            GraphPattern::Distinct { inner } => Operator::Distinct {
                inner: (Box::new(Self::rec_pattern(inner))),
            },
            GraphPattern::Reduced { inner } => Operator::Reduced {
                inner: Box::new(Self::rec_pattern(inner)),
            },
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => Operator::Slice {
                inner: Box::new(Self::rec_pattern(inner)),
                start: *start,
                length: *length,
            },
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => Operator::Group {
                inner: (Box::new(Self::rec_pattern(inner))),
                variables: (variables.iter().map(Self::variable_mapper).collect()),
                aggregates: (aggregates.iter().map(Self::aggregate_mapper)).collect(),
            },
            GraphPattern::Service {
                name,
                inner,
                silent,
            } => Operator::Service {
                name: Self::named_node_pattern_to_pattern(name),
                inner: Box::new(Self::rec_pattern(inner)),
                silent: *silent,
            },
        }
    }

    /// Transforms spargebra::GraphPattern::TriplePattern to Enum Operator::TriplePattern
    fn triple_pattern_to_own_tp(tp: &[TriplePattern]) -> Vec<Operator> {
        let mut vec = Vec::new();
        for i in tp.iter() {
            let new_tp = Operator::TriplePattern {
                s: (match &i.subject {
                    spargebra::term::TermPattern::NamedNode(named_node) => {
                        Box::new(Pattern::NamedNode {
                            name: named_node.as_str().into(),
                        })
                    }

                    spargebra::term::TermPattern::BlankNode(_) => Box::new(Pattern::BlankNode {
                        name: (i.subject.to_string()),
                    }),

                    spargebra::term::TermPattern::Literal(_) => Box::new(Pattern::Literal {
                        name: (i.subject.to_string()),
                    }),

                    spargebra::term::TermPattern::Variable(_) => Box::new(Pattern::Variable {
                        name: (i.subject.to_string()),
                    }),
                }),
                p: (match &i.predicate {
                    spargebra::term::NamedNodePattern::NamedNode(named_node) => {
                        Box::new(Pattern::NamedNode {
                            name: named_node.as_str().into(),
                        })
                    }

                    spargebra::term::NamedNodePattern::Variable(_) => Box::new(Pattern::Variable {
                        name: (i.predicate.to_string()),
                    }),
                }),

                o: (match &i.object {
                    spargebra::term::TermPattern::NamedNode(named_node) => {
                        Box::new(Pattern::NamedNode {
                            name: named_node.as_str().into(),
                        })
                    }

                    spargebra::term::TermPattern::BlankNode(_) => Box::new(Pattern::BlankNode {
                        name: (i.object.to_string()),
                    }),

                    spargebra::term::TermPattern::Literal(_) => Box::new(Pattern::Literal {
                        name: (i.object.to_string()),
                    }),

                    spargebra::term::TermPattern::Variable(_) => Box::new(Pattern::Variable {
                        name: (i.object.to_string()),
                    }),
                }),
            };
            vec.push(new_tp);
        }
        vec
    }

    /// Transforms spargebra->termpattern to Enum "Pattern"
    fn termpattern_to_pattern(termpattern: &spargebra::term::TermPattern) -> Pattern {
        match termpattern {
            spargebra::term::TermPattern::NamedNode(named_node) => Pattern::NamedNode {
                name: named_node.to_string(),
            },

            spargebra::term::TermPattern::BlankNode(blank_node) => Pattern::BlankNode {
                name: (blank_node.to_string()),
            },

            spargebra::term::TermPattern::Literal(literal) => Pattern::Literal {
                name: (literal.to_string()),
            },

            spargebra::term::TermPattern::Variable(variable) => Pattern::Variable {
                name: (variable.to_string()),
            },
        }
    }

    /// Transforms spargebra::NamedNodePattern to Enum "Pattern"
    fn named_node_pattern_to_pattern(pattern: &spargebra::term::NamedNodePattern) -> Pattern {
        match pattern {
            spargebra::term::NamedNodePattern::NamedNode(named_node) => Pattern::NamedNode {
                name: named_node.as_str().into(),
            },

            spargebra::term::NamedNodePattern::Variable(variable) => Pattern::Variable {
                name: (variable.to_string()),
            },
        }
    }

    /// Transforms spargebra::PropertyPathExpression to Enum "PPE"
    fn property_path_expression_to_ppe(ppe: &spargebra::algebra::PropertyPathExpression) -> PPE {
        match ppe {
            spargebra::algebra::PropertyPathExpression::NamedNode(pattern) => {
                PPE::NamedNode(Pattern::NamedNode {
                    name: pattern.to_string(),
                })
            }

            spargebra::algebra::PropertyPathExpression::Reverse(reverse) => PPE::Reverse {
                reverse: Box::new(Self::property_path_expression_to_ppe(reverse)),
            },

            spargebra::algebra::PropertyPathExpression::Sequence(one, two) => PPE::Sequence {
                front: (Box::new(Self::property_path_expression_to_ppe(one))),
                back: (Box::new(Self::property_path_expression_to_ppe(two))),
            },

            spargebra::algebra::PropertyPathExpression::Alternative(one, two) => PPE::Alternative {
                front: (Box::new(Self::property_path_expression_to_ppe(one))),
                back: (Box::new(Self::property_path_expression_to_ppe(two))),
            },

            spargebra::algebra::PropertyPathExpression::ZeroOrMore(zom) => PPE::ZeroOrMore {
                zom: Box::new(Self::property_path_expression_to_ppe(zom)),
            },

            spargebra::algebra::PropertyPathExpression::OneOrMore(oom) => PPE::OneOrMore {
                oom: Box::new(Self::property_path_expression_to_ppe(oom)),
            },

            spargebra::algebra::PropertyPathExpression::ZeroOrOne(zoo) => PPE::ZeroOrOne {
                zoo: Box::new(Self::property_path_expression_to_ppe(zoo)),
            },

            spargebra::algebra::PropertyPathExpression::NegatedPropertySet(set) => {
                PPE::NegatedPropertySet {
                    set: (set.iter().map(Self::named_node_mapper).collect()),
                }
            }
        }
    }

    /// Transforms spargebra->expression to Enum "Expression" to work with it
    fn expression_to_expression(expression: &spargebra::algebra::Expression) -> Expression {
        match expression {
            spargebra::algebra::Expression::NamedNode(node) => {
                Expression::NamedNode(Pattern::NamedNode {
                    name: node.to_string(),
                })
            }

            spargebra::algebra::Expression::Literal(literal) => {
                Expression::Literal(Pattern::Literal {
                    name: (literal.to_string()),
                })
            }

            spargebra::algebra::Expression::Variable(variable) => {
                Expression::Variable(Pattern::Variable {
                    name: (variable.to_string()),
                })
            }

            spargebra::algebra::Expression::Or(one, two) => Expression::Or(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::And(one, two) => Expression::And(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::Equal(one, two) => Expression::Equal(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::SameTerm(one, two) => Expression::SameTerm(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::Greater(one, two) => Expression::Greater(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::GreaterOrEqual(one, two) => Expression::GreaterOrEqual(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::Less(one, two) => Expression::Less(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::LessOrEqual(one, two) => Expression::LessOrEqual(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::In(one, two) => Expression::In(
                Box::new(Self::expression_to_expression(one)),
                two.iter().map(Self::expression_to_expression).collect(),
            ),

            spargebra::algebra::Expression::Add(one, two) => Expression::Add(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::Subtract(one, two) => Expression::Subtract(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::Multiply(one, two) => Expression::Multiply(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::Divide(one, two) => Expression::Divide(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
            ),

            spargebra::algebra::Expression::UnaryPlus(expr) => {
                Expression::UnaryPlus(Box::new(Self::expression_to_expression(expr)))
            }

            spargebra::algebra::Expression::UnaryMinus(expr) => {
                Expression::UnaryMinus(Box::new(Self::expression_to_expression(expr)))
            }

            spargebra::algebra::Expression::Not(expr) => {
                Expression::Not(Box::new(Self::expression_to_expression(expr)))
            }

            spargebra::algebra::Expression::Exists(operator) => {
                Expression::Exists(Box::new(Self::rec_pattern(operator)))
            }

            spargebra::algebra::Expression::Bound(pattern) => {
                Expression::Bound(Pattern::Variable {
                    name: (pattern.to_string()),
                })
            }

            spargebra::algebra::Expression::If(one, two, three) => Expression::If(
                Box::new(Self::expression_to_expression(one)),
                Box::new(Self::expression_to_expression(two)),
                Box::new(Self::expression_to_expression(three)),
            ),

            spargebra::algebra::Expression::Coalesce(expr) => {
                Expression::Coalesce(expr.iter().map(Self::expression_to_expression).collect())
            }

            spargebra::algebra::Expression::FunctionCall(func, expr) => {
                // original implement: convert function to string
                // func.to_string(),
                // expr.iter().map(Self::expression_to_expression).collect())

                // new implement: use function
                let expr_convert: Vec<Expression> =
                    expr.iter().map(Self::expression_to_expression).collect();
                match func {
                    Function::Str => {
                        Expression::FunctionCall(calc_data_types::Function::Str, expr_convert)
                    }
                    Function::Lang => {
                        Expression::FunctionCall(calc_data_types::Function::Lang, expr_convert)
                    }
                    Function::LangMatches => Expression::FunctionCall(
                        calc_data_types::Function::LangMatches,
                        expr_convert,
                    ),
                    Function::Datatype => {
                        Expression::FunctionCall(calc_data_types::Function::Datatype, expr_convert)
                    }
                    Function::Iri => {
                        Expression::FunctionCall(calc_data_types::Function::Iri, expr_convert)
                    }
                    Function::BNode => {
                        Expression::FunctionCall(calc_data_types::Function::BNode, expr_convert)
                    }
                    Function::Rand => {
                        Expression::FunctionCall(calc_data_types::Function::Rand, expr_convert)
                    }
                    Function::Abs => {
                        Expression::FunctionCall(calc_data_types::Function::Abs, expr_convert)
                    }
                    Function::Ceil => {
                        Expression::FunctionCall(calc_data_types::Function::Ceil, expr_convert)
                    }
                    Function::Floor => {
                        Expression::FunctionCall(calc_data_types::Function::Floor, expr_convert)
                    }
                    Function::Round => {
                        Expression::FunctionCall(calc_data_types::Function::Round, expr_convert)
                    }
                    Function::Concat => {
                        Expression::FunctionCall(calc_data_types::Function::Concat, expr_convert)
                    }
                    Function::SubStr => {
                        Expression::FunctionCall(calc_data_types::Function::SubStr, expr_convert)
                    }
                    Function::StrLen => {
                        Expression::FunctionCall(calc_data_types::Function::StrLen, expr_convert)
                    }
                    Function::Replace => {
                        Expression::FunctionCall(calc_data_types::Function::Replace, expr_convert)
                    }
                    Function::UCase => {
                        Expression::FunctionCall(calc_data_types::Function::UCase, expr_convert)
                    }
                    Function::LCase => {
                        Expression::FunctionCall(calc_data_types::Function::LCase, expr_convert)
                    }
                    Function::EncodeForUri => Expression::FunctionCall(
                        calc_data_types::Function::EncodeForUri,
                        expr_convert,
                    ),
                    Function::Contains => {
                        Expression::FunctionCall(calc_data_types::Function::Contains, expr_convert)
                    }
                    Function::StrStarts => {
                        Expression::FunctionCall(calc_data_types::Function::StrStarts, expr_convert)
                    }
                    Function::StrEnds => {
                        Expression::FunctionCall(calc_data_types::Function::StrEnds, expr_convert)
                    }
                    Function::StrBefore => {
                        Expression::FunctionCall(calc_data_types::Function::StrBefore, expr_convert)
                    }
                    Function::StrAfter => {
                        Expression::FunctionCall(calc_data_types::Function::StrAfter, expr_convert)
                    }
                    Function::Year => {
                        Expression::FunctionCall(calc_data_types::Function::Year, expr_convert)
                    }
                    Function::Month => {
                        Expression::FunctionCall(calc_data_types::Function::Month, expr_convert)
                    }
                    Function::Day => {
                        Expression::FunctionCall(calc_data_types::Function::Day, expr_convert)
                    }
                    Function::Hours => {
                        Expression::FunctionCall(calc_data_types::Function::Hours, expr_convert)
                    }
                    Function::Minutes => {
                        Expression::FunctionCall(calc_data_types::Function::Minutes, expr_convert)
                    }
                    Function::Seconds => {
                        Expression::FunctionCall(calc_data_types::Function::Seconds, expr_convert)
                    }
                    Function::Timezone => {
                        Expression::FunctionCall(calc_data_types::Function::Timezone, expr_convert)
                    }
                    Function::Tz => {
                        Expression::FunctionCall(calc_data_types::Function::Tz, expr_convert)
                    }
                    Function::Now => {
                        Expression::FunctionCall(calc_data_types::Function::Now, expr_convert)
                    }
                    Function::Uuid => {
                        Expression::FunctionCall(calc_data_types::Function::Uuid, expr_convert)
                    }
                    Function::StrUuid => {
                        Expression::FunctionCall(calc_data_types::Function::StrUuid, expr_convert)
                    }
                    Function::Md5 => {
                        Expression::FunctionCall(calc_data_types::Function::Md5, expr_convert)
                    }
                    Function::Sha1 => {
                        Expression::FunctionCall(calc_data_types::Function::Sha1, expr_convert)
                    }
                    Function::Sha256 => {
                        Expression::FunctionCall(calc_data_types::Function::Sha256, expr_convert)
                    }
                    Function::Sha384 => {
                        Expression::FunctionCall(calc_data_types::Function::Sha384, expr_convert)
                    }
                    Function::Sha512 => {
                        Expression::FunctionCall(calc_data_types::Function::Sha512, expr_convert)
                    }
                    Function::StrLang => {
                        Expression::FunctionCall(calc_data_types::Function::StrLang, expr_convert)
                    }
                    Function::StrDt => {
                        Expression::FunctionCall(calc_data_types::Function::StrDt, expr_convert)
                    }
                    Function::IsIri => {
                        Expression::FunctionCall(calc_data_types::Function::IsIri, expr_convert)
                    }
                    Function::IsBlank => {
                        Expression::FunctionCall(calc_data_types::Function::IsBlank, expr_convert)
                    }
                    Function::IsLiteral => {
                        Expression::FunctionCall(calc_data_types::Function::IsLiteral, expr_convert)
                    }
                    Function::IsNumeric => {
                        Expression::FunctionCall(calc_data_types::Function::IsNumeric, expr_convert)
                    }
                    Function::Regex => {
                        Expression::FunctionCall(calc_data_types::Function::Regex, expr_convert)
                    }
                    Function::Custom(iri) if iri.as_str() == "http://wikiba.se/ontology#label" => 
                    {
                        Expression::FunctionCall(calc_data_types::Function::getLabel, expr_convert)
                    }
                    Function::Custom(iri) => panic!("unsupported function {}", iri.as_str()),
                }
            }
        }
    }

    /// Transforms spargebra->graphterm to Enum "GT" to work with it
    // fn graphterm_to_gt(groundterm: &spargebra::term::GroundTerm) -> GT{
    //     match groundterm{
    //         spargebra::term::GroundTerm::NamedNode(node) => return GT::NamedNode(Pattern::NamedNode { name: (InterpreterStruct::replace_string(node.to_string())) }),
    //         spargebra::term::GroundTerm::Literal(literal) => return GT::Literal(Pattern::Literal { name: (literal.to_string()) }),
    //     }
    // }

    /// Transforms spargebra->order_expression to Enum "SO" to work with it
    fn order_expression_to_so(sorting: &spargebra::algebra::OrderExpression) -> SO {
        match sorting {
            spargebra::algebra::OrderExpression::Asc(expr) => {
                SO::ASC(Self::expression_to_expression(expr))
            }
            spargebra::algebra::OrderExpression::Desc(expr) => {
                SO::DESC(Self::expression_to_expression(expr))
            }
        }
    }
    /// Transforms spargebra->aggregate_expression to Enum "AE" to work with it
    fn aggregate_expression_to_ae(expr: &spargebra::algebra::AggregateExpression) -> AE {
        match expr {
            spargebra::algebra::AggregateExpression::Count { expr, distinct } => match expr {
                Some(expre) => AE::Count {
                    expr: Some(Box::new(Self::expression_to_expression(expre))),
                    distinct: *(distinct),
                },

                None => AE::Count {
                    expr: (None),
                    distinct: *(distinct),
                },
            },

            spargebra::algebra::AggregateExpression::Sum { expr, distinct } => AE::Sum {
                expr: (Box::new(Self::expression_to_expression(expr))),
                distinct: *(distinct),
            },

            spargebra::algebra::AggregateExpression::Avg { expr, distinct } => AE::Avg {
                expr: (Box::new(Self::expression_to_expression(expr))),
                distinct: *(distinct),
            },

            spargebra::algebra::AggregateExpression::Min { expr, distinct } => AE::Min {
                expr: (Box::new(Self::expression_to_expression(expr))),
                distinct: *(distinct),
            },

            spargebra::algebra::AggregateExpression::Max { expr, distinct } => AE::Max {
                expr: (Box::new(Self::expression_to_expression(expr))),
                distinct: *(distinct),
            },

            spargebra::algebra::AggregateExpression::GroupConcat {
                expr,
                distinct,
                separator,
            } => match separator {
                Some(sep) => AE::GroupConcat {
                    expr: (Box::new(Self::expression_to_expression(expr))),
                    distinct: *(distinct),
                    separator: Some(sep.to_string()),
                },

                None => AE::GroupConcat {
                    expr: (Box::new(Self::expression_to_expression(expr))),
                    distinct: *(distinct),
                    separator: None,
                },
            },

            spargebra::algebra::AggregateExpression::Sample { expr, distinct } => AE::Sample {
                expr: (Box::new(Self::expression_to_expression(expr))),
                distinct: *(distinct),
            },

            spargebra::algebra::AggregateExpression::Custom {
                name,
                expr,
                distinct,
            } => AE::Custom {
                name: (Pattern::NamedNode {
                    name: (name.to_string()),
                }),
                expr: (Box::new(Self::expression_to_expression(expr))),
                distinct: *(distinct),
            },
        }
    }

    /// Maps a given spargebra::term::NamedNode to our Enum "Pattern"
    fn named_node_mapper(term: &spargebra::term::NamedNode) -> Pattern {
        Pattern::NamedNode {
            name: term.to_string(),
        }
    }
    /// Maps a given spargebra::term::Variable to our Enum "Pattern"
    fn variable_mapper(term: &spargebra::term::Variable) -> Pattern {
        Pattern::Variable {
            name: (term.to_string()),
        }
    }
    /// Maps a given tuple (Variable, AggregateExpression) to a tuple (Pattern, AE)       
    fn aggregate_mapper(
        (term, aggregate): &(
            spargebra::term::Variable,
            spargebra::algebra::AggregateExpression,
        ),
    ) -> (Pattern, AE) {
        (
            Pattern::Variable {
                name: term.to_string(),
            },
            Self::aggregate_expression_to_ae(aggregate),
        )
    }
}

fn rewrite_label_service(query: Operator, variables: &HashSet<&str>) -> Operator {
    match query {
        Operator::MergeJoin {
            left_child,
            right_child,
        } => match (*left_child, *right_child) {
            (
                Operator::Service {
                    name: Pattern::NamedNode { name },
                    inner,
                    silent: _,
                },
                other_child,
            )
            | (
                other_child,
                Operator::Service {
                    name: Pattern::NamedNode { name },
                    inner,
                    silent: _,
                },
            ) if name == "http://wikiba.se/ontology#label" => Operator::ProjectExtend {
                inner: Box::new(other_child),
                expressions: label_service_to_function_calls(*inner, variables),
            },
            (lc, rc) => Operator::MergeJoin {
                left_child: Box::new(rewrite_label_service(lc, variables)),
                right_child: Box::new(rewrite_label_service(rc, variables)),
            },
        },

        // TODO: implement service call that is not wrapped in a MergeJoin

        Operator::Bgp { .. }
        | Operator::Path { .. }
        | Operator::Filter { .. }
        | Operator::TriplePattern { .. }
        | Operator::Values { .. }
        | Operator::Union { .. }
        | Operator::Graph { .. }
        | Operator::Minus { .. }
        | Operator::Service { .. } => query,
        Operator::LeftJoin {
            left_child,
            right_child,
            expression,
        } => Operator::LeftJoin {
            left_child: Box::new(rewrite_label_service(*left_child, variables)),
            right_child: Box::new(rewrite_label_service(*right_child, variables)),
            expression,
        },
        Operator::LateralJoin {
            left_child,
            right_child,
        } => Operator::LateralJoin {
            left_child: Box::new(rewrite_label_service(*left_child, variables)),
            right_child: Box::new(rewrite_label_service(*right_child, variables)),
        },
        Operator::Extend {
            inner,
            variable,
            expression,
        } => {
            let mut variables = variables.clone();
            if let Pattern::Variable { name } = &variable {
                variables.remove(name.as_str());
            }
            Operator::Extend {
                inner: Box::new(rewrite_label_service(*inner, &variables )),
                variable,
                expression,
            }
        },
        Operator::Order {
            inner,
            order_attribute,
        } => Operator::Order {
            inner: Box::new(rewrite_label_service(*inner, variables)),
            order_attribute,
        },
        Operator::Distinct { inner } => Operator::Distinct {
            inner: Box::new(rewrite_label_service(*inner, variables)),
        },
        Operator::Reduced { inner } => Operator::Reduced {
            inner: Box::new(rewrite_label_service(*inner, variables)),
        },
        Operator::Slice {
            inner,
            start,
            length,
        } => Operator::Slice {
            inner: Box::new(rewrite_label_service(*inner, variables)),
            start,
            length,
        },
        Operator::Group {
            inner,
            variables: vars,
            aggregates,
        } => Operator::Group {
            inner: Box::new(rewrite_label_service(*inner, variables)),
            variables: vars,
            aggregates,
        },
        Operator::ProjectExtend { inner, expressions } => {
            let variables: HashSet<_> = expressions
                .iter()
                .filter_map(|v| match v {
                    (Pattern::Variable { name }, _) => Some(name.as_str()),
                    _ => None,
                })
                .collect();

            Operator::ProjectExtend {
                inner: Box::new(rewrite_label_service(*inner, &variables)),
                expressions,
            }
        }
        Operator::Projection { inner, attributes } => {
            let variables: HashSet<_> = attributes
                .iter()
                .filter_map(|v| match v {
                    Pattern::Variable { name } => Some(name.as_str()),
                    _ => None,
                })
                .collect();

            Operator::Projection {
                inner: Box::new(rewrite_label_service(*inner, &variables)),
                attributes,
            }
        }
    }
}

fn label_service_to_function_calls(
    graph_pattern: Operator,
    variables: &HashSet<&str>,
) -> Vec<(Pattern, Expression)> {
    let Operator::Bgp { inner: triples } = graph_pattern else { return Vec::new(); };
    let mut languages = Vec::new();
    let mut mappings = HashMap::new();

    for (s, p, o) in triples.into_iter().filter_map(|op| match op {
        Operator::TriplePattern { s, p, o } => Some((*s, *p, *o)),
        _ => None,
    }) {
        match (s, p, o) { // get the language(s) from service, it works for explicit and implizit mapping 
            
            (
                Pattern::NamedNode { name: subject },
                Pattern::NamedNode { name: predicate },
                Pattern::Literal {
                    name: language_string,
                },
            ) if (subject == "http://www.bigdata.com/rdf#serviceParam")
                & (predicate == "http://wikiba.se/ontology#language") =>
            {
                languages = language_string[1..language_string.len()-1]
                    .split(',')
                    .map(|lang| {
                        Expression::Literal(Pattern::Literal {
                            name: format!("\"{lang}\""),
                        })
                    })
                    .collect();
            }
            // explicit mapping (if exists)
            (   subject ,
                Pattern::NamedNode { name: predicate },
                Pattern::Variable { name: object }
            ) =>
            {
                let expression = match subject {
                    Pattern::Variable { .. } => Expression::Variable(subject.clone()),
                    Pattern::NamedNode { .. } => Expression::NamedNode(subject.clone()),
                    Pattern::BlankNode { .. } => panic!("Blank node not allowed as subject in label service."),
                    Pattern::Literal { .. } => panic!("Literal not allowed as subject."),
                };
                // create the mapping for explicit mapping

                //match the predicate (IRI) to choice the function
                // details: https://www.mediawiki.org/wiki/Wikidata_Query_Service/User_Manual#Label_service
                match predicate.as_str()
                {
                    "http://www.w3.org/2000/01/rdf-schema#label" =>
                    {
                        mappings.extend(Some((object, (expression, calc_data_types::Function::getLabel))));
                    }
                    "http://www.w3.org/2004/02/skos/core#altLabel" =>
                    {
                        mappings.extend(Some((object, (expression, calc_data_types::Function::getAlias))));
                    }
                    "http://schema.org/description" =>
                    {
                        mappings.extend(Some((object, (expression, calc_data_types::Function::getDescription))));
                    }
                    _=>
                    {}
                }
            }
            _=> panic!("Unrecognized triple in label service.")
        }
    }


    // get no language from service, so return empty array
    if languages.is_empty() {
        return Vec::new();
    }
    // for implizit mapping
    if mappings.is_empty() {
        mappings.extend(variables.iter().filter_map(|var| {
            if let Some((var1, suffix)) = var.rsplit_once("AltLabel")
            {
                if suffix.is_empty() { // ?xyzAltLabel
                    Some((var.to_string(), (Expression::Variable(Pattern::Variable { name: var1.to_string() }), calc_data_types::Function::getAlias))) 
                } else {
                    None
                }
            } 
            else if let Some((var1, suffix)) = var.rsplit_once("Label") {
                if suffix.is_empty() { // ?xyzLabel
                    Some((var.to_string(), (Expression::Variable(Pattern::Variable { name: var1.to_string() }), calc_data_types::Function::getLabel)))
                } else {
                    None
                }
            } 
            else if let Some((var1, suffix)) = var.rsplit_once("Description") {
                if suffix.is_empty() { // ?xyzDescription
                    Some((var.to_string(), (Expression::Variable(Pattern::Variable { name: var1.to_string() }), calc_data_types::Function::getDescription)))
                } else {
                    None
                }
            } 
            else {
                None
            }
        }));
    }
    
    variables
        .iter()
        .map(|var| {
            if let Some((variable,(target, fun))) = mappings.remove_entry(*var)
            {
                // create vec argument, (subject, language 1, ..., language n)
                let mut arguments = Vec::with_capacity(languages.len() + 1);
                // push the subject into argument
                arguments.push(target);
                // then push the languages into argument
                arguments.extend_from_slice(&languages);

                (
                    Pattern::Variable {
                        name: variable,
                    },
                    Expression::FunctionCall(fun, arguments),
                )
            }   
            else {
                (
                    Pattern::Variable {
                        name: var.to_string(),
                    },
                    Expression::Variable(Pattern::Variable {
                        name: var.to_string(),
                    }),
                )
            }
        })
        .collect()

}
