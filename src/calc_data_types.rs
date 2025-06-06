use spargebra::term::NamedNode;


/// Pattern: enum of variables and terms
/// -> NamedNode, Variable, Literal, BlankNode(, Placeholder)
#[derive(Debug, Clone)]
pub enum Pattern {
    Variable { name: String },
    NamedNode { name: String },
    BlankNode { name: String },
    Literal { name: String },
}

/// #spargebra: PropertyPathExpression 
#[derive(Debug)]
#[derive(Clone)]
pub enum PPE{
    NamedNode(Pattern),
    Reverse { reverse: Box<PPE> },
    Sequence { front: Box<PPE>, back: Box<PPE> },
    Alternative { front: Box<PPE>, back: Box<PPE> },
    ZeroOrMore { zom: Box<PPE> },
    OneOrMore { oom: Box<PPE> },
    ZeroOrOne { zoo: Box<PPE> },
    NegatedPropertySet { set: Vec<Pattern> },
}

/// #spargebra: Expression
#[derive(Debug, Clone)]
pub enum Expression {
    NamedNode(Pattern),
    Literal(Pattern),
    Variable(Pattern),
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    Equal(Box<Self>, Box<Self>),
    SameTerm(Box<Self>, Box<Self>),
    Greater(Box<Self>, Box<Self>),
    GreaterOrEqual(Box<Self>, Box<Self>),
    Less(Box<Self>, Box<Self>),
    LessOrEqual(Box<Self>, Box<Self>),
    In(Box<Self>, Vec<Self>),
    Add(Box<Self>, Box<Self>),
    Subtract(Box<Self>, Box<Self>),
    Multiply(Box<Self>, Box<Self>),
    Divide(Box<Self>, Box<Self>),
    UnaryPlus(Box<Self>),
    UnaryMinus(Box<Self>),
    Not(Box<Self>),
    Exists(Box<Operator>),
    Bound(Pattern),
    If(Box<Self>, Box<Self>, Box<Self>),
    Coalesce(Vec<Self>),
    FunctionCall(Function, Vec<Self>),
    //FunctionCall(String, Vec<Self>),
    ColumnNumber(usize),
    BoundVariable(usize),
}

/// #spargebra: GroundTerm
#[derive(Debug, Clone)]
pub enum GT {
    NamedNode(Pattern),
    Literal(Pattern),
}

/// #spargebra: Sorting Order
/// Sort either ascending (ASC) or descending (DESC) depending on the given expression
#[derive(Debug, Clone)]
pub enum SO {
    ASC(Expression),
    DESC(Expression),
}

/// #spargebra: Aggregate Expression
#[derive(Debug, Clone)]
pub enum AE {
    Count {
        expr: Option<Box<Expression>>,
        distinct: bool,
    },
    Sum {
        expr: Box<Expression>,
        distinct: bool,
    },
    Avg {
        expr: Box<Expression>,
        distinct: bool,
    },
    Min {
        expr: Box<Expression>,
        distinct: bool,
    },
    Max {
        expr: Box<Expression>,
        distinct: bool,
    },
    GroupConcat {
        expr: Box<Expression>,
        distinct: bool,
        separator: Option<String>,
    },
    Sample {
        expr: Box<Expression>,
        distinct: bool,
    },
    Custom {
        name: Pattern,
        expr: Box<Expression>,
        distinct: bool,
    },
}

///Enum Operator for creating the queryplan (qp) -> Operator is our qp
#[derive(Debug)]
#[derive(Clone)]
pub enum Operator{
    /// BGP: Basic Graph Pattern is a set of TriplePatterns
    Bgp{inner: Vec<Self>},

    /// GRAPH: A subject endpoint (an RDF term or a variable), 
    /// a property path express and an object endpoint.
    Path{subject: Box<Pattern>,
         path: Box<PPE>,
         object: Box<Pattern>},

    /// MERGEJOIN: Takes two relations to merge them
    MergeJoin{left_child: Box<Self>, 
              right_child: Box<Self>},

    /// LEFTJOIN: Takes two tables and returns also columns 
    /// of data that are present only in  the LEFT table
    LeftJoin{left_child: Box<Self>, 
             right_child: Box<Self>,
             expression: Option<Expression>},

    /// LATERALJOIN: A correlated subquery, not a plain subquery
    LateralJoin{left_child: Box<Self>,
                right_child: Box<Self>},

    /// FILTER: Filters for an expression
    Filter{expression: Expression,
           inner: Box<Self>},

    /// UNION: Combine the result-set of two or more SELECT statements
    Union{left: Box<Self>,
          right: Box<Self>},
    
    /// GRAPH: Create a new graph with the given pattern
    Graph{name: Pattern, 
          inner: Box<Self>},

    /// EXTEND: appends element to the given collection
    Extend{inner: Box<Self>,
           variable: Pattern,
           expression: Expression},
    
    /// MINUS: Return all rows in the first SELECT statement that are not
    /// returned by the second SELECT statement
    Minus{left:Box<Self>,
          right:Box<Self>},

    /// VALUES: ---
    Values{variables: Vec<Pattern>,
           bindings: Vec<Vec<Option<GT>>>},

    /// ORDER: Sort the result-set in ascending or descending order
    Order{inner: Box<Self>,
          order_attribute: Vec<SO>},

    /// PROJECTION: Chooses which columns the query shall return
    Projection{inner: Box<Self>,
               attributes: Vec<Pattern>},

    /// PROJECT_EXTEND: Combines Extend and Project into one Operation
    ProjectExtend{inner: Box<Self>,
                  expressions: Vec<(Pattern, Expression)>},

    /// DISTINCT: Only return different (distinct) values
    Distinct{inner: Box<Self>},

    /// REDUCED: rowset reducer
    Reduced{inner: Box<Self>},

    /// SLICE: Returns a subset of an array
    Slice{inner: Box<Self>,
          start: usize,
          length: Option<usize>},

    /// GROUP: groups rows that have the same values into summary rows
    Group{inner: Box<Self>, 
          variables: Vec<Pattern>,
          aggregates: Vec<(Pattern, AE)>},

    /// SERVICE: get infos from external service e.g. labels
    Service{name: Pattern,
            inner: Box<Self>,
            silent: bool},

    /// TRIPLEPATTERN: Is a triple of type: (subject, predicate, object)
    TriplePattern{
        s:Box<Pattern> ,
        p:Box<Pattern> ,
        o:Box<Pattern>},
}

/// #spargebra: Function
/// This function enum is from algebra.rs at 322 row
/// It is from this website: https://github.com/oxigraph/oxigraph/blob/main/lib/spargebra/src/algebra.rs 

/// The number behind Function is the index of function in Website https://www.w3.org/TR/sparql11-query/
/// I wrote this to make it easier to find the definitions of these functions on this website later.
/// Meaning of index 17.x
/// 17.4 Function Definitions
/// 17.4.1 Functional Forms
/// 17.4.2 Functions on RDF Terms
/// 17.4.3 Functions on Strings
/// 17.4.4 Functions on Numerics
/// 17.4.5 Functions on Dates and Times
/// 17.4.6 Hash Functions  
#[derive(Debug, Clone)]
pub enum Function {

    // RDF Terms functions
    IsIri, // 17.4.2.1
    IsBlank, // 17.4.2.2
    IsLiteral, // 17.4.2.3
    IsNumeric, // 17.4.2.4
    Str, // 17.4.2.5
    Lang, // 17.4.2.6
    Datatype, // 17.4.2.7
    Iri, // 17.4.2.8
    BNode, // 17.4.2.9
    StrDt, // 17.4.2.10
    StrLang, // 17.4.2.11
    Uuid, // 17.4.2.12
    StrUuid, // 17.4.2.13

    // String functions 
    StrLen, // 17.4.3.2
    SubStr, // 17.4.3.3
    UCase, // 17.4.3.4
    LCase, // 17.4.3.5
    StrStarts, // 17.4.3.6
    StrEnds, // 17.4.3.7
    Contains, // 17.4.3.8
    StrBefore, // 17.4.3.9
    StrAfter, // 17.4.3.10
    EncodeForUri, // 17.4.3.11
    Concat, // 17.4.3.12
    LangMatches, // 17.4.3.13
    Regex, // 17.4.3.14
    Replace, // 17.4.3.15

    // Numerics functions
    Abs, // 17.4.4.1
    Round, // 17.4.4.2
    Ceil, // 17.4.4.3
    Floor, // 17.4.4.4
    Rand, // 17.4.4.5

    // time functions
    Now, // 17.4.5.1
    Year, // 17.4.5.2
    Month, // 17.4.5.3
    Day, // 17.4.5.4
    Hours, // 17.4.5.5
    Minutes, // 17.4.5.6
    Seconds, // 17.4.5.7
    Timezone, // 17.4.5.8
    Tz, // 17.4.5.9

    // hash functions
    Md5, // 17.4.6.1
    Sha1, // 17.4.6.2
    Sha256, // 17.4.6.3
    Sha384, // 17.4.6.4
    Sha512, // 17.4.6.5

    // functions about the label service 
    getLabel, 
    getDescription,
    getAlias,
    // the custom function 
    Custom(Pattern) 
}
