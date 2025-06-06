use enum_primitive_derive::Primitive;
use num_traits::Zero;
use serde::Deserialize;
use serde::Serialize;

use crate::small_string::SmallString;
use crate::to_json_result;

/// DataValue contains all datatypes that a property can have (everything under "datavalue" in json)
#[derive(Debug, PartialEq, Clone)]
pub enum DataValue<'a> {
    /*
        String-based data types include: (general) Strings, external identifiers, URLs, monolingual text
        commons media, geographic shape, tabular data, mathematical expressions and musical notation.

    */
    WikidataidQ(u64),
    WikidataidP(u64),
    WikidataidL(u64),
    String(SmallString<'a>),
    Identifier(SmallString<'a>),
    Url(SmallString<'a>),
    MonolinText(Box<MonolinText<'a>>),
    Media(SmallString<'a>),
    GeoShape(SmallString<'a>),
    Tabular(SmallString<'a>),
    MathExp(SmallString<'a>),
    MusicNotation(SmallString<'a>),
    Quantity(Box<Quantity<'a>>),
    Time(Time),
    Coord(Coord),
    Label(SmallString<'a>), //store the language codes as str
    Description(SmallString<'a>),
    Alias(SmallString<'a>),
    Null(),
    NumberInt(i32),
    NumberFloat(f32),
    Boolean(bool),
    Edge(u64),
    //WikidataidPStatement(u64),
    NamedEdge(SmallString<'a>),
    WikidataidPstmt(u64),
}

#[derive(PartialEq)]
pub enum RDFType {
    IRI,
    Literal,
    BNode,
}

impl From<RDFType> for &'static str {
    fn from(value: RDFType) -> Self {
        match value {
            RDFType::IRI => "uri",
            RDFType::Literal => "literal",
            RDFType::BNode => "bnode",
        }
    }
}

impl From<RDFType> for SmallString<'static> {
    fn from(value: RDFType) -> Self {
        let str: &str = value.into();
        str.into()
    }
}

impl DataValue<'_> {
    pub fn get_rdf_type(&self) -> RDFType {
        match self
        {
            DataValue::WikidataidP{..} | DataValue::WikidataidPstmt{..} |
            DataValue::WikidataidL{..} | DataValue::WikidataidQ{..} | DataValue::NamedEdge{..} 
            => RDFType::IRI,
            _ => RDFType::Literal
        }
    }

    pub fn get_rdf_value(&self) -> SmallString<'static> {
        match self
        {
            DataValue::WikidataidQ(number) =>
            {
                let str_form = format!("http://www.wikidata.org/entity/Q{}", number);
                str_form.into()
            }
            DataValue::WikidataidP(number) =>
            {
                let str_form = format!("http://www.wikidata.org/prop/P{}", number);
                str_form.into()
            }
            DataValue::WikidataidL(number) =>
            {
                let str_form = format!("http://www.wikidata.org/entity/L{}", number);
                str_form.into()
            }
            DataValue::Coord(inputval) =>
            {
                let str_form = format!("Point({} {})", inputval.longitude, inputval.latitude);
                str_form.into()
            }
            DataValue::MonolinText(inputval) =>
            {
                inputval.text.clone().to_owned()
            }
            DataValue::Media(inputval) =>
            {
                let str_form = format!(
                    "http://commons.wikimedia.org/wiki/Special:FilePath/{}",
                    inputval.as_str());
                str_form.into()
            }
            DataValue::GeoShape(inputval) =>
            {
                let str_form = format!(
                    "http://commons.wikimedia.org/data/main/{}",
                    inputval.as_str());
                str_form.into()
            }
            DataValue::Tabular(inputval) =>
            {
                let str_form = format!(
                    "http://commons.wikimedia.org/data/main/{}",
                    inputval.as_str());
                str_form.into()
            }
            DataValue::Quantity(inputval) =>
            {
                let str_form = ignore_leading_plus_sign(inputval.amount.clone());
                str_form
            }
            DataValue::Time(inputval) =>
            {
                let str_form = format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                    inputval.year,
                    inputval.month,
                    inputval.day,
                    inputval.hour,
                    inputval.minute,
                    inputval.second
                );
                str_form.into()
            }
            DataValue::MathExp(this_string) =>
            {
                this_string.clone().to_owned()
            }
            DataValue::Alias(inputval) =>
            {
                let str_form = format!("Alias: {inputval}");
                str_form.into()
            }
            DataValue::Label(inputval) =>
            {
                let str_form = format!("Label: {inputval}");
                str_form.into()
            }
            DataValue::Description(inputval) =>
            {
                let str_form = format!("Description: {inputval}");
                str_form.into()
            }
            DataValue::NumberInt(inputval) =>
            {
                let str_form = format!("{}",inputval);
                str_form.into()
            }
            DataValue::NumberFloat(inputval) =>
            {
                let str_form = format!("{}",inputval);
                str_form.into()
            }
            DataValue::Boolean(inputval) =>
            {
                inputval.to_string().into()
            }
            DataValue::Edge(inputval) =>
            {
                let str_form = format!("http://www.wikidata.org/entity/statement/{}", inputval);
                str_form.into()
            }
            DataValue::NamedEdge(inputval) =>
            {
                let str_form = format!("http://www.wikidata.org/entity/statement/{}", inputval);
                str_form.into()
            }
            DataValue::String(this_string) | DataValue::Identifier(this_string) |
            DataValue::Url(this_string) | DataValue::MusicNotation(this_string) =>
            {
                this_string.clone().to_owned()
            }
            _=> // temporarily return Null and WikidataidPstmt's value "Not Implemented"
            {
                "Not Implemented!".into()
            }
        }
    }

    pub fn get_lang(&self) -> Option<SmallString<'static>> {
        match self {
            DataValue::MonolinText(text) => Some(text.language.clone().to_owned()),
            _ => None,
        }
    }

    pub fn get_datatype(&self) -> Option<SmallString<'static>> {
        match self
        {
            DataValue::Coord(_inputval) =>
            {
                Some("http://www.opengis.net/ont/geosparql#wktLiteral".into())
            }
            DataValue::Quantity(_inputval) =>
            {
                Some("http://www.w3.org/2001/XMLSchema#decimal".into())
            }
            DataValue::Time(_inputval) =>
            {
                Some("http://www.w3.org/2001/XMLSchema#dateTime".into())
            }
            DataValue::MathExp(_inputval) =>
            {
                Some("http://www.w3.org/1998/Math/MathML".into())
            }
            DataValue::NumberInt(_inputval) =>
            {
                Some("http://www.w3.org/2001/XMLSchema#integer".into())
            }
            DataValue::NumberFloat(_inputval) =>
            {
                Some("http://www.w3.org/2001/XMLSchema#decimal".into())
            }
            DataValue::Boolean(_inputval) =>
            {
                Some("http://www.w3.org/2001/XMLSchema#boolean".into())
            }
            // temporarily return None as other types' datatype
            _=> None
        }
    }

    pub fn into_effective_boolean(self) -> Result<bool, String> {
        match self {
            DataValue::Boolean(b) => Ok(b),
            DataValue::String(s) => Ok(! s.is_empty()),
            DataValue::NumberInt(i) => Ok(! i.is_zero()),
            DataValue::NumberFloat(f) => Ok(! (f.is_zero() | f.is_nan())),
            _ => Err(format!("Unexpected datatype for boolean {:?}", self)),
        }
    }
}

/// auxiliary function for conversion of Quantity datatypes to JsonObject structs
fn ignore_leading_plus_sign(inputstring: SmallString) -> SmallString<'static> {
    let mut owned_string: String = inputstring.to_string();
    let first = inputstring.chars().next();
    if first == Some('+') {
        owned_string.remove(0);
        return owned_string.into();
    } else {
        return owned_string.into();
    }
}


impl From<bool> for DataValue<'_> {
    fn from(value: bool) -> Self {
        DataValue::Boolean(value)
    }
}

use std::cmp::Ordering;
use std::f64::MAX;

impl PartialOrd for DataValue<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else {
            match (self, other) {
                (DataValue::WikidataidQ(value1), DataValue::WikidataidQ(value2)) => {
                    Some(value1.cmp(value2))
                }
                (DataValue::WikidataidP(value1), DataValue::WikidataidP(value2)) => {
                    Some(value1.cmp(value2))
                }
                (DataValue::WikidataidL(value1), DataValue::WikidataidL(value2)) => {
                    Some(value1.cmp(value2))
                }
                (DataValue::Edge(value1), DataValue::Edge(value2)) => Some(value1.cmp(value2)),
                (DataValue::MonolinText(value1), DataValue::MonolinText(value2)) => {
                    value1.partial_cmp(value2)
                }
                (DataValue::NumberFloat(value1), DataValue::NumberFloat(value2)) => {
                    value1.partial_cmp(value2)
                }
                (DataValue::NumberInt(value1), DataValue::NumberInt(value2)) => {
                    Some(value1.cmp(value2))
                }
                (DataValue::NumberFloat(value1), DataValue::NumberInt(value2)) => {
                    value1.partial_cmp(&(*value2 as f32))
                }
                (DataValue::NumberInt(value1), DataValue::NumberFloat(value2)) => {
                    (*value1 as f32).partial_cmp(value2)
                }
                (DataValue::Quantity(value1), DataValue::Quantity(value2)) => {
                    value1.partial_cmp(value2)
                }
                (DataValue::String(value1), DataValue::String(value2)) => Some(value1.cmp(value2)),
                (DataValue::Time(value1), DataValue::Time(value2)) => value1.partial_cmp(value2),
                (DataValue::WikidataidPstmt(value1), DataValue::WikidataidPstmt(value2)) => {
                    Some(value1.cmp(value2))
                }
                (DataValue::NamedEdge(value1), DataValue::NamedEdge(value2)) => {
                    Some(value1.cmp(value2))
                }
                _ => None,
            }
        }
    }
}

use std::fmt::Display;
use std::fmt::Write;
use std::str::Split;

/// Fields: &str amount, Option<&str> upper_bound, Option<&str> lowerBound and Option<&str> unit
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// Allow non-snake-case since it is directly used for parsing the file.
#[allow(non_snake_case)]
pub struct Quantity<'a> {
    #[serde(borrow)]
    pub amount: SmallString<'a>, // amount is a &str in the json file and can be bigger than a f64
    #[serde(borrow)]
    pub unit: SmallString<'a>, // unit of measurement
    #[serde(borrow)]
    pub upperBound: Option<SmallString<'a>>, // optional
    #[serde(borrow)]
    pub lowerBound: Option<SmallString<'a>>, // optional
}

impl PartialOrd for Quantity<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.unit == other.unit {
            let amount1: f64 = self.amount.as_str().parse().unwrap();
            let amount2: f64 = other.amount.as_str().parse().unwrap();

            if (amount1 == MAX) && (amount2 == MAX) {
                None
            } else {
                amount1.partial_cmp(&amount2)
            }
        } else {
            None
        }
    }
}

impl Display for Quantity<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.amount)?;
        f.write_char(0x1f.into())?;
        f.write_str(&self.unit)?;
        if self.upperBound.is_some() || self.lowerBound.is_some() {
            f.write_char(0x1f.into())?;
            f.write_str(self.lowerBound.as_ref().unwrap_or(&SmallString::empty()))?;
            f.write_char(0x1f.into())?;
            f.write_str(self.upperBound.as_ref().unwrap_or(&SmallString::empty()))?;
        }

        Ok(())
    }
}

/// for conversion of string slices to SmallString
impl<'a> From<&'a str> for Quantity<'static> {
    fn from(qstr: &'a str) -> Quantity<'static> {
        let mut split: Split<char> = qstr.split(0x1fu8.into());
        let amount: SmallString = split.next().unwrap().into();
        let unit: SmallString = split.next().unwrap_or_default().into();
        let lower_bound: Option<SmallString> = split.next().map(|s| s.into());
        let upper_bound: Option<SmallString> = split.next().map(|s| s.into());

        Quantity {
            amount: amount.to_owned(),
            unit: unit.to_owned(),
            lowerBound: lower_bound.map(|s| s.to_owned()),
            upperBound: upper_bound.map(|s| s.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct Time {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub before: u8,
    pub after: u8,
    pub precision: u8,
    pub timezone: i16,
    pub calendarmodel: CalendarModel,
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.calendarmodel == CalendarModel::Unknown || self.calendarmodel != other.calendarmodel
        {
            return None;
        }
        match self.year.partial_cmp(&other.year) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.month.partial_cmp(&other.month) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.day.partial_cmp(&other.day) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.hour.partial_cmp(&other.hour) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.minute.partial_cmp(&other.minute) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }

        self.second.partial_cmp(&other.second)
    }
}

/// Fields: String text, &str language
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MonolinText<'a> {
    #[serde(borrow)]
    pub text: SmallString<'a>,
    #[serde(borrow)]
    pub language: SmallString<'a>,
}

impl PartialOrd for MonolinText<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.language != other.language {
            None
        } else {
            self.text.partial_cmp(other.text.as_str())
        }
    }
}

/// for conversion of string slices to SmallString
impl<'a> From<&'a str> for MonolinText<'static> {
    fn from(qstr: &'a str) -> MonolinText<'static> {
        let mut split: Split<char> = qstr.split(0x1fu8.into());
        let text: SmallString = split.next().unwrap().into();
        let language: SmallString = split.next().unwrap().into();

        MonolinText {
            text: text.to_owned(),
            language: language.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Primitive)]
pub enum CalendarModel {
    #[serde(alias = "http://www.wikidata.org/entity/Q1985727")]
    Gregorian = 0x1,
    #[serde(alias = "http://www.wikidata.org/entity/Q1985786")]
    Julian = 0x2,
    #[serde(other)]
    Unknown = 0x3,
}

/// Fields: f32 latitude, f32 longitude, Option<f64> altitude, String globe, f32 precision
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Coord {
    pub latitude: f32,  // WGS84 coordinates
    pub longitude: f32, // WGS84 coordinates
    pub globe: u64,     // given as URI; example:"http:\/\/www.wikidata.org\/entity\/Q2"
    pub precision: Option<f32>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn boolean_cases() {
        assert!(DataValue::Boolean(true).into_effective_boolean() == Ok(true));
        assert!(DataValue::Boolean(false).into_effective_boolean() == Ok(false));
        assert!(DataValue::String("".into()).into_effective_boolean() == Ok(false));
        assert!(DataValue::String("some".into()).into_effective_boolean() == Ok(true));
        assert!(DataValue::NumberInt(0).into_effective_boolean() == Ok(false));
        assert!(DataValue::NumberInt(1).into_effective_boolean() == Ok(true));
        assert!(DataValue::NumberFloat(0.0).into_effective_boolean() == Ok(false));
        assert!(DataValue::NumberFloat(1.0).into_effective_boolean() == Ok(true));
        assert!(DataValue::NumberFloat(f32::NAN).into_effective_boolean() == Ok(false));
        println!(
            "{:?}",
            DataValue::Media("some".into()).into_effective_boolean()
        );
        assert!(DataValue::Media("some".into())
            .into_effective_boolean()
            .is_err());
    }

    #[test]
    fn datavalues_comparable() {
        assert!(DataValue::WikidataidQ(12) == DataValue::WikidataidQ(12));
        assert!(DataValue::WikidataidQ(12) != DataValue::NumberInt(12));
        assert!(DataValue::WikidataidPstmt(12) == DataValue::WikidataidPstmt(12));
        assert!(DataValue::WikidataidPstmt(12) != DataValue::WikidataidPstmt(13));
        assert!(DataValue::WikidataidPstmt(12) != DataValue::WikidataidP(12));
    }
}
