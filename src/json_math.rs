use std::ops::{self};
use std::convert::From;

use serde_json::Number;


#[derive(PartialEq, PartialOrd)]
pub enum JsonNumber { 
    U64(u64),
    I64(i64),
    F64(f64),
}

impl JsonNumber {
    pub fn to_number(self) -> Number {
        match self {
            JsonNumber::U64(n) => Number::from(n),
            JsonNumber::I64(n) => Number::from(n),
            JsonNumber::F64(n) => Number::from_f64(n).unwrap()
        }
    }
}

impl From<&Number> for JsonNumber {
    fn from(number: &Number) -> Self {
        if number.is_u64() {
            JsonNumber::U64(number.as_u64().unwrap())
        } else if number.is_i64() {
            JsonNumber::I64(number.as_i64().unwrap())
        } else if number.is_f64() {
            JsonNumber::F64(number.as_f64().unwrap())
        } else {
            panic!("Invalid number type");
        }
    }
}

impl From<u64> for JsonNumber {
    fn from(number: u64) -> Self {
        JsonNumber::U64(number)
    }
}

impl From<i64> for JsonNumber {
    fn from(number: i64) -> Self {
        JsonNumber::I64(number)
    }
}

impl From<f64> for JsonNumber {
    fn from(number: f64) -> Self {
        JsonNumber::F64(number)
    }
}

macro_rules! json_number_arith {
    ($e1:ident $op:tt $e2:ident) => {
        match ($e1, $e2) {
            (JsonNumber::U64(n1), JsonNumber::U64(n2)) => JsonNumber::from(n1 $op n2),
            (JsonNumber::U64(n1), JsonNumber::I64(n2)) => JsonNumber::from(n1 as i64 $op n2),
            (JsonNumber::U64(n1), JsonNumber::F64(n2)) => JsonNumber::from(n1 as f64 $op n2),
            (JsonNumber::I64(n1), JsonNumber::U64(n2)) => JsonNumber::from(n1 $op n2 as i64),
            (JsonNumber::I64(n1), JsonNumber::I64(n2)) => JsonNumber::from(n1 $op n2),
            (JsonNumber::I64(n1), JsonNumber::F64(n2)) => JsonNumber::from(n1 as f64 $op n2),
            (JsonNumber::F64(n1), JsonNumber::U64(n2)) => JsonNumber::from(n1 $op n2 as f64),
            (JsonNumber::F64(n1), JsonNumber::I64(n2)) => JsonNumber::from(n1 $op n2 as f64),
            (JsonNumber::F64(n1), JsonNumber::F64(n2)) => JsonNumber::from(n1 $op n2)
        }
    };
}


impl ops::Add<JsonNumber> for JsonNumber {
    type Output = JsonNumber;

    fn add(self, rhs: JsonNumber) -> JsonNumber {
        json_number_arith!(self + rhs)
    }
}

impl ops::Sub<JsonNumber> for JsonNumber {
    type Output = JsonNumber;
    fn sub(self, rhs: JsonNumber) -> JsonNumber {
        json_number_arith!(self - rhs)
    }
}

impl ops::Mul<JsonNumber> for JsonNumber {
    type Output = JsonNumber;
    fn mul(self, rhs: JsonNumber) -> JsonNumber {
        json_number_arith!(self * rhs)
    }
}

impl ops::Div<JsonNumber> for JsonNumber {
    type Output = JsonNumber;
    fn div(self, rhs: JsonNumber) -> JsonNumber {
        json_number_arith!(self / rhs)
    }
}
