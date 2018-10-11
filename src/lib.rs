extern crate string_interner;

use string_interner::Sym;

#[derive(Debug)]
pub enum ValVec {
    InternedString(Vec<Sym>),
    Integer(Vec<i64>),
    Float(Vec<f64>),
}

#[macro_export]
macro_rules! valvec_call {
    ($self:ident, $fn:ident, $v:expr) => (
        match $self {
            ValVec::InternedString(is) => is.$fn($v as Sym),
            ValVec::Integer(i) => i.$fn($v),
            ValVec::Float(f) => f.$fn($v),
        }
    )
}

impl ValVec {
    // pub fn push2<T>(&mut self, value: T) {
    //     valvec_call!(self, push, value);
    // }
    pub fn push_is(&mut self, value: Sym) {
        match self {
            ValVec::InternedString(i) => i.push(value),
            _ => (),
        }
    }
    pub fn push_i(&mut self, value: i64) {
        match self {
            ValVec::Integer(i) => i.push(value),
            _ => (),
        }
    }
    pub fn push_f(&mut self, value: f64) {
        match self {
            ValVec::Float(i) => i.push(value),
            _ => (),
        }
    }
}
