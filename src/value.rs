use std::rc::Rc;

use mrubyedge::{
    Error,
    yamrb::value::{RObject, RValue},
};

pub(crate) fn redis_value_to_robject(val: redis::Value) -> Rc<RObject> {
    match val {
        redis::Value::Nil => RObject::nil().to_refcount_assigned(),
        redis::Value::Int(i) => RObject::integer(i).to_refcount_assigned(),
        redis::Value::BulkString(bytes) => {
            let s = String::from_utf8_lossy(&bytes).into_owned();
            RObject::string(s).to_refcount_assigned()
        }
        redis::Value::Array(arr) => {
            let objs: Vec<Rc<RObject>> = arr.into_iter().map(redis_value_to_robject).collect();
            RObject::array(objs).to_refcount_assigned()
        }
        redis::Value::SimpleString(s) => RObject::string(s).to_refcount_assigned(),
        redis::Value::Okay => RObject::string("OK".to_string()).to_refcount_assigned(),
        redis::Value::Double(f) => RObject::float(f).to_refcount_assigned(),
        redis::Value::Boolean(b) => RObject::boolean(b).to_refcount_assigned(),
        _ => RObject::nil().to_refcount_assigned(),
    }
}

pub(crate) fn robject_to_redis_arg(obj: &RObject) -> Result<Vec<u8>, Error> {
    match &obj.value {
        RValue::String(s, _) => Ok(s.borrow().clone()),
        RValue::Integer(i) => Ok(i.to_string().into_bytes()),
        RValue::Float(f) => Ok(f.to_string().into_bytes()),
        RValue::Symbol(sym) => Ok(sym.name.as_bytes().to_vec()),
        RValue::Nil => Ok(b"".to_vec()),
        _ => Err(Error::ArgumentError(
            "unsupported argument type for Redis command".to_string(),
        )),
    }
}
