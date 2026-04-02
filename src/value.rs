use std::rc::Rc;

use mrubyedge::{
    Error,
    yamrb::{value::{RObject, RValue}, vm::VM},
};

/// Common connection parameters extracted from kwargs.
pub(crate) struct RedisConnParams {
    pub url: String,
}

/// Extract common Redis connection kwargs and build a Redis URL.
/// If `url` kwarg is given, it takes precedence over individual kwargs.
/// Otherwise, builds URL from `host`, `port`, `tls`, `username`, `password`.
pub(crate) fn parse_redis_conn_params(vm: &mut VM) -> RedisConnParams {
    if let Some(kwargs) = vm.get_kwargs() {
        // url kwarg takes precedence
        if let Some(u) = kwargs.get("url") {
            if !u.is_nil() {
                if let Ok(url) = <&RObject as TryInto<String>>::try_into(u.as_ref()) {
                    return RedisConnParams { url };
                }
            }
        }

        let mut host = "127.0.0.1".to_string();
        let mut port: u16 = 6379;
        let mut tls = false;
        let mut username: Option<String> = None;
        let mut password: Option<String> = None;

        if let Some(h) = kwargs.get("host") {
            if let Ok(v) = h.as_ref().try_into() {
                host = v;
            }
        }
        if let Some(p) = kwargs.get("port") {
            if let Ok(v) = <&RObject as TryInto<i64>>::try_into(p.as_ref()) {
                port = v as u16;
            }
        }
        if let Some(t) = kwargs.get("tls") {
            tls = t.is_truthy();
        }
        if let Some(u) = kwargs.get("username") {
            if !u.is_nil() {
                if let Ok(v) = u.as_ref().try_into() {
                    username = Some(v);
                }
            }
        }
        if let Some(p) = kwargs.get("password") {
            if !p.is_nil() {
                if let Ok(v) = p.as_ref().try_into() {
                    password = Some(v);
                }
            }
        }

        let scheme = if tls { "rediss" } else { "redis" };
        let auth = match (username, password) {
            (Some(u), Some(p)) => format!("{}:{}@", u, p),
            (None, Some(p)) => format!(":{}@", p),
            _ => String::new(),
        };
        let url = format!("{}://{}{}:{}", scheme, auth, host, port);
        return RedisConnParams { url };
    }

    RedisConnParams {
        url: "redis://127.0.0.1:6379".to_string(),
    }
}

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
