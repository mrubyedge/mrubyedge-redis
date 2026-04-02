use std::any::Any;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mrubyedge::{
    Error,
    yamrb::{
        helpers::{mrb_define_class_cmethod, mrb_define_cmethod},
        value::{RData, RHashMap, RObject, RType, RValue},
        vm::VM,
    },
};

use crate::value::{parse_redis_conn_params, redis_value_to_robject, robject_to_redis_arg};

// ---------------------------------------------------------------------------
// Internal data types
// ---------------------------------------------------------------------------

pub(crate) enum RedisConn {
    Direct(redis::Connection),
    Pooled(r2d2::PooledConnection<redis::Client>),
}

impl RedisConn {
    pub(crate) fn execute(&mut self, cmd: &mut redis::Cmd) -> redis::RedisResult<redis::Value> {
        match self {
            RedisConn::Direct(c) => cmd.query(c),
            RedisConn::Pooled(c) => cmd.query(&mut **c),
        }
    }
}

pub(crate) struct RedisData {
    pub(crate) conn: RefCell<Option<RedisConn>>,
}

// ---------------------------------------------------------------------------
// Data access helper
// ---------------------------------------------------------------------------

pub(crate) fn with_redis_conn<F, R>(obj: &Rc<RObject>, f: F) -> Result<R, Error>
where
    F: FnOnce(&mut RedisConn) -> Result<R, Error>,
{
    match &obj.value {
        RValue::Data(data) => {
            let borrow = data.data.borrow();
            let any_ref = borrow
                .as_ref()
                .ok_or_else(|| Error::RuntimeError("connection is closed".to_string()))?;
            let redis_data = any_ref
                .downcast_ref::<RedisData>()
                .ok_or_else(|| Error::RuntimeError("not a Redis object".to_string()))?;
            let mut conn = redis_data.conn.borrow_mut();
            let conn = conn
                .as_mut()
                .ok_or_else(|| Error::RuntimeError("connection is closed".to_string()))?;
            f(conn)
        }
        _ => Err(Error::RuntimeError("expected a Redis object".to_string())),
    }
}

// ---------------------------------------------------------------------------
// Object factory
// ---------------------------------------------------------------------------

pub(crate) fn make_redis_object(vm: &mut VM, conn: RedisConn) -> Rc<RObject> {
    let class_obj = vm
        .get_const_by_name("Redis")
        .expect("Redis class not found; did you call init_redis?");
    let class = match &class_obj.value {
        RValue::Class(c) => c.clone(),
        _ => panic!("Redis is not a class"),
    };
    let data = RedisData {
        conn: RefCell::new(Some(conn)),
    };
    let rdata = Rc::new(RData {
        class,
        data: RefCell::new(Some(Rc::new(Box::new(data) as Box<dyn Any>))),
        ref_count: 1,
    });
    Rc::new(RObject {
        tt: RType::Data,
        value: RValue::Data(rdata),
        object_id: Cell::new(u64::MAX),
        singleton_class: RefCell::new(None),
        ivar: RefCell::new(RHashMap::default()),
    })
}

// ---------------------------------------------------------------------------
// Redis class methods
// ---------------------------------------------------------------------------

/// Redis.new(host: "127.0.0.1", port: 6379, tls: false, username: nil, password: nil)
fn mrb_redis_new(vm: &mut VM, _args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    let params = parse_redis_conn_params(vm);
    let client = redis::Client::open(params.url.as_str())
        .map_err(|e| Error::RuntimeError(format!("Redis connection error: {}", e)))?;
    let conn = client
        .get_connection()
        .map_err(|e| Error::RuntimeError(format!("Redis connection error: {}", e)))?;

    Ok(make_redis_object(vm, RedisConn::Direct(conn)))
}

/// Redis#call(*args) - Execute a Redis command
fn mrb_redis_call(vm: &mut VM, args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    if args.is_empty() {
        return Err(Error::ArgumentError(
            "wrong number of arguments (given 0, expected 1+)".to_string(),
        ));
    }

    let self_obj = vm.getself()?;

    let cmd_name: String = args[0].as_ref().try_into()?;
    let mut cmd = redis::cmd(&cmd_name);
    for arg in &args[1..] {
        cmd.arg(robject_to_redis_arg(arg)?);
    }

    with_redis_conn(&self_obj, |conn| {
        let result = conn
            .execute(&mut cmd)
            .map_err(|e| Error::RuntimeError(format!("Redis error: {}", e)))?;
        Ok(redis_value_to_robject(result))
    })
}

// ---------------------------------------------------------------------------
// Public initializer
// ---------------------------------------------------------------------------

pub fn init_redis_class(vm: &mut VM) {
    let redis_class = vm.define_class("Redis", None, None);
    mrb_define_class_cmethod(vm, redis_class.clone(), "new", Box::new(mrb_redis_new));
    mrb_define_cmethod(vm, redis_class.clone(), "call", Box::new(mrb_redis_call));
}
