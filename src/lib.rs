use std::any::Any;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use mrubyedge::{
    Error,
    yamrb::{
        helpers::{mrb_call_block, mrb_define_class_cmethod, mrb_define_cmethod},
        value::{RData, RHashMap, RObject, RType, RValue},
        vm::VM,
    },
};

// ---------------------------------------------------------------------------
// Internal data types
// ---------------------------------------------------------------------------

enum RedisConn {
    Direct(redis::Connection),
    Pooled(r2d2::PooledConnection<redis::Client>),
}

impl RedisConn {
    fn execute(&mut self, cmd: &mut redis::Cmd) -> redis::RedisResult<redis::Value> {
        match self {
            RedisConn::Direct(c) => cmd.query(c),
            RedisConn::Pooled(c) => cmd.query(&mut **c),
        }
    }
}

struct RedisData {
    conn: RefCell<Option<RedisConn>>,
}

struct RedisPoolData {
    pool: r2d2::Pool<redis::Client>,
}

// ---------------------------------------------------------------------------
// Value conversion
// ---------------------------------------------------------------------------

fn redis_value_to_robject(val: redis::Value) -> Rc<RObject> {
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

fn robject_to_redis_arg(obj: &RObject) -> Result<Vec<u8>, Error> {
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

// ---------------------------------------------------------------------------
// Data access helpers
// ---------------------------------------------------------------------------

fn with_redis_conn<F, R>(obj: &Rc<RObject>, f: F) -> Result<R, Error>
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

fn with_redis_pool<F, R>(obj: &Rc<RObject>, f: F) -> Result<R, Error>
where
    F: FnOnce(&r2d2::Pool<redis::Client>) -> Result<R, Error>,
{
    match &obj.value {
        RValue::Data(data) => {
            let borrow = data.data.borrow();
            let any_ref = borrow
                .as_ref()
                .ok_or_else(|| Error::RuntimeError("pool is invalid".to_string()))?;
            let pool_data = any_ref.downcast_ref::<RedisPoolData>().ok_or_else(|| {
                Error::RuntimeError("not a RedisConnectionPool object".to_string())
            })?;
            f(&pool_data.pool)
        }
        _ => Err(Error::RuntimeError(
            "expected a RedisConnectionPool object".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Object factories
// ---------------------------------------------------------------------------

fn make_redis_object(vm: &mut VM, conn: RedisConn) -> Rc<RObject> {
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

fn make_pool_object(vm: &mut VM, pool: r2d2::Pool<redis::Client>) -> Rc<RObject> {
    let class_obj = vm
        .get_const_by_name("RedisConnectionPool")
        .expect("RedisConnectionPool class not found; did you call init_redis?");
    let class = match &class_obj.value {
        RValue::Class(c) => c.clone(),
        _ => panic!("RedisConnectionPool is not a class"),
    };
    let data = RedisPoolData { pool };
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

/// Redis.new(host: "127.0.0.1", port: 6379)
fn mrb_redis_new(vm: &mut VM, _args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 6379;

    if let Some(kwargs) = vm.get_kwargs() {
        if let Some(h) = kwargs.get("host") {
            host = h.as_ref().try_into()?;
        }
        if let Some(p) = kwargs.get("port") {
            let p_val: i64 = p.as_ref().try_into()?;
            port = p_val as u16;
        }
    }

    let url = format!("redis://{}:{}", host, port);
    let client = redis::Client::open(url.as_str())
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
// RedisConnectionPool class methods
// ---------------------------------------------------------------------------

/// RedisConnectionPool.new(size: 5, timeout: 5, host: "127.0.0.1", port: 6379)
fn mrb_pool_new(vm: &mut VM, _args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 6379;
    let mut size: u32 = 5;
    let mut timeout: u64 = 5;

    if let Some(kwargs) = vm.get_kwargs() {
        if let Some(h) = kwargs.get("host") {
            host = h.as_ref().try_into()?;
        }
        if let Some(p) = kwargs.get("port") {
            let p_val: i64 = p.as_ref().try_into()?;
            port = p_val as u16;
        }
        if let Some(s) = kwargs.get("size") {
            let s_val: i64 = s.as_ref().try_into()?;
            size = s_val as u32;
        }
        if let Some(t) = kwargs.get("timeout") {
            let t_val: i64 = t.as_ref().try_into()?;
            timeout = t_val as u64;
        }
    }

    let url = format!("redis://{}:{}", host, port);
    let client = redis::Client::open(url.as_str())
        .map_err(|e| Error::RuntimeError(format!("Redis connection error: {}", e)))?;
    let pool = r2d2::Pool::builder()
        .max_size(size)
        .connection_timeout(Duration::from_secs(timeout))
        .build(client)
        .map_err(|e| Error::RuntimeError(format!("Redis pool error: {}", e)))?;

    Ok(make_pool_object(vm, pool))
}

/// RedisConnectionPool#checkout - Get a connection from the pool
fn mrb_pool_checkout(vm: &mut VM, _args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    let self_obj = vm.getself()?;

    let pooled_conn = with_redis_pool(&self_obj, |pool| {
        pool.get()
            .map_err(|e| Error::RuntimeError(format!("Redis pool checkout error: {}", e)))
    })?;

    Ok(make_redis_object(vm, RedisConn::Pooled(pooled_conn)))
}

/// RedisConnectionPool#checkin(redis) - Return a connection to the pool
fn mrb_pool_checkin(_vm: &mut VM, args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    if args.is_empty() {
        return Err(Error::ArgumentError(
            "wrong number of arguments (given 0, expected 1)".to_string(),
        ));
    }

    // Take the connection out; dropping it returns it to the pool
    match &args[0].value {
        RValue::Data(data) => {
            let borrow = data.data.borrow();
            let any_ref = borrow
                .as_ref()
                .ok_or_else(|| Error::RuntimeError("invalid Redis object".to_string()))?;
            let redis_data = any_ref
                .downcast_ref::<RedisData>()
                .ok_or_else(|| Error::RuntimeError("not a Redis object".to_string()))?;
            redis_data.conn.borrow_mut().take();
        }
        _ => {
            return Err(Error::ArgumentError(
                "expected a Redis object".to_string(),
            ));
        }
    }

    Ok(RObject::nil().to_refcount_assigned())
}

/// RedisConnectionPool#with { |redis| ... }
fn mrb_pool_with(vm: &mut VM, args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    let block = args
        .first()
        .cloned()
        .ok_or_else(|| Error::ArgumentError("block required".to_string()))?;
    if !matches!(block.value, RValue::Proc(_)) {
        return Err(Error::ArgumentError(
            "RedisConnectionPool#with requires a block".to_string(),
        ));
    }

    let self_obj = vm.getself()?;

    // Checkout a connection from the pool
    let pooled_conn = with_redis_pool(&self_obj, |pool| {
        pool.get()
            .map_err(|e| Error::RuntimeError(format!("Redis pool checkout error: {}", e)))
    })?;

    let redis_obj = make_redis_object(vm, RedisConn::Pooled(pooled_conn));

    // Call block with the connection as argument
    let result = mrb_call_block(vm, block, Some(self_obj), &[redis_obj.clone()], 0);

    // Checkin: drop the connection to return it to pool
    if let RValue::Data(data) = &redis_obj.value {
        let borrow = data.data.borrow();
        if let Some(any_ref) = borrow.as_ref() {
            if let Some(redis_data) = any_ref.downcast_ref::<RedisData>() {
                redis_data.conn.borrow_mut().take();
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Public initializer
// ---------------------------------------------------------------------------

/// Initialize the Redis and RedisConnectionPool classes in the VM.
/// Call this after `VM::open` to make Redis classes available in Ruby code.
pub fn init_redis(vm: &mut VM) {
    // Redis class
    let redis_class = vm.define_class("Redis", None, None);
    mrb_define_class_cmethod(vm, redis_class.clone(), "new", Box::new(mrb_redis_new));
    mrb_define_cmethod(vm, redis_class.clone(), "call", Box::new(mrb_redis_call));

    // RedisConnectionPool class
    let pool_class = vm.define_class("RedisConnectionPool", None, None);
    mrb_define_class_cmethod(vm, pool_class.clone(), "new", Box::new(mrb_pool_new));
    mrb_define_cmethod(vm, pool_class.clone(), "checkout", Box::new(mrb_pool_checkout));
    mrb_define_cmethod(vm, pool_class.clone(), "checkin", Box::new(mrb_pool_checkin));
    mrb_define_cmethod(vm, pool_class.clone(), "with", Box::new(mrb_pool_with));
}
