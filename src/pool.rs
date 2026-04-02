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

use crate::redis::{make_redis_object, RedisConn, RedisData};

// ---------------------------------------------------------------------------
// Internal data types
// ---------------------------------------------------------------------------

struct RedisPoolData {
    pool: r2d2::Pool<redis::Client>,
}

// ---------------------------------------------------------------------------
// Data access helper
// ---------------------------------------------------------------------------

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
// Object factory
// ---------------------------------------------------------------------------

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
// RedisConnectionPool class methods
// ---------------------------------------------------------------------------

/// RedisConnectionPool.new(size: 5, timeout: 5, host: "127.0.0.1", port: 6379, tls: false)
fn mrb_pool_new(vm: &mut VM, _args: &[Rc<RObject>]) -> Result<Rc<RObject>, Error> {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 6379;
    let mut tls = false;
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
        if let Some(t) = kwargs.get("tls") {
            tls = t.is_truthy();
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

    let scheme = if tls { "rediss" } else { "redis" };
    let url = format!("{}://{}:{}", scheme, host, port);
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

pub fn init_pool_class(vm: &mut VM) {
    let pool_class = vm.define_class("RedisConnectionPool", None, None);
    mrb_define_class_cmethod(vm, pool_class.clone(), "new", Box::new(mrb_pool_new));
    mrb_define_cmethod(vm, pool_class.clone(), "checkout", Box::new(mrb_pool_checkout));
    mrb_define_cmethod(vm, pool_class.clone(), "checkin", Box::new(mrb_pool_checkin));
    mrb_define_cmethod(vm, pool_class.clone(), "with", Box::new(mrb_pool_with));
}
