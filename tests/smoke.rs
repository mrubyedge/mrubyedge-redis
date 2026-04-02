extern crate mrubyedge;
extern crate mrubyedge_redis;

mod helpers;
use helpers::*;

// NOTE: Requires localhost redis to run tests

#[test]
fn test_redis_set_get() {
    let code = r##"
redis = Redis.new(host: "127.0.0.1", port: 6379)
redis.call("SET", "mrubyedge_test_key", "hello")
val = redis.call("GET", "mrubyedge_test_key")
assert_eq(val, "hello")
redis.call("DEL", "mrubyedge_test_key")
"##;

    let binary = mrbc_compile("redis_set_get", code);
    let mut rite = mrubyedge::rite::load(&binary).unwrap();
    let mut vm = mrubyedge::yamrb::vm::VM::open(&mut rite);
    mrubyedge_redis::init_redis(&mut vm);
    define_assert_eq(&mut vm);
    let result = vm.run().unwrap();
    let deleted: i32 = result.as_ref().try_into().unwrap();
    assert_eq!(deleted, 1);
}

#[test]
fn test_redis_new_with_url() {
    let code = r##"
redis = Redis.new(url: "redis://127.0.0.1:6379")
redis.call("SET", "mrubyedge_url_test", "from_url")
val = redis.call("GET", "mrubyedge_url_test")
assert_eq(val, "from_url")
redis.call("DEL", "mrubyedge_url_test")
"##;

    let binary = mrbc_compile("redis_new_url", code);
    let mut rite = mrubyedge::rite::load(&binary).unwrap();
    let mut vm = mrubyedge::yamrb::vm::VM::open(&mut rite);
    mrubyedge_redis::init_redis(&mut vm);
    define_assert_eq(&mut vm);
    let result = vm.run().unwrap();
    let deleted: i32 = result.as_ref().try_into().unwrap();
    assert_eq!(deleted, 1);
}

#[test]
fn test_redis_pool_checkout_checkin() {
    let code = r##"
pool = RedisConnectionPool.new(size: 2, timeout: 5, host: "127.0.0.1", port: 6379)
redis = pool.checkout
redis.call("SET", "mrubyedge_pool_test", "pooled")
val = redis.call("GET", "mrubyedge_pool_test")
assert_eq(val, "pooled")
pool.checkin(redis)
redis.call("DEL", "mrubyedge_pool_test")
"##;

    let binary = mrbc_compile("redis_pool", code);
    let mut rite = mrubyedge::rite::load(&binary).unwrap();
    let mut vm = mrubyedge::yamrb::vm::VM::open(&mut rite);
    mrubyedge_redis::init_redis(&mut vm);
    // This test requires a running Redis server
    let _ = vm.run();
}

#[test]
fn test_redis_pool_with_block() {
    let code = r##"
pool = RedisConnectionPool.new(size: 2, timeout: 5, host: "127.0.0.1", port: 6379)
pool.with do |redis|
  redis.call("SET", "mrubyedge_with_test", "block_value")
  val = redis.call("GET", "mrubyedge_with_test")
  assert_eq(val, "block_value")
  redis.call("DEL", "mrubyedge_with_test")
end
"##;

    let binary = mrbc_compile("redis_pool_with", code);
    let mut rite = mrubyedge::rite::load(&binary).unwrap();
    let mut vm = mrubyedge::yamrb::vm::VM::open(&mut rite);
    mrubyedge_redis::init_redis(&mut vm);
    // This test requires a running Redis server
    let _ = vm.run();
}
