mod pool;
mod redis;
mod value;

use mrubyedge::yamrb::vm::VM;

/// Initialize the Redis and RedisConnectionPool classes in the VM.
/// Call this after `VM::open` to make Redis classes available in Ruby code.
pub fn init_redis(vm: &mut VM) {
    redis::init_redis_class(vm);
    pool::init_pool_class(vm);
}
