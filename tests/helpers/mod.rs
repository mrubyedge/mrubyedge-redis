#![allow(dead_code)]

use std::rc::Rc;
use std::{ffi::CStr, fs::File, io::Write};

use mrubyedge::yamrb::helpers::{mrb_call_inspect, mrb_define_cmethod};
use mrubyedge::yamrb::value::RObject;
use mrubyedge::yamrb::vm::VM;
use mrubyedge::Error;

/// Define `Object#assert_eq(expected)` in the VM.
/// Raises RuntimeError if `self != expected`.
pub fn define_assert_eq(vm: &mut VM) {
    let object_class = vm.get_class_by_name("Object");

    mrb_define_cmethod(
        vm,
        object_class,
        "assert_eq",
        Box::new(
            |vm: &mut VM, args: &[Rc<RObject>]| -> Result<Rc<RObject>, Error> {
                if args.len() < 2 {
                    return Err(Error::ArgumentError(
                        "assert_eq requires 2 arguments".to_string(),
                    ));
                }
                let expected = &args[0];
                let got = &args[1];

                let expected_inspect = mrb_call_inspect(vm, expected.clone())?;
                let got_inspect = mrb_call_inspect(vm, got.clone())?;

                let expected_str: String = expected_inspect.as_ref().try_into()?;
                let got_str: String = got_inspect.as_ref().try_into()?;

                if expected_str != got_str {
                    return Err(Error::RuntimeError(format!(
                        "assertion failed: expected {}, got {}",
                        expected_str, got_str
                    )));
                }

                Ok(RObject::nil().to_refcount_assigned())
            },
        ),
    );
}

pub fn mrbc_compile(fname: &'static str, code: &str) -> Vec<u8> {
    let mut src = std::env::temp_dir();
    src.push(format!("{}.{}.rb", fname, std::process::id()));
    let mut f = File::create(&src).expect("cannot open src file");
    f.write_all(code.as_bytes())
        .expect("cannot create src file");
    f.flush().unwrap();

    let mut src0 = src.as_os_str().to_string_lossy().into_owned();
    src0.push('\0');

    let mut dest = std::env::temp_dir();
    dest.push(format!("{}.{}.mrb", fname, std::process::id()));
    let mut dest0 = dest.as_os_str().to_string_lossy().into_owned();
    dest0.push('\0');

    let args = [
        c"mrbc".as_ptr(),
        c"-o".as_ptr(),
        CStr::from_bytes_with_nul(dest0.as_bytes())
            .unwrap()
            .as_ptr(),
        CStr::from_bytes_with_nul(src0.as_bytes()).unwrap().as_ptr(),
    ];
    unsafe {
        mec_mrbc_sys::mrbc_main(args.len() as i32, args.as_ptr() as *mut *mut i8);
    }

    std::fs::read(dest).unwrap()
}
