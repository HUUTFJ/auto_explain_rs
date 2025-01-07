mod hooks;
mod guc;

::pgrx::pg_module_magic!();


#[pgrx::pg_guard]
pub extern "C" fn _pg_init() {
    self::guc::init();
    unsafe { self::hooks::init() };
}