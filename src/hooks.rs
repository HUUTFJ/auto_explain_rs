use std::ffi::CStr;

use pgrx::{ereport, pg_sys::{pg_global_prng_state, pg_prng_double, standard_ExecutorEnd, standard_ExecutorFinish, standard_ExecutorRun, standard_ExecutorStart, ExecutorEnd_hook_type, ExecutorFinish_hook_type, ExecutorRun_hook_type, ExecutorStart_hook_type, ExplainBeginOutput, ExplainEndOutput, ExplainFormat::EXPLAIN_FORMAT_JSON, ExplainPrintJITSummary, ExplainPrintPlan, ExplainPrintTriggers, ExplainQueryParameters, ExplainQueryText, InstrAlloc, InstrEndLoop, InstrumentOption::*, MemoryContextSwitchTo, NewExplainState, ParallelWorkerNumber, QueryDesc, ScanDirection, EXEC_FLAG_EXPLAIN_ONLY}, PgTryBuilder};
use crate::guc;

/* Saved hook values */
pub static mut PREV_EXECUTOR_START: ExecutorStart_hook_type = None;
pub static mut PREV_EXECUTOR_RUN: ExecutorRun_hook_type = None;
pub static mut PREV_EXECUTOR_FINISH: ExecutorFinish_hook_type = None;
pub static mut PREV_EXECUTOR_END: ExecutorEnd_hook_type = None;

pub fn is_parallel_worker() -> bool {
    unsafe {
        ParallelWorkerNumber >= 0
    }
}


#[pgrx::pg_guard]
unsafe extern "C" fn explain_executor_start(query_desc: *mut QueryDesc, eflags: i32) {
    if (guc::NESTING_LEVEL == 0) {
        if (guc::AUTO_EXPLAIN_LOG_MIN_DURATION.get() >= 0 && !is_parallel_worker()) {
            guc::CURRENT_QUERY_SAMPLED = pg_prng_double(&mut pg_global_prng_state) < guc::AUTO_EXPLAIN_SAMPLE_RATE.get();
        }else {
            guc::CURRENT_QUERY_SAMPLED = false;
        }
    }

    if (guc::auto_explain_enabled()) {
        if (guc::AUTO_EXPLAIN_LOG_ANALYZE.get() && (eflags as u32 & EXEC_FLAG_EXPLAIN_ONLY) == 0) {
            if (guc::AUTO_EXPLAIN_LOG_TIMING.get()) {
                (*query_desc).instrument_options |= INSTRUMENT_TIMER as i32;
            }else {
                (*query_desc).instrument_options |= INSTRUMENT_ROWS as i32;
            }
            if (guc::AUTO_EXPLAIN_LOG_BUFFERS.get()) {
                (*query_desc).instrument_options |= INSTRUMENT_BUFFERS as i32;
            }
            if (guc::AUTO_EXPLAIN_LOG_WAL.get()) {
                (*query_desc).instrument_options |= INSTRUMENT_WAL as i32;
            }
        }
    }

    if let Some(prev_executor_start) = PREV_EXECUTOR_START {
        prev_executor_start(query_desc, eflags);
    }else {
        standard_ExecutorStart(query_desc, eflags);
    }

    if(guc::auto_explain_enabled()) {
        if ((*query_desc).totaltime.is_null()) {
            let old_cxt = MemoryContextSwitchTo((*(*query_desc).estate).es_query_cxt);
            (*query_desc).totaltime = InstrAlloc(1, INSTRUMENT_ALL.try_into().unwrap(), false);
            MemoryContextSwitchTo(old_cxt);
        }
    }

}



unsafe extern "C" fn explain_executor_run(query_desc: *mut QueryDesc, direction: ScanDirection::Type, count: u64, execute_once: bool){
    guc::NESTING_LEVEL += 1;
    PgTryBuilder::new(|| {
        if let Some(prev_executor_run) = PREV_EXECUTOR_RUN {
            prev_executor_run(query_desc, direction, count, execute_once);
        }else {
            standard_ExecutorRun(query_desc, direction, count, execute_once);
        }
    })
    .finally(|| guc::NESTING_LEVEL -= 1)
    .execute()
}

unsafe extern "C" fn explain_executor_finish(query_desc: *mut QueryDesc){
    guc::NESTING_LEVEL += 1;
    PgTryBuilder::new(|| {
        if let Some(prev_executor_finish) = PREV_EXECUTOR_FINISH {
            prev_executor_finish(query_desc);
        }else {
            standard_ExecutorFinish(query_desc);
        }
    })
    .finally(|| guc::NESTING_LEVEL -= 1)
    .execute()

}

unsafe extern "C" fn explain_executor_end(query_desc: *mut QueryDesc){
    if let Some(totaltime) = unsafe { (*query_desc).totaltime.as_ref()} {
        let old_cxt = MemoryContextSwitchTo((*(*query_desc).estate).es_query_cxt);
        InstrEndLoop((*query_desc).totaltime);

        let msec = (*(*query_desc).totaltime).total * 1000.0;
        if (msec >= guc::AUTO_EXPLAIN_LOG_MIN_DURATION.get().into()) {
            let es = NewExplainState();
            (*es).analyze = (*query_desc).instrument_options != 0 && guc::AUTO_EXPLAIN_LOG_ANALYZE.get();
            (*es).verbose = guc::AUTO_EXPLAIN_LOG_VERBOSE.get();
            (*es).buffers = (*es).analyze && guc::AUTO_EXPLAIN_LOG_BUFFERS.get();
            (*es).wal = (*es).analyze && guc::AUTO_EXPLAIN_LOG_WAL.get();
            (*es).timing = (*es).analyze && guc::AUTO_EXPLAIN_LOG_TIMING.get();
            (*es).summary = (*es).analyze;
            (*es).format = guc::AUTO_EXPLAIN_LOG_FORMAT.get().as_u32();
            (*es).settings = guc::AUTO_EXPLAIN_LOG_SETTINGS.get();

            ExplainBeginOutput(es);
            ExplainQueryText(es, query_desc);
            ExplainQueryParameters(es, (*query_desc).params, guc::AUTO_EXPLAIN_LOG_PARAMETER_MAX_LENGTH.get());
            ExplainPrintPlan(es, query_desc);
            if ((*es).analyze && guc::AUTO_EXPLAIN_LOG_TRIGGERS.get()) {
                ExplainPrintTriggers(es, query_desc);
            }
            if ((*es).costs) {
                ExplainPrintJITSummary(es, query_desc);
            }
            ExplainEndOutput(es);

            let c_str_data = CStr::from_ptr((*(*es).str_).data);
            let c_str_bytes = c_str_data.to_bytes();
            if ((*(*es).str_).len > 0 && c_str_bytes[c_str_bytes.len() -1] == b'\n') {
                let ptr_to_newline = (*(*es).str_).data.add(c_str_bytes.len() - 1);
                *ptr_to_newline = b'\0' as std::ffi::c_char;
            }

            if (guc::AUTO_EXPLAIN_LOG_FORMAT.get().as_u32() == EXPLAIN_FORMAT_JSON) {
                let ptr_to_newline = (*(*es).str_).data.add(0);
                *ptr_to_newline = b'{' as std::ffi::c_char;
                let ptr_to_newline = (*(*es).str_).data.add(c_str_bytes.len() - 1);
                *ptr_to_newline = b'}' as std::ffi::c_char;
                
            }

			/*
			 * Note: we rely on the existing logging of context or
			 * debug_query_string to identify just which statement is being
			 * reported.  This isn't ideal but trying to do it here would
			 * often result in duplication.
			 */
             ereport!(
                guc::AUTO_EXPLAIN_LOG_LEVEL.get().log_level(),
                guc::AUTO_EXPLAIN_LOG_LEVEL.get().errcode(),
                format!("duration: {:.3} ms  plan:\n{}", msec, c_str_data.to_string_lossy()));
        }
        MemoryContextSwitchTo(old_cxt);
    }


    if let Some(prev_executor_end) = PREV_EXECUTOR_END {
        prev_executor_end(query_desc);
    }else {
        standard_ExecutorEnd(query_desc);
    }
}

pub unsafe fn init() {
    unsafe {
        /* Install hooks. */
        PREV_EXECUTOR_START = pgrx::pg_sys::ExecutorStart_hook;
        pgrx::pg_sys::ExecutorStart_hook = Some(explain_executor_start);
        PREV_EXECUTOR_RUN = pgrx::pg_sys::ExecutorRun_hook;
        pgrx::pg_sys::ExecutorRun_hook = Some(explain_executor_run);
        PREV_EXECUTOR_FINISH = pgrx::pg_sys::ExecutorFinish_hook;
        pgrx::pg_sys::ExecutorFinish_hook = Some(explain_executor_finish);
        PREV_EXECUTOR_END = pgrx::pg_sys::ExecutorEnd_hook;
        pgrx::pg_sys::ExecutorEnd_hook = Some(explain_executor_end);
    }
}
