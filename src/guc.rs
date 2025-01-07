use pgrx::*;

pub static AUTO_EXPLAIN_LOG_MIN_DURATION: GucSetting<i32> = GucSetting::<i32>::new(-1);
pub static AUTO_EXPLAIN_LOG_PARAMETER_MAX_LENGTH: GucSetting<i32> = GucSetting::<i32>::new(-1);
pub static AUTO_EXPLAIN_LOG_ANALYZE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_VERBOSE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_BUFFERS: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_WAL: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_TRIGGERS: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_TIMING: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_SETTINGS: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_LOG_FORMAT: GucSetting<ExplainFormat> = GucSetting::<ExplainFormat>::new(ExplainFormat::TEXT);
pub static AUTO_EXPLAIN_LOG_LEVEL: GucSetting<GucLogLevel> = GucSetting::<GucLogLevel>::new(GucLogLevel::LOG);
pub static AUTO_EXPLAIN_LOG_NESTED_STATEMENTS: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static AUTO_EXPLAIN_SAMPLE_RATE: GucSetting<f64> = GucSetting::<f64>::new(1.0);

#[derive(PostgresGucEnum, Clone, Copy, PartialEq, Debug)]
pub enum ExplainFormat {
	TEXT,
	XML,
	JSON,
	YAML,
}

impl ExplainFormat {
    pub fn as_u32(self) -> u32 {
        match self {
            ExplainFormat::TEXT => 0,
            ExplainFormat::XML => 1,
            ExplainFormat::JSON => 2,
            ExplainFormat::YAML => 3,

        }
    }
}

#[derive(PostgresGucEnum, Clone, Copy, PartialEq, Debug)]
pub enum GucLogLevel {
    DEBUG5 = PgLogLevel::DEBUG5 as isize,
    DEBUG4 = PgLogLevel::DEBUG4 as isize,
    DEBUG3 = PgLogLevel::DEBUG3 as isize,
    DEBUG1 = PgLogLevel::DEBUG1 as isize,
    DEBUG = PgLogLevel::DEBUG2 as isize,
    INFO = PgLogLevel::INFO as isize,
    NOTICE = PgLogLevel::NOTICE as isize,
    WARNING = PgLogLevel::WARNING as isize,
    LOG = PgLogLevel::LOG as isize,
    NULL = 0,
}

impl GucLogLevel {
    pub fn log_level(self) -> PgLogLevel {
        match self {
            GucLogLevel::DEBUG => PgLogLevel::DEBUG2,
            GucLogLevel::DEBUG1 => PgLogLevel::DEBUG1,
            GucLogLevel::DEBUG3 => PgLogLevel::DEBUG3,
            GucLogLevel::DEBUG4 => PgLogLevel::DEBUG4,
            GucLogLevel::DEBUG5 => PgLogLevel::DEBUG5,
            GucLogLevel::INFO => PgLogLevel::INFO,
            GucLogLevel::NOTICE => PgLogLevel::NOTICE,
            GucLogLevel::WARNING => PgLogLevel::WARNING,
            GucLogLevel::LOG => PgLogLevel::LOG,
            GucLogLevel::NULL => (0 as isize).into(),
        }
    }

    pub fn errcode(self) -> PgSqlErrorCode {
        match self {
            GucLogLevel::DEBUG => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::DEBUG1 => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::DEBUG3 => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::DEBUG4 => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::DEBUG5 => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::INFO => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::NOTICE => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION ,
            GucLogLevel::WARNING => PgSqlErrorCode::ERRCODE_WARNING ,
            GucLogLevel::LOG => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            GucLogLevel::NULL => PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        }
    }
}


pub static mut NESTING_LEVEL: i32 = 0;
pub static mut CURRENT_QUERY_SAMPLED: bool = false;

pub fn auto_explain_enabled() -> bool{
    AUTO_EXPLAIN_LOG_MIN_DURATION.get() >= 0 && 
    unsafe { NESTING_LEVEL } == 0 && 
    AUTO_EXPLAIN_LOG_NESTED_STATEMENTS.get() && 
    unsafe { CURRENT_QUERY_SAMPLED }
}

pub fn init() {
    GucRegistry::define_int_guc(
        "auto_explain_rs.log_min_duration",
        "Sets the minimum execution time above which plans will be logged.",
		"Zero prints all plans. -1 turns this feature off.",
        &AUTO_EXPLAIN_LOG_MIN_DURATION,
        -1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "auto_explain_rs.log_parameter_max_length",
        "Sets the maximum length of query parameters to log.",
        "Zero logs no query parameters, -1 logs them in full.",
        &AUTO_EXPLAIN_LOG_PARAMETER_MAX_LENGTH,
        -1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_analyze",
        "Use EXPLAIN ANALYZE for plan logging.",
        "",
        &AUTO_EXPLAIN_LOG_ANALYZE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_settings",
        "Log modified configuration parameters affecting query planning.",
        "",
        &AUTO_EXPLAIN_LOG_SETTINGS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_verbose",
        "Use EXPLAIN VERBOSE for plan logging.",
        "",
        &AUTO_EXPLAIN_LOG_VERBOSE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_buffers",
        "Log buffers usage.",
        "",
        &AUTO_EXPLAIN_LOG_BUFFERS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_wal",
        "Log WAL usage.",
        "",
        &AUTO_EXPLAIN_LOG_WAL,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_triggers",
        "Include trigger statistics in plans.",
        "This has no effect unless log_analyze is also set.",
        &AUTO_EXPLAIN_LOG_TRIGGERS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_enum_guc(
        "auto_explain_rs.log_format",
        "EXPLAIN format to be used for plan logging.",
        "",
        &AUTO_EXPLAIN_LOG_FORMAT,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_enum_guc(
        "auto_explain_rs.log_level",
        "Log level for the plan.",
        "",
        &AUTO_EXPLAIN_LOG_LEVEL,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_nested_statements",
        "Log nested statements.",
        "",
        &AUTO_EXPLAIN_LOG_NESTED_STATEMENTS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "auto_explain_rs.log_timing",
        "Collect timing data, not just row counts.",
        "",
        &AUTO_EXPLAIN_LOG_TIMING,
        GucContext::Suset,
        GucFlags::default(),
    );

    
    GucRegistry::define_float_guc(
        "auto_explain_rs.sample_rate",
        "Fraction ofqueries to process.",
        "",
        &AUTO_EXPLAIN_SAMPLE_RATE,
        0.0,
        f64::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    // pgrx does not implement MarkGUCPrefixReserved() yet
    // MarkGUCPrefixReserved("auto_explain_rs");
}
