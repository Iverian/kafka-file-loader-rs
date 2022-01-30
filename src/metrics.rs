use prometheus::register_counter_vec;
use prometheus::register_int_gauge;
use prometheus::register_int_gauge_vec;
use prometheus::CounterVec;
use prometheus::IntGauge;
use prometheus::IntGaugeVec;

lazy_static! {
    pub static ref QUEUE_SIZE: IntGauge =
        register_int_gauge!("file_loader_queue_size", "Queue Size").unwrap();
    pub static ref STREAMS: IntGaugeVec =
        register_int_gauge_vec!("file_loader_stream", "Stream Info", &["stream_name"]).unwrap();
    pub static ref FILES: IntGaugeVec = register_int_gauge_vec!(
        "file_loader_processed_files",
        "Processed Files",
        &["stream_name"]
    )
    .unwrap();
    pub static ref RECORDS: CounterVec = register_counter_vec!(
        "file_loader_processed_records",
        "Records Processed",
        &["stream_name", "type"]
    )
    .unwrap();
}
