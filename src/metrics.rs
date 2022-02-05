use stable_eyre::eyre::Result;
use prometheus::register_counter_vec;
use prometheus::register_int_gauge;
use prometheus::register_int_gauge_vec;
use prometheus::CounterVec;
use prometheus::IntGauge;
use prometheus::IntGaugeVec;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

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

pub fn create_metrics_endpoint(metrics_port: u16) {
    tokio::spawn(async move {
        warp::serve(warp::path!("metrics").and_then(metrics_handler))
            .run(([0, 0, 0, 0], metrics_port))
            .await;
    });
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        log::warn!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        log::warn!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    Ok(res)
}
