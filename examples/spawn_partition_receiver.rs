//! To run the example with logs printed to stdout, run the following command:
//!
//! ```bash
//! export EVENT_HUB_CONNECTION_STRING="Endpoint=sb://....servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...=;EntityPath=dmv"
//! export EVENT_HUB_CONSUMER_GROUP_NAME="rust"
//! export EVENT_HUB_NAME="dmv"
//! export EVENT_HUB_MAX_BATCH_SIZE_PER_CPU="50"
//! export EVENT_HUB_MAX_WAIT_TIME_SECONDS="60"
//! export EVENT_HUB_NUM_PARTITIONS="2"
//! export EVENT_HUB_PREFETCH_COUNT="3000"
//! export STREAM_FOR_SECONDS="300000"
//! export RUST_BACKTRACE=1 
//! export RUST_LOG=info
//! RUST_BACKTRACE=1 RUST_LOG=debug cargo run --example spawn_partition_receiver
//! ```

use azeventhubs::{
    consumer::EventPosition,
    primitives::PartitionReceiver,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

async fn partition_consumer_main(
    consumer_group: &str,
    partition_id: &str,
    connection_string: impl Into<String>,
    event_hub_name: impl Into<String>,
    prefetch_count: u32,
    max_wait_time_seconds: u64,
    max_batch_event_count: usize,
    cancel: CancellationToken,
) -> Result<(), azure_core::Error> {

    let mut receiver = PartitionReceiver::new_from_connection_string(
        consumer_group.into(),
        partition_id.into(),
        EventPosition::earliest(),
        connection_string.into(),
        event_hub_name.into(),
        azeventhubs::primitives::PartitionReceiverOptions {
            prefetch_count: prefetch_count,
            maximum_receive_wait_time: Duration::from_secs(max_wait_time_seconds),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                log::info!("{}: Cancelled", partition_id);
                break;
            },
            result = receiver.recv_batch(max_batch_event_count, Duration::from_secs(max_wait_time_seconds)) => {
                match result {
                    Ok(batch) => {
                        for event in batch {
                            let sequence_number = event.sequence_number();
                            let body = event.body()?;
                            let _value = std::str::from_utf8(body)?;
                            log::info!("{}: {:?}", partition_id, sequence_number);
                        }
                    },
                    Err(e) => {
                        log::error!("{}: {:?}", partition_id, e);
                    }
                }
            }
        }
    }

    receiver.close().await.unwrap();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::init();

    let connection_string = std::env::var("EVENT_HUB_CONNECTION_STRING")?;
    let consumer_group_name = std::env::var("EVENT_HUB_CONSUMER_GROUP_NAME")?;
    let event_hub_name = std::env::var("EVENT_HUB_NAME")?;
    let max_batch_per_cpu: u32 = std::env::var("EVENT_HUB_MAX_BATCH_SIZE_PER_CPU").unwrap_or_else(|_| "50".to_string()).parse().unwrap();
    let max_wait_time_seconds: u32 = std::env::var("EVENT_HUB_MAX_WAIT_TIME_SECONDS").unwrap_or_else(|_| "60".to_string()).parse().unwrap();
    let num_cpus = std::thread::available_parallelism().unwrap().get();
    let num_partitions: usize = std::env::var("EVENT_HUB_NUM_PARTITIONS").unwrap_or_else(|_| "3".to_string()).parse().unwrap();
    let prefetch_count: u32 = std::env::var("EVENT_HUB_PREFETCH_COUNT").unwrap_or_else(|_| "300".to_string()).parse().unwrap();

    let stream_duration_in_seconds: u64 = std::env::var("STREAM_FOR_SECONDS").unwrap_or_else(|_| "1000".to_string()).parse().unwrap();

    log::info!("Connection string: {}", connection_string);
    log::info!("Consumer group name: {}", consumer_group_name);
    log::info!("Event hub name: {}", event_hub_name);
    log::info!("Max batch per CPU: {}", max_batch_per_cpu);
    log::info!("Max batch wait time (seconds): {}", max_wait_time_seconds);
    log::info!("Number of CPUs available: {}", num_cpus);
    log::info!("Number of partitions: {}", num_partitions);
    log::info!("Prefetch count: {}", prefetch_count);
    log::info!("Stream duration in seconds: {}", stream_duration_in_seconds);

    // Create one receiver for each partition
    //
    let cancel = CancellationToken::new();
    let mut handles = Vec::new();
    for i in 0..num_partitions {
        let handle = tokio::spawn({
            let connection_string = connection_string.clone();
            let event_hub_name = event_hub_name.clone();
            let consumer_group_name = consumer_group_name.clone();
            let cancel = cancel.child_token();
            async move {
                partition_consumer_main(
                    &consumer_group_name,
                    &i.to_string(),
                    connection_string,
                    event_hub_name,
                    prefetch_count,
                    max_wait_time_seconds as u64,
                    (max_batch_per_cpu * num_cpus as u32) as usize,
                    cancel,
                ).await
            }
        });
        handles.push(handle);
    }

    // Wait and  cancel the spawned tasks
    //
    tokio::time::sleep(Duration::from_secs(stream_duration_in_seconds)).await;
    cancel.cancel();
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
