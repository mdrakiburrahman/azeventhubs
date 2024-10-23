//! To run the example with logs printed to stdout, run the following command:
//!
//! ```bash
//! export EVENT_HUB_CONNECTION_STRING="Endpoint=sb://....servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...=;EntityPath=dmv"
//! export EVENT_HUB_NAME="dmv"
//! export EVENT_HUB_NUM_PARTITIONS="100"
//! export EVENT_HUB_PREFETCH_COUNT="3000"
//! export STREAM_FOR_SECONDS="3000"
//! export RUST_BACKTRACE=1
//! export RUST_LOG=debug
//! RUST_BACKTRACE=1 RUST_LOG=debug cargo run --example spawn_multiple_consumer
//! ```

use std::time::Duration;

use azeventhubs::consumer::{
    EventHubConsumerClient, EventHubConsumerClientOptions, EventPosition, ReadEventOptions,
};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

async fn consumer_main(
    partition_num: usize,
    prefetch_count: u32,
    connection_string: impl Into<String>,
    event_hub_name: impl Into<String>,
    client_options: EventHubConsumerClientOptions,
    cancel: CancellationToken,
) -> Result<(), azure_core::Error> {
    let mut consumer = EventHubConsumerClient::new_from_connection_string(
        EventHubConsumerClient::DEFAULT_CONSUMER_GROUP_NAME,
        connection_string,
        event_hub_name.into(),
        client_options,
    )
    .await?;
    let partition_ids = consumer.get_partition_ids().await?;
    let partition_id = &partition_ids[partition_num];
    let starting_position = EventPosition::earliest();
    let read_event_options = ReadEventOptions::default().with_prefetch_count(prefetch_count);

    let mut stream = consumer
        .read_events_from_partition(partition_id, starting_position, read_event_options)
        .await?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                log::info!("{}: Cancelled", partition_id);
                break;
            },
            event = stream.next() => {
                match event {
                    Some(Ok(event)) => {
                        let sequence_number = event.sequence_number();
                        let body = event.body()?;
                        let _value = std::str::from_utf8(body)?;
                        log::info!("{}: {:?}", partition_id, sequence_number);
                    },
                    Some(Err(e)) => {
                        log::error!("{}: {:?}", partition_id, e);
                    },
                    None => {
                        log::info!("{}: Stream closed", partition_id);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let connection_string = std::env::var("EVENT_HUB_CONNECTION_STRING")?;
    let event_hub_name = std::env::var("EVENT_HUB_NAME")?;
    let num_partitions: usize = std::env::var("EVENT_HUB_NUM_PARTITIONS")
        .unwrap_or_else(|_| "3".to_string())
        .parse()
        .unwrap();
    let stream_duration_in_seconds: u64 = std::env::var("STREAM_FOR_SECONDS")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap();
    let prefetch_count: u32 = std::env::var("EVENT_HUB_PREFETCH_COUNT")
        .unwrap_or_else(|_| "300".to_string())
        .parse()
        .unwrap();

    log::info!("Connection string: {}", connection_string);
    log::info!("Event hub name: {}", event_hub_name);
    log::info!("Number of partitions: {}", num_partitions);
    log::info!("Stream duration in seconds: {}", stream_duration_in_seconds);
    log::info!("Pre-fetch count: {}", prefetch_count);

    let client_options = EventHubConsumerClientOptions::default();

    // Cancellation token to stop the spawned tasks.
    let cancel = CancellationToken::new();

    // Create one consumer for each partition.
    let mut handles = Vec::new();
    for i in 0..num_partitions {
        let handle = tokio::spawn(consumer_main(
            i,
            prefetch_count,
            connection_string.clone(),
            event_hub_name.clone(),
            client_options.clone(),
            cancel.child_token(),
        ));
        handles.push(handle);
    }

    // Wait and  cancel the spawned tasks.
    tokio::time::sleep(Duration::from_secs(stream_duration_in_seconds)).await;
    cancel.cancel();
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
