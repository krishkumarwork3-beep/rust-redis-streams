use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:6379")?;

    let stream_name = "stream-c04";

    // -- Writer Task
    let mut con_writer = client.get_multiplexed_async_connection().await?;

    let writer_handle = tokio::spawn(async move {
        println!("WRITER - started");

        for i in 0..10 {
            let id: String = con_writer
                .xadd(
                    stream_name,
                    "*",
                    &[("val", &i.to_string())]
                )
                .await
                .expect("XADD Fail");

            println!(
                "WRITER - sent 'val: {i}' with id: {id}"
            );

            sleep(Duration::from_millis(200)).await;
        }
    });

    // -- Create a group
    let group_01 = "group_01";

    let mut con_group_01 =
        client.get_multiplexed_async_connection().await?;

    let group_create_res: Result<(), _> =
        con_group_01
            .xgroup_create_mkstream(
                stream_name,
                group_01,
                "0"
            )
            .await;

    if let Err(err) = group_create_res {
        if let Some("BUSYGROUP") = err.code() {
            println!(
                "XGROUP - group '{group_01}' ALREADY created"
            );
        } else {
            return Err(err.into());
        }
    } else {
        println!(
            "XGROUP - group '{group_01}' created"
        );
    }