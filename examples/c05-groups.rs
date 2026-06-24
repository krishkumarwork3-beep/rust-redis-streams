use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let client = redis::Client::open("redis://127.0.0.1:6379")?;

	let stream_name = "stream-c05";

	// -- Writer Task
	let mut con_writer = client.get_multiplexed_async_connection().await?;
	let writer_handle = tokio::spawn(async move {
		println!("WRITER - started");
		for i in 0..10 {
			let id: String = con_writer
				.xadd(stream_name, "*", &[("val", &i.to_string())])
				.await
				.expect("XADD Fail");
			println!("WRITER - sent 'val: {i}' with id: {id}");
			sleep(Duration::from_millis(200)).await;
		}
	});

	// -- Create groups
	let group_01 = "group_01";
	create_group(&client, stream_name, group_01).await?;

	let group_02 = "group_02";
	create_group(&client, stream_name, group_02).await?;

	// -- Run consumers
	let consumer_g01_a_handle =
		run_consumer(
			&client,
			stream_name,
			group_01,
			"consumer_g01_a"
		)
		.await?;

	let consumer_g01_b_handle =
		run_consumer(
			&client,
			stream_name,
			group_01,
			"consumer_g01_b"
		)
		.await?;

	let consumer_g02_a_handle =
		run_consumer(
			&client,
			stream_name,
			group_02,
			"consumer_g02_a"
		)
		.await?;
	// -- Wait for the tasks
	writer_handle.await?;
	consumer_g01_a_handle.await?;
	consumer_g01_b_handle.await?;
	consumer_g02_a_handle.await?;

	// -- Clean up the stream
	let mut con = client.get_multiplexed_async_connection().await?;

	let count: i32 = con.del(stream_name).await?;

	println!(
		"Stream '{stream_name}' deleted ({count} key)."
	);

	Ok(())
}