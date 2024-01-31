use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv_store::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    // Connect to the server
    let stream = TcpStream::connect(addr).await?;

    // Use AsyncProstStream to handle TCP Frame
    let mut client = AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    // Generate a HSET command
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());

    // Send HSET command
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await {
        info!("Got response {:?}", data);
    }

    Ok(())
}

// use kv_store::command_request::RequestData;

// if let Some(cr) = cmd.request_data {
//     println!("{:?}", cr);
//     if let RequestData::Hset(hset) = cr {
//         println!("{:?}", hset.pair);
//         if let Some(t) = hset.pair {
//             println!("{:?}",t.value);
//             println!("{:?}",t.key);
//         }
//         println!("{:?}", hset.table);
//     }
// }