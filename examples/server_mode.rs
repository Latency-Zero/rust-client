use latzero::{Client, ProcessOptions};
use serde_json::{Map, Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let worker = Client::connect("latzero://rust-worker", "rust-example").await?;
    worker
        .on_event("add", |data: Map<String, Value>| async move {
            Ok::<_, String>(data["x"].as_i64().unwrap() + data["y"].as_i64().unwrap())
        })
        .await;
    let process = worker
        .register_process(
            "multiply",
            ProcessOptions::default(),
            |data: Map<String, Value>| async move {
                Ok::<_, String>(data["x"].as_i64().unwrap() * data["y"].as_i64().unwrap())
            },
        )
        .await?;

    let caller = Client::connect("latzero://rust-caller", "rust-example").await?;
    caller.set("greeting", &"hello from Rust").await?;
    let greeting: Option<String> = caller.get("greeting").await?;
    println!("buffer: {greeting:?}");

    let sum: i64 = caller
        .call_app("rust-worker", "add", &json!({"x": 3, "y": 4}))
        .await?;
    let product: i64 = caller
        .call_process(&process.process_id, &json!({"x": 6, "y": 7}))
        .await?;
    println!("sum={sum}, product={product}");

    caller.disconnect().await?;
    worker.disconnect().await
}
