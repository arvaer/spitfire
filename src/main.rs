use tokio::sync::mpsc;
mod mapper;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let arg = args.get(1).expect("no argument given");
    let directory = std::path::Path::new(arg);
    let files = std::fs::read_dir(directory)?
        .map(|res| res.unwrap().path())
        .collect::<Vec<_>>();
    println!("files: {:?}", files);

    let (tx, mut rx) = mpsc::channel(100);

    tokio::spawn(async move {
        for i in files {
            if let Err(_) = tx.send(i).await {
                println!("receiver dropped");
                return;
            }
        }
    });

    while let Some(i) = rx.recv().await {
        println!("got = {:?}", i);
    }

    let handle = mapper::HandleMapper::new();
    let message_id = mapper::HandleMapper::get_unique_id(&handle).await;
    println!("message_id: {:?}", message_id);

    let next_message_id = mapper::HandleMapper::get_unique_id(&handle).await;
    println!("message_id: {:?}", next_message_id);

    Ok(())
}
