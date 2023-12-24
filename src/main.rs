mod mapper;
mod reducer;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let arg = args.get(1).expect("no argument given");
    let directory = std::path::Path::new(arg);
    let files = std::fs::read_dir(directory)?
        .map(|res| res.unwrap().path())
        .collect::<Vec<_>>();
    println!("files: {:?}", files);

    let mut thread_handles = Vec::with_capacity(files.len());

    for file in files {
        let file_clone = file.clone();
        let thread_handle: tokio::task::JoinHandle<(mapper::HandleMapper, String)> = tokio::task::spawn(async move {
            let mapper_handle = mapper::HandleMapper::new();
           // let wcs: BTreeMap<String, u32> = mapper_handle.process_file(file_clone).await;
            //return wcs;
            let processed_file_name = mapper_handle.process_file_with_buffer(file_clone).await;
            return (mapper_handle, processed_file_name);
        });
        thread_handles.push(thread_handle);
    }


    let results = futures::future::join_all(thread_handles).await;
    for result in results {
        match result {
            Ok((handle, file)) => {
                handle.cleanup_signal().await;
                println!("{:?}", file);
            }
            Err(_) => panic!("aw shiet"),
        }
    }

    Ok(())
}
