use std::collections::BTreeMap;

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

    let mut thread_handles = Vec::with_capacity(files.len());

    for file in files {
        let file_clone = file.clone();
        let thread_handle = tokio::task::spawn(async move {
            let mapper_handle = mapper::HandleMapper::new();
            let wcs: BTreeMap<String, u32> = mapper_handle.process_file(file_clone).await;
            return wcs;
        });
        thread_handles.push(thread_handle);
    }

    let results = futures::future::join_all(thread_handles).await;
    for result in results {
        match result {
            Ok(wcs) => {
                println!("{:?}", wcs);
            }
            Err(_) => panic!("aw shiet"),
        }
    }

    Ok(())
}
