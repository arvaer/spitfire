#![allow(dead_code)]
mod mapper;
mod reducer;
use std::future::IntoFuture;

use std::collections::HashMap;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // ------------------ MAPPER ------------------
    let args: Vec<String> = std::env::args().collect();
    let arg = args.get(1).expect("no argument given");
    let directory = std::path::Path::new(arg);
    let mut files = std::fs::read_dir(directory)?
        .map(|res| res.unwrap().path())
        .collect::<Vec<_>>();
    println!("files: {:?}", files);

    let mut unoccupied_mappers: HashMap<usize, mapper::HandleMapper> =
        HashMap::with_capacity(num_cpus::get());
    let mut occupied_mappers: HashMap<usize, mapper::HandleMapper> =
        HashMap::with_capacity(num_cpus::get());

    for id in 0..num_cpus::get() {
        unoccupied_mappers.insert(id, mapper::HandleMapper::new());
    }

    //i will keep two seperate queues, an occupied queue and an unoccupied queue.
    //in the main loop of the program, i will constantly assign files to be processed
    //from mappers in the unoccupied queue.
    let mut counter = 0;
    loop {
        println!("counter: {}", counter);
        let mut tasks = Vec::new();
        if files.is_empty() {
            break;
        }
        while let Some((id, mapper)) = unoccupied_mappers
            .iter()
            .next()
            .map(|(id, mapper)| (*id, mapper.clone()))
        {
            if let Some(file) = files.pop() {
                unoccupied_mappers.remove(&id);
                occupied_mappers.insert(id, mapper.clone());
                let handle = tokio::spawn(async move {
                    let message_id: usize = mapper.process_file_with_buffer(file).await;
                    return (message_id, id);
                });
                tasks.push(handle);
            } else {
                break;
            }
        }
        for task in tasks.iter_mut() {
            let future_results = task.into_future().await;
            match future_results {
                Ok((_, mapper_id)) => {
                    if let Some(free_worker) = occupied_mappers.remove(&mapper_id) {
                        unoccupied_mappers.insert(mapper_id, free_worker);
                    }
                }
                Err(e) => {
                    eprintln!("Task failed with error :{}", e)
                }
            };
        }
        tasks.clear();
        if files.is_empty() && tasks.is_empty() {
            for (_, mapper) in unoccupied_mappers.iter() {
                mapper.cleanup_signal().await;
            }
            break;
        }

        counter += 1;
    }
    drop(unoccupied_mappers);
    drop(occupied_mappers);

    // ------------------ REDUCER ------------------

    let mut files = std::fs::read_dir("./tmp")?
        .map(|res| res.unwrap().path())
        .collect::<Vec<_>>();
    println!("files: {:?}", files);

    let mut unoccupied_mappers: HashMap<usize, reducer::HandleReducer> =
        HashMap::with_capacity(num_cpus::get());
    let mut occupied_mappers: HashMap<usize, reducer::HandleReducer> =
        HashMap::with_capacity(num_cpus::get());

    for id in 0..num_cpus::get() {
        unoccupied_mappers.insert(id, reducer::HandleReducer::new());
    }

    Ok(())
}
