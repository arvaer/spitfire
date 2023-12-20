use std::collections::HashMap;

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



    for file in files{
        let handle = mapper::HandleMapper::new();
        println!("{}", file.display());
        let wcs: HashMap<String, u32> = handle.process_file(file).await;
        println!("{:?}", wcs);

    }




    Ok(())

}
