use file_lock::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::sync::{mpsc, oneshot, Mutex};
mod helpers;
use crate::writer;
use crate::writer::Request;
use crate::writer::WriterHandle;

pub struct Mapper {
    receiver: mpsc::Receiver<MapperMessage>,
    message_id: Mutex<usize>,
    internal_buffer: Mutex<Vec<BTreeMap<String, u32>>>,
    writer_handle: writer::WriterHandle,
}

pub enum MapperMessage {
    GetId {
        respond_to: oneshot::Sender<usize>,
    },
    Cleanup {
        respond_to: oneshot::Sender<String>,
    },
    ProcessFileTest {
        filename: PathBuf,
        respond_to: oneshot::Sender<String>,
    },
    ProcessSingleFile {
        filename: PathBuf,
        respond_to: oneshot::Sender<BTreeMap<String, u32>>,
    },
    ProcessFileWithBuffer {
        filename: PathBuf,
        respond_to: oneshot::Sender<usize>,
    },
}

impl Mapper {
    fn new(receiver: mpsc::Receiver<MapperMessage>, writer: writer::WriterHandle) -> Self {
        Mapper {
            receiver,
            message_id: Mutex::new(0),
            internal_buffer: Mutex::new(Vec::new()),
            writer_handle: writer,
        }
    }

    async fn handle_message(&mut self, msg: MapperMessage) {
        match msg {
            MapperMessage::GetId { respond_to } => {
                let mut guard = self.message_id.lock().await;
                *guard += 1;
                let _ = respond_to.send(*guard);
                drop(guard);
            }
            MapperMessage::Cleanup { respond_to } => {
                drain_internal_buffer(self).await;
                let _ = respond_to.send(String::from("Cleanup Finished"));
            }
            MapperMessage::ProcessFileTest {
                filename,
                respond_to,
            } => {
                let mut file = File::open(&filename).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();
                let _ = respond_to.send(contents);
            }
            MapperMessage::ProcessSingleFile {
                filename,
                respond_to,
            } => {
                let file = File::open(&filename).unwrap();
                let reader = BufReader::new(file);
                let mut wordcount: BTreeMap<String, u32> = BTreeMap::new();

                for line in reader.lines().filter_map(Result::ok) {
                    for word in line.split_ascii_whitespace() {
                        *wordcount
                            .entry(String::from(word).to_lowercase())
                            .or_insert(0) += 1;
                    }
                }

                let _ = respond_to.send(wordcount);
            }
            MapperMessage::ProcessFileWithBuffer {
                filename,
                respond_to,
            } => {
                let mut guard = self.message_id.lock().await;
                *guard += 1;
                let message_id = guard.clone();
                if *guard % 10 == 0 {
                    drop(guard);
                    drain_internal_buffer(self).await;
                }
                let file = File::open(&filename).unwrap();
                let reader = BufReader::new(file);
                let mut wordcount: BTreeMap<String, u32> = BTreeMap::new();

                for line in reader.lines().filter_map(Result::ok) {
                    for word in line.split_ascii_whitespace() {
                        *wordcount
                            .entry(String::from(word).to_lowercase())
                            .or_insert(0) += 1;
                    }
                }

                let mut guard = self.internal_buffer.lock().await;
                guard.push(wordcount);
                drop(guard);

                let _ = respond_to.send(message_id);
            }
        }
    }
}

async fn drain_internal_buffer(mapper: &mut Mapper) {
    let mut guard = Mutex::lock(&mapper.internal_buffer).await;
    println!("Lock aquired in drain buffer");
    let internal_buffer = guard.deref_mut();
    for buffer in internal_buffer.iter() {
        let mut target_partition = 5; //default partition
        for (key, value) in buffer.iter() {
            if let Some(first_letter) = key.chars().next() {
                for (partition, letters) in helpers::PARTITION_MAP.iter() {
                    if letters.contains(&first_letter) {
                        target_partition = *partition;
                        break;
                    }
                }
            }
            let file_name = format!("./tmp/{}.txt", target_partition);
            let content = format!("{}:{}\n", key, value);


            let zero = mapper.writer_handle.begin_writing(PathBuf::from(&file_name)).await;
            assert_eq!(zero.status, 200);


            let message = Request {
                header: writer::RequestHeader::Payload {
                    key: PathBuf::from_str(&file_name).unwrap(),
                },
                body: Some(content),
            };

            let _ = mapper.writer_handle.write_message(message).await;

        }
    }
    internal_buffer.clear();
    drop(guard);

    println!("Drained internal buffer");
}

pub async fn run_mapper(mut mapper: Mapper) {
    while let Some(msg) = mapper.receiver.recv().await {
        mapper.handle_message(msg).await;
    }
}

#[derive(Clone)]
pub struct HandleMapper {
    sender: mpsc::Sender<MapperMessage>,
}

impl HandleMapper {
    pub fn new(writer: writer::WriterHandle) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mapper = Mapper::new(receiver, writer);
        tokio::spawn(run_mapper(mapper));

        Self { sender }
    }
    pub async fn get_unique_id(&self) -> usize {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::GetId { respond_to: send };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor died")
    }
    pub async fn load_file(&self, filename: PathBuf) -> String {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::ProcessFileTest {
            filename,
            respond_to: send,
        };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor ded")
    }
    pub async fn process_file(&self, filename: PathBuf) -> BTreeMap<String, u32> {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::ProcessSingleFile {
            filename,
            respond_to: send,
        };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor ded")
    }
    pub async fn process_file_with_buffer(&self, filename: PathBuf) -> usize {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::ProcessFileWithBuffer {
            filename,
            respond_to: send,
        };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor ded")
    }
    pub async fn cleanup_signal(&self) -> String {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::Cleanup { respond_to: send };
        let _ = self.sender.send(message).await;
        recv.await.expect("Cleanup actor ded")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mapper() {
        let writer_handle = writer::WriterHandle::new().await;
        let mapper = HandleMapper::new(writer_handle);
        let id1 = mapper.get_unique_id().await;
        let id2 = mapper.get_unique_id().await;
        println!("id1: {}, id2: {}", id1, id2);
        assert_eq!(id1 + 1, id2);
    }

    #[tokio::test]
    async fn test_mapper_load_file() {
        let writer_handle = writer::WriterHandle::new().await;
        let mapper = HandleMapper::new(writer_handle);
        let res = mapper.load_file(PathBuf::from("./test.txt")).await;
        assert_eq!("Hello World!\n", &res);
    }

    #[tokio::test]
    async fn test_mapper_process_file() {
        let writer_handle = writer::WriterHandle::new().await;
        let mapper = HandleMapper::new(writer_handle);
        let res = mapper.process_file(PathBuf::from("./test.txt")).await;
        let map: BTreeMap<String, u32> =
            BTreeMap::from([(String::from("hello"), 1), (String::from("world!"), 1)]);
        assert_eq!(&res.len(), &map.len());
        assert!(&res.keys().all(|key| map.contains_key(key)))
    }

    #[tokio::test]
    async fn test_mapper_process_files() {
        let writer_handle = writer::WriterHandle::new().await;
        let mapper = HandleMapper::new(writer_handle);
        let res = mapper
            .process_file_with_buffer(PathBuf::from("./test.txt"))
            .await;
        assert_eq!(res, 1);
    }
}
