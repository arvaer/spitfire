use tokio::sync::{mpsc, oneshot};

pub struct Reducer {
    receiver: mpsc::Receiver<ReducerMessage>,
    message_id: usize,
}

pub enum ReducerMessage {
    GetId { respond_to: oneshot::Sender<usize> },
}

impl Reducer {
    fn new(receiver: mpsc::Receiver<ReducerMessage>) -> Self {
        Self {
            receiver,
            message_id: 0,
        }
    }

    fn handle_message(&mut self, msg: ReducerMessage) {
        match msg {
            ReducerMessage::GetId { respond_to } => {
                self.message_id += 1;
                let _ = respond_to.send(self.message_id);
            }
        }
    }
}

async fn run_reducer(mut reducer: Reducer) {
    while let Some(msg) = reducer.receiver.recv().await {
        reducer.handle_message(msg);
    }
}

#[derive(Clone)]
pub struct HandleReducer {
    sender: mpsc::Sender<ReducerMessage>,
}

impl HandleReducer {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let reducer: Reducer = Reducer::new(receiver);
        tokio::spawn(run_reducer(reducer));

        Self { sender }
    }

    pub async fn get_unique_id(self) -> usize {
        let (send, recv) = oneshot::channel();
        let message = ReducerMessage::GetId { respond_to: send };
        let _ = self.sender.send(message).await;
        recv.await.expect("Reduce Actor died")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_unique_id() {
        let reducer = HandleReducer::new();
        let id = reducer.get_unique_id().await;
        assert_eq!(id, 1);
    }
}
