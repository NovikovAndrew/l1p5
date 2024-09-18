use std::time::Duration;
use flume::{Receiver};
use tokio::task;
use tokio::signal;
use tokio::sync::Notify;
use std::sync::Arc;

//как я понял задание нужно сделать gracefully shutdown

#[tokio::main]
async fn main() {
    let worker_count = 10; // колличество воркеров
    let (tx, rx) = flume::unbounded(); // канал flume для mpmc
    let notify = Arc::new(Notify::new()); // уведомление для завершения

    start_worker_pool(worker_count, rx, Arc::clone(&notify)).await;

    // продюсер нашего воркера
    let tx_producer = tx.clone();
    let producer_handler = task::spawn(async move {

        // предлоголаем что здесь бизнес логика
        for i in (0..10) {
            let message = format!("message id is {}", i).to_string();
            if let Ok(_) = tx_producer.send_async(Message::Message(message)).await {
                // моканные данные которые кидаем продюсеру
                println!("message {} is send", i);
            }
        }
    });

    // ожидаем завершения продюсера
    producer_handler.await.expect("failed to ");

    signal::ctrl_c().await.expect("failed to signal ctrl_c");
    println!("\nterminate the program");

    // отправляем вокерам что прошрамма завершается
    for i in 0..worker_count {
        tx.send_async(Message::Terminate).await.expect("failed to send terminate message");
    }

    for i in 0..worker_count {
        // ждем уведомления от каждого воркера о завершении
        notify.notified().await
    }

    print!("workers terminated")
}

// тип сообщения который отправляем воркеру
enum Message {
    Message(String), // сообщение которое нужно обработать и выполнить бизнес локигу
    Terminate,    // сообщение которое говорит закончить работу воркера
}

// реализация worker pool
async fn start_worker_pool(worker_size: usize, rx: Receiver<Message>, notify: Arc<Notify>) {
    for i in 0..worker_size {
        let rx = rx.clone();
        let notify = Arc::clone(&notify);
        // запускаем каждого воркера в отдельном треде
        task::spawn(worker(i, rx, notify));
    }
}

// функции worker
async fn worker(worker_id: usize, rx: Receiver<Message>, notify: Arc<Notify>) {
    loop {
        match rx.recv_async().await {
            Ok(Message::Message(id)) => {
                // имитируем бизнес логику
                println!("received task by id, id - {}", id);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Message::Terminate) => {
                // увидоляем о завершеннии работы
                notify.notify_one();
                return
            }
            Err(_) => {
                println!("failed ti recv worker, id - {}", worker_id);
            }
        }
    }
}
