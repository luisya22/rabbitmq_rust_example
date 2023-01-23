use std::borrow::Borrow;
use std::time::Duration;
use deadpool_lapin::{Manager,Pool, PoolError};
use futures::{join, StreamExt};
use lapin::ConnectionProperties;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use thiserror::Error as ThisError;
use tokio_amqp::LapinTokioExt;

type RMQResult<T> = Result<T, PoolError>;
type Connection = deadpool::managed::Object<deadpool_lapin::Manager>;

#[derive(ThisError, Debug)]
enum Error {
    #[error("rmq error: {0}")]
    RMQError(#[from] lapin::Error),
    #[error("rmq pool error: {0}")]
    RMQPoolError(#[from] PoolError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let addr = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into());

    let manager = Manager::new(addr, ConnectionProperties::default().with_tokio());
    let pool: Pool = Pool::builder(manager)
        .max_size(10)
        .build()
        .expect("can create pool");

    let _ = join!(
        rmq_listen(pool.clone())
    );

    Ok(())
}

async fn rmq_listen(pool: Pool) -> Result<(), Error>{
    let mut retry_interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        retry_interval.tick().await;
        println!("connection rmq consumer...");
        match init_rmq_listen(pool.clone()).await {
            Ok(_) => println!("rmq listen returned"),
            Err(e) => eprintln!("rmq listen had an error: {}", e),
        }
    }
}

async fn init_rmq_listen(pool: Pool) -> Result<(), Error> {
    let rmq_con = get_rmq_con(pool).await.map_err(|e| {
        eprintln!("could not get rmq con: {}", e);
        e
    })?;

    let channel = rmq_con.create_channel().await?;

    let options = QueueDeclareOptions{
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        nowait: false,
    };

    let queue = channel
        .queue_declare(
            "task_queue",
            options,
                FieldTable::default()
        )
        .await?;

    println!("Declared queue {:?}", queue);

    let mut consumer = channel
        .basic_consume(
            queue.name().borrow(),
            "",
            BasicConsumeOptions::default(),
            FieldTable::default()
        )
        .await?;

    println!("rmq consumer connected, waiting for messages");
    while let Some(delivery) = consumer.next().await {
        if let Ok((channel, delivery)) = delivery {
            println!("received msg: {:?}", delivery);

            let data = delivery.data;

            println!("data: {:?}", data);

            let s = match std::str::from_utf8(&*data){
                Ok(v) => v,
                 _ => ""
            };
            println!("data str: {:?}", s);

            channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await?
        } else {
            println!("No messages")
        }
    }

    Ok(())
}

async fn get_rmq_con(pool: Pool) -> RMQResult<Connection> {
   let connection = pool.get().await?;

    Ok(connection)
}
