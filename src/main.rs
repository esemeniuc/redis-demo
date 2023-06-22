use std::mem::{size_of, size_of_val};
use std::thread::sleep;
use std::time::{Duration, Instant};
use redis::{AsyncCommands, ConnectionLike, Msg, RedisResult};

use tokio::time::timeout;

use futures::StreamExt;
use futures_util::Stream;
use redis::aio::Connection;

use deadpool_redis::{redis::{cmd, FromRedisValue}, Config, Runtime};

async fn deadpool() {
    let mut cfg = Config::from_url("rediss://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();


    let start = Instant::now();
    // let out: Vec<RedisResult<Vec<u8>>> = futures_util::future::join_all((0..1).map(|i|
    //     async {
    //         let mut connection = pool.get().await.unwrap();
    //         let val = redis::pipe()
    //             .cmd("MSETNX")
    //             .arg(&[("key1", 1), ("key2",2)])
    //             .cmd("MGET")
    //             .arg(&["key1", "key2"])
    //             .query_async(&mut connection).await;
    //         val
    //         // connection.mget(&["key1","key1"]).await
    //     })).await;

    let mut connection = pool.get().await.unwrap();
    let out : (i64, Vec<String>)= redis::pipe()
        .cmd("MSETNX")
        .arg(&[("key1", "foo1"), ("key2","bar1")])
        .cmd("EVAL")
        .arg("for i, key in ipairs(KEYS) do redis.pcall('PEXPIRE', key, ARGV[1], 'NX') end")
        .arg(3)
        .arg(&["key1", "key000", "key2",])
        .arg(Duration::from_secs(5).as_millis() as u64)
        .ignore()
        .cmd("MGET")
        .arg(&["key1", "key2"])
        .query_async(&mut connection).await.unwrap();
    dbg!(out);
    println!("{:?}", start.elapsed());

    {
        let mut conn = pool.get().await.unwrap();
        cmd("SET")
            .arg(&["deadpool/test_key", "42"])
            .query_async::<_, ()>(&mut conn)
            .await.unwrap();
    }
    {
        let mut conn = pool.get().await.unwrap();
        let value: String = cmd("GET")
            .arg(&["deadpool/test_key"])
            .query_async(&mut conn)
            .await.unwrap();
        assert_eq!(value, "42".to_string());
    }
}

// Subscribe to keyspace events
async fn get_event_stream(client: &redis::Client) -> RedisResult<impl Stream<Item=redis::Msg>> {
    let mut con = client.get_async_connection().await?;
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg("KEA")
        .query_async(&mut con).await?;
    let mut pubsub = con.into_pubsub();
    pubsub.psubscribe("*").await?; // subscribes to any db

    Ok(pubsub.into_on_message())
}

/// Redis Rust pubsub with reconnection example
async fn pubsub() -> redis::RedisResult<()> {
    let client = redis::Client::open("rediss://127.0.0.1/").unwrap();
    let mut events = get_event_stream(&client).await?;
    let start = Instant::now();
    loop {
        let msg: redis::Msg = match timeout(Duration::from_secs(5), events.next()).await {
            Ok(None) => {
                println!("Sub Fail");
                dbg!(client.is_open());
                if client.is_open() {
                    events = get_event_stream(&client).await?;
                }
                sleep(Duration::from_secs(1));
                continue;
            }
            Err(_) => {
                println!("Sub Timeout");
                continue;
            }
            Ok(Some(msg)) => msg,
        };
        println!("elapsed {:?}", start.elapsed());
        dbg!(&msg);
        dbg!(msg.get_channel::<String>());
        dbg!(msg.get_channel_name());
        let payload: String = msg.get_payload()?;
        dbg!(&payload);
        match payload.as_str() {
            "set" => {
                println!("get set");
            }

            "expire" => {
                println!("get expire");
            }
            _ => { println!("Other!") }
        }
    }
}

async fn benchmark() -> RedisResult<()> {
    let client = redis::Client::open("rediss://127.0.0.1/").unwrap();
    let mut con = client.get_multiplexed_tokio_connection().await?;

    con.set("key1", b"foo").await?;

    redis::cmd("SET").arg(&["key2", "bar"]).query_async(&mut con).await?;
    let result = redis::cmd("MGET")
        .arg(&["key1", "key2"])
        .query_async(&mut con)
        .await;
    assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));

    let start = Instant::now();
    for _request_nbr in 0..1_000_000 {
        // con.get("key1").await?
        let _ = con.clone();
    }
    println!("clone took {:?}", start.elapsed());

    let a: Vec<_> = (0..1_000_000).map(|_i| con.clone()).collect();
    let start = Instant::now();
    let out: Vec<RedisResult<String>> = futures_util::future::join_all(a.into_iter().map(|mut connection|
        async move {
            connection.get("key1").await
        })).await;
    println!("{:?}", start.elapsed());
    dbg!(out.len());
    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();
    // deadpool().await;
    // benchmark().await.unwrap();
    pubsub().await.unwrap();
}
