use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use pubsub::{Listener, Message};

fn main() {
    let ctx = zmq::Context::new();

    let mut args = std::env::args();
    let topic = args.nth(1).unwrap();

    let _listener = Listener::new(
        &ctx,
        &topic,
        Some(move |message: Message| {
            println!("Received payload: '{:?}", &message.payload);
        }),
    );

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("Handling ctrlc!");
        r.store(false, Ordering::Relaxed);
    })
    .unwrap();

    while running.load(Ordering::Relaxed) {}
}
