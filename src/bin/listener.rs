use pubsub::{Listener, Message};

fn main() {
    let ctx = zmq::Context::new();

    let mut args = std::env::args();
    let topic = args.nth(1).unwrap();

    Listener::new(
        &ctx,
        &topic,
        Some(move |message: Message| {
            println!("Received payload: '{:?}", &message.payload);
        }),
    );

    println!("Listening on topic '{}'", topic);

    loop {}
}
