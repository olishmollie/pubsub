use pubsub::{Message, Publisher};

fn main() {
    let ctx = zmq::Context::new();
    let publisher = Publisher::new(&ctx);

    let mut args = std::env::args();

    let topic = args.nth(1).unwrap();
    let payload = args.nth(0).unwrap();

    let message = Message::new(&topic, &payload);

    let timer = timer::Timer::new();
    let guard = timer.schedule_repeating(chrono::Duration::seconds(1), move || {
        publisher.publish(message.clone());
    });

    std::thread::sleep(std::time::Duration::new(10, 0));

    drop(guard)
}
