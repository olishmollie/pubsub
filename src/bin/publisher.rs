use pubsub::{Message, Publisher};

fn main() {
    let ctx = zmq::Context::new();
    let publisher = Publisher::new(&ctx);

    let mut args = std::env::args();

    let topic = args.nth(1).unwrap();
    println!("topic = {}", topic);

    let payload = args.nth(0).unwrap();
    println!("payload = {}", payload);

    let message = Message::new(&topic, &payload);

    let timer = timer::Timer::new();
    let guard = timer.schedule_repeating(chrono::Duration::seconds(1), move || {
        println!(
            "Publishing on topic '{:?}', payload '{:?}'",
            message.topic, message.payload
        );
        publisher.publish(message.clone());
    });

    std::thread::sleep(std::time::Duration::new(10, 0));

    drop(guard)
}
