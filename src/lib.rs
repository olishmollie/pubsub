use std::collections::HashMap;

/// A serializable message.
#[derive(Clone, Debug)]
pub struct Message {
    pub topic: String,
    pub payload: String,
}

impl Message {
    /// Create a new message.
    pub fn new(topic: &str, payload: &str) -> Self {
        Message {
            topic: topic.to_owned(),
            payload: payload.to_owned(),
        }
    }
}

/// Subscribes to and acts on messages as they arrive.
pub struct Listener {}

impl Listener {
    /// Subscribe to `topic` and execute `callback` on when message is received.
    pub fn new(
        ctx: &zmq::Context,
        topic: &str,
        callback: Option<impl Fn(Message) -> () + Send + 'static>,
    ) {
        let socket = ctx.socket(zmq::SUB).unwrap();
        let running = true;

        socket.connect(Proxy::PUB_ADDR).unwrap();
        socket.set_subscribe(topic.as_bytes()).unwrap();

        std::thread::sleep(std::time::Duration::new(1, 0));

        std::thread::spawn(move || {
            while running {
                let topic = socket.recv_msg(0).unwrap();
                let data = socket.recv_msg(0).unwrap();
                let payload = bincode::deserialize(&data).unwrap();

                let message = Message::new(topic.as_str().unwrap(), payload);
                println!("Received message {:?}", message);

                if let Some(callback) = &callback {
                    callback(message);
                }
            }
        });
    }
}

pub struct Publisher {
    socket: zmq::Socket,
}

impl Publisher {
    pub fn new(ctx: &zmq::Context) -> Self {
        let socket = ctx.socket(zmq::PUB).unwrap();
        socket.connect(Proxy::SUB_ADDR).unwrap();

        // Ensure publisher is connected before sending messages.
        std::thread::sleep(std::time::Duration::new(1, 0));

        Publisher { socket }
    }

    pub fn publish(&self, message: Message) {
        self.socket
            .send(message.topic.as_bytes(), zmq::SNDMORE)
            .unwrap();
        println!(
            "Publishing on topic {:?}, payload {:?}",
            message.topic, message.payload
        );
        self.socket
            .send(bincode::serialize(&message.payload).unwrap(), 0)
            .unwrap();
    }
}

/// Forwards subscriptions and implements last value caching (LVC).
pub struct Proxy {}

impl Proxy {
    pub const SUB_ADDR: &'static str = "ipc://sub.ipc";
    pub const PUB_ADDR: &'static str = "ipc://pub.ipc";

    pub fn run() {
        let ctx = zmq::Context::new();

        let frontend = ctx.socket(zmq::XSUB).unwrap();
        let backend = ctx.socket(zmq::XPUB).unwrap();

        frontend.bind(Self::SUB_ADDR).unwrap();
        backend.bind(Self::PUB_ADDR).unwrap();

        let mut cache = HashMap::new();

        let mut items = [
            frontend.as_poll_item(zmq::POLLIN),
            backend.as_poll_item(zmq::POLLIN),
        ];

        // zmq::proxy(&frontend, &backend).unwrap();

        loop {
            if zmq::poll(&mut items, -1).is_err() {
                break; // Interrupted
            }

            if items[0].is_readable() {
                let topic = frontend.recv_msg(0).unwrap();
                let message = frontend.recv_msg(0).unwrap();

                println!("Proxy received topic {:?}, payload {:?}", topic, message);

                cache.insert(topic.to_vec(), message.to_vec());

                backend.send(topic, zmq::SNDMORE).unwrap();
                backend.send(message, 0).unwrap();
            }

            if items[1].is_readable() {
                // Event is one byte 0=unsub or 1=sub, followed by topic.
                let event = backend.recv_msg(0).unwrap();

                println!("Proxy received event {:?}", event);

                if event[0] == 1 {
                    let topic = &event[1..];

                    println!(
                        "Sending cached topic {}",
                        std::str::from_utf8(topic).unwrap()
                    );

                    if let Some(previous) = cache.get(topic) {
                        backend.send(topic, zmq::SNDMORE).unwrap();
                        backend.send(previous, 0).unwrap();
                    }

                    frontend.send(event, 0).unwrap();
                }
            }
        }
    }
}
