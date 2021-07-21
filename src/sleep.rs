use std::{
    cmp::Reverse,
    collections::{binary_heap::PeekMut, BinaryHeap, HashMap},
    future::Future,
    lazy::SyncOnceCell,
    pin::Pin,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

// TODO: use std::sync::mpsc
use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
// use ::sync::OnceCell;
use pin_project::pin_project;
use snowflake::ProcessUniqueId;

// DelayState is an enum that tracks weather the Delay has been created or is Waiting (in which
// case the Future is on the receiving end of the message channel to accept the signal)
#[derive(Debug)]
enum DelayState {
    New,
    Waiting {
        signal: Receiver<()>,
        id: ProcessUniqueId,
    },
}

#[derive(Debug)]
pub struct Delay {
    deadline: Instant,
    state: DelayState,
}

pub fn sleep_until(deadline: Instant) -> Delay {
    Delay {
        deadline,
        state: DelayState::New,
    }
}

pub fn sleep(how_long: Duration) -> Delay {
    sleep_until(Instant::now() + how_long)
}

#[derive(Debug)]
pub enum Message {
    New {
        deadline: Instant,
        waker: Waker,
        // notify is a channel used to sync the different threads. It avoids potential syncing issues between threads (a situation that might potentially
        // arise if a thread thinks it is in the future in respect to the other thread
        notify: Sender<()>,
        id: ProcessUniqueId,
    },
    Polled {
        waker: Waker,
        id: ProcessUniqueId, // this is needed for identifying Future Wakers in the BinaryHeap
    },
}

impl Future for Delay {
    // we just wanna know when the Future is done. No need to wrap a result
    type Output = ();

    // poll -ing the function we are going to sleep and we are going to signal the waker when we
    // are ready to be polled again
    // The waker is awoken when we are ready to make more progress
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // get_mut turns this into a standard mutable reference. This is possible because we are
        // using plain old struct to model the `Delay` and we can guarantee that nothing will go
        // wrong if we unpin self
        let this = self.get_mut();

        match this.state {
            DelayState::New => {
                // first time we Poll, we have been given a `Waker` in the `Context` cx. We want to notify
                // the Reactor when we're done
                // We create a bounded channel since we will deal with only one Message at the time. This
                // is the equivalent of a buffered channel in Go with 1 message capacity
                let (notify, signal) = bounded(1);
                // creating a unique ID for this Future
                let id = ProcessUniqueId::new();
                // we are not checking the time here on purpose, since we assume that the first time we are
                // polling, the deadline has not commenced.
                // This is handled in the sleeper thread anyway
                let msg = Message::New {
                    notify,
                    waker: cx.waker().clone(),
                    deadline: this.deadline, // remember that `this` instead of `self` is a normal mutable reference rather than a pinned one
                    id,
                };

                this.state = DelayState::Waiting { signal, id };

                // since this is a static reference, the thread should never die nor be dropped. This means
                // that we can safely unwrap here
                sleeper_thread_channel().send(msg).unwrap();
                Poll::Pending
            }

            // when waiting, we check our signal to see if we are done.
            // Since it is critical not to block here, we `try_recv`
            // ref references the `signal` channel receiver stored in Waiting
            DelayState::Waiting { ref signal, id } => match signal.try_recv() {
                Ok(()) => Poll::Ready(()), // we are done
                Err(TryRecvError::Disconnected) => {
                    panic!("sleeper thread dropped the Delay future!")
                } // the thread dropped us. We panic
                // we were polled but we are not ready yet
                Err(TryRecvError::Empty) => {
                    // we cannot be guaranteed that we will be given the same waker, so we need to
                    // let the thread know that we have a new waker.
                    // This because the Futures do not track wakers but only care about the most
                    // recent one.
                    // In this naìve implementation rather than keeping a local reference to the
                    // waker and check if the one we got is the same, we always send it.
                    let msg = Message::Polled {
                        waker: cx.waker().clone(),
                        id,
                    };
                    sleeper_thread_channel()
                        .send(msg)
                        .expect("error sending to the sleeper thread");
                    Poll::Pending
                }
            },
        }
    }
}

// We have to return `Pending` and then wake up the Waker we got when the timer expires
// To do that we need a Thread that goes to sleep in a blocking way.
// This is the reason why we need a `Reactor`: to schedule each sleep Future within a single thread,
// rather than having each of them manage its own thread
//
// The Reactor is a global thread that helps with the absence of a runtime by storing every single timer in its
// `BinaryHeap` so that it is always sorted and tracks which timer is attached to which `Waker` and
// wakes them all as they come, one by one.
// The Reactor has a channel to listen to incoming new sleeps
//
fn sleeper_thread_channel() -> &'static Sender<Message> {
    // we make a global static channel
    static CHANNEL: SyncOnceCell<Sender<Message>> = SyncOnceCell::new();

    CHANNEL.get_or_init(|| {
        // NOTE: An unbounded channel is not the right choice here, because the channel could grow
        // indefinitely.
        // We should not use a bounded channel because otherwise this would block
        // here we should use an aync-aware channel instead of an unbounded one, which would pretty
        // much notify a waker in the same fashion as the Delay future
        // For now we are going to make sure that we have a reasonable duration of sleep as to not
        // overflow the channel
        let (tx, rx) = unbounded();
        // The whole point of this thread is that as we create all these Delays, everytime we poll them we
        // are gonna send them messages that we need to respond to.
        thread::spawn(move || {
            // we use a BinaryHeap to store the timers themselves.
            // However, we wanna pop the smallest timer (the soonest event), so we `Reverse` the instant
            // We cannot modify stuff that we store in the BinaryHeap, so we cannot store
            // Wakers directly, since we might create new ones and swap them. Therefore we need to
            // be able to identify the Delay object. Hence the snowflake::ProcessUniqueId
            let mut timers: BinaryHeap<(Reverse<Instant>, ProcessUniqueId)> = BinaryHeap::new();
            // then we create a map of ID and Wakers/sync channel
            let mut wakers: HashMap<ProcessUniqueId, (Waker, Sender<()>)> = HashMap::new();

            // looping forever and dealing with messages
            loop {
                let now = Instant::now();
                // we wanna make sure that every single timer that expires have been dealt with
                let next_event = loop {
                    match timers.peek_mut() {
                        // this lets return from the heap without an Option
                        None => break None,
                        Some(slot) => {
                            // if the slot is in the past...
                            // .0 is the reverse and .0.0 is the instant inside the Reverse
                            if slot.0 .0 <= now {
                                // removing this slot from the heap
                                let (_, id) = PeekMut::pop(slot);
                                // check in the waker table
                                if let Some((waker, sender)) = wakers.remove(&id) {
                                    // tell the associated Delay that its time expired
                                    if let Ok(()) = sender.send(()) {
                                        waker.wake();
                                    }
                                }
                            } else {
                                // the slot is in the future, that is our duration
                                // Listening to events until that timers expires
                                // so we break out with that instant
                                break Some(slot.0 .0);
                            }
                        }
                    }
                };
                let msg = match next_event {
                    // there is no deadline, so it is infinitely in the future
                    None => rx.recv().expect("sender has been dropped!"),
                    // there is a deadline detected
                    Some(deadline) => match rx.recv_deadline(deadline) {
                        // we got our message, we return it
                        Ok(msg) => msg,
                        // a timeout means that did not get any messages but one of our timers expired. We resume the loop
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => {
                            panic!("the sender has been dropped despite being static!")
                        }
                    },
                };

                // now we handle the message
                match msg {
                    // new delay was created
                    Message::New {
                        deadline,
                        waker,
                        notify,
                        id,
                    } => {
                        // we populate our tables
                        timers.push((Reverse(deadline), id));
                        wakers.insert(id, (waker, notify));
                    }
                    // existing delay was polled
                    Message::Polled { waker, id } => {
                        // looking in the hashmap for the waker
                        if let Some((old_waker, _)) = wakers.get_mut(&id) {
                            *old_waker = waker;
                        }
                    }
                }
            }
        });

        tx
    })
}
