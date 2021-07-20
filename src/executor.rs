use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
};

use cooked_waker::{IntoWaker, WakeRef};
use futures::pin_mut;

// This is a naive implementation of a notifier which should already be available with any runtime
// like Tokio
#[derive(Debug, Default)]
struct Notifier {
    // a simple Mutex guarding the condition for blocking
    was_notified: Mutex<bool>,
    // blocking the thread while waiting for an event (i.e. was_notified) to occur
    cv: Condvar,
}

impl Notifier {
    // blocking the thread until it is notified
    fn wait(&self) {
        let mut was_notified = self.was_notified.lock().expect("cannot acquire lock");

        // waiting for the was_notified to be set to true
        while !*was_notified {
            eprintln!("Waiting for wake");
            // the cv is blocking until a separate thread sends a signal (through the Notifier)
            was_notified = self
                .cv
                .wait(was_notified)
                .expect("cannot block the thread and waiting for notifications")
        }

        // setting it to false so other Futures can notify us again
        *was_notified = false;
    }
}

// implementing the WakeRef trait so Notifier can be awoken
impl WakeRef for Notifier {
    fn wake_by_ref(&self) {
        let was_notified = {
            let mut lock = self
                .was_notified
                .lock()
                .expect("cannot lock was_notified mutex");
            // only if it wasn't notified, only then we signal the Condvar. In fact, replace
            // mem::replace turns the was_notified to true while returning the old value
            mem::replace(&mut *lock, true)
        };

        // if not already notified, signal the cv
        if !was_notified {
            eprintln!("Awoken");
            self.cv.notify_one();
        }
    }
}

pub fn run_future<F: Future>(future: F) -> F::Output {
    // this guarantees that whatever we have pinned to the stack, will remain pinned forever
    // the pin_mut macro basically takes care of the unsafe
    // essentially what it does is to shadow the variable with a pinned reference
    // so it is not possible to get the original object ever again (so the object cannot be moved
    // because we lost ownership of the reference here)
    pin_mut!(future);

    // wakers need to be cheaply clonable across the Futures, since the latters would probably have
    // to manage their own wakers in their logic. This is the reason why they are an Arc
    // on a struct like the Notifier
    let notifier = Arc::new(Notifier::default());
    // into_waker is part of the WakeRef trait and deals with the raw pointers and
    // function pointers, etc
    let waker = notifier.clone().into_waker();
    let mut cx = Context::from_waker(&waker);

    // when we poll the future...
    loop {
        // a pinned reference is not a normal movable reference, so when we call a function on it,
        // it cannot be moved, so `future` provides an `as_mut()` function, which is kinda like a
        // copy, but with a shorter lifetime which takes care of moving the pinned reference
        match future.as_mut().poll(&mut cx) {
            //...it might be ready, which means we are done and can return the output
            Poll::Ready(output) => return output,
            //...when instead it returns `Pending`, it means that it arranged for the `Waker` in the
            //context to be called when it wants to progress on. So all we have to do is block the
            //thread
            Poll::Pending => notifier.wait(),
        };
    }
}
