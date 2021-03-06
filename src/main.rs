#![allow(unused_imports)]
#![feature(once_cell)]

mod executor;
mod sleep;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::future::join;

#[derive(Debug)]
struct Yield {
    yielded: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

async fn simple() -> i32 {
    let sleep5 = sleep::sleep(Duration::from_secs(5));
    let sleep3 = sleep::sleep(Duration::from_secs(3));

    join(sleep3, sleep5).await;

    let inner = Yield { yielded: false };
    inner.await;

    10
}

fn main() {
    let fut = simple();

    let output = executor::run_future(fut);

    assert_eq!(output, 10);
}
