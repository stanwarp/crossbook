use std::hint::black_box;
use std::time::Instant;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use crossbook::book::{Book, DepthEvent, Snapshot};
use crossbook::protocol::Envelope;
use crossbook::store::book_store;
use crossbook::view::{BookView, NotLiveReason};

// The frame and the snapshot were captured together, 8 s apart, so 42 of the frame's 58 levels
// fall inside the book's band. Re-record them as a pair, or the apply benchmark measures nothing.
const FRAME: &str = include_str!("../tests/fixtures/binance_depth_frame.json");
const SNAPSHOT: &str = include_str!("../tests/fixtures/binance_depth_snapshot.json");
const K_LEVELS: usize = 20;
const FEED: &str = "binance";

fn parse(text: &str) -> DepthEvent {
    let envelope: Envelope = serde_json::from_str(text).expect("the recorded frame parses");
    DepthEvent::from(envelope.data)
}

fn snapshot() -> Snapshot {
    serde_json::from_str(SNAPSHOT).expect("the recorded snapshot parses")
}

fn hot_path(c: &mut Criterion) {
    let snapshot = snapshot();
    let event = parse(FRAME);
    let now = Instant::now();
    let fresh_book = || Book::from_snapshot(&snapshot).expect("both sides recorded");
    let (writer, _reader) = book_store(BookView::empty(FEED, NotLiveReason::Connecting, now));

    c.bench_function("parse frame", |b| {
        b.iter(|| parse(black_box(FRAME)));
    });
    c.bench_function("apply event to a fresh 1000-level book", |b| {
        b.iter_batched(
            fresh_book,
            |mut book| {
                book.apply_event(&event);
                book
            },
            BatchSize::LargeInput,
        );
    });
    let mut book = fresh_book();
    book.apply_event(&event);
    c.bench_function("apply event again to the same book", |b| {
        b.iter(|| book.apply_event(black_box(&event)));
    });
    c.bench_function("build and store the view", |b| {
        b.iter(|| {
            writer.publish(BookView::live(FEED, &book, K_LEVELS, now, Some(20)));
        });
    });
    c.bench_function("frame to stored view", |b| {
        b.iter_batched(
            fresh_book,
            |mut book| {
                let event = parse(black_box(FRAME));
                book.apply_event(&event);
                writer.publish(BookView::live(FEED, &book, K_LEVELS, now, Some(20)));
                book
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(benches, hot_path);
criterion_main!(benches);
