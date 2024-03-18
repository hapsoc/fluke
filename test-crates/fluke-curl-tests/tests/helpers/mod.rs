use std::future::Future;

pub(crate) mod tracing_common;

pub(crate) fn run(test: impl Future<Output = eyre::Result<()>>) {
    color_eyre::install().unwrap();
    fluke::maybe_uring::start(async {
        tracing_common::setup_tracing();

        if let Err(e) = test.await {
            panic!("Error: {e:?}");
        }
    });
}
