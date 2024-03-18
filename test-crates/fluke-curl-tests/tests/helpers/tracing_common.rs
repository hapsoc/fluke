use tracing::Level;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

/// Set up a global tracing subscriber.
///
/// This won't play well outside of cargo-nextest or running a single
/// test, which is a limitation we accept.
pub(crate) fn setup_tracing() {
    let filter_layer = Targets::new()
        .with_default(Level::INFO)
        .with_target("fluke", Level::DEBUG)
        .with_target("want", Level::INFO);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_file(false)
        .with_line_number(false);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();
}
