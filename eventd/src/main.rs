//! eventd process entry point.

fn main() {
    if let Err(error) = eventd::run() {
        eprintln!("eventd: {error}");
        std::process::exit(1);
    }
}
