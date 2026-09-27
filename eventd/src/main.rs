//! eventd process entry point.

fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let result = if args.is_empty() {
        eventd::run()
    } else if args.len() == 1 && args[0] == "--prepare-security" {
        eventd::prepare_security()
    } else {
        eprintln!("usage: eventd [--prepare-security]");
        std::process::exit(2);
    };
    if let Err(error) = result {
        eprintln!("eventd: {error}");
        std::process::exit(1);
    }
}
