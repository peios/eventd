//! Native command-line client for eventd's PSPU query channel.

mod cli;
mod output;
mod protocol;

use std::io::{self, IsTerminal};
use std::os::unix::net::UnixStream;
use std::process::ExitCode;

use cli::{Action, Arguments};
use output::TransactionalOutput;
use protocol::{Connection, Response};

fn main() -> ExitCode {
    match run(std::env::args_os()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_broken_pipe() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("evctl: {error}");
            error.exit_code()
        }
    }
}

fn run(arguments: impl IntoIterator<Item = std::ffi::OsString>) -> Result<(), Error> {
    let arguments = Arguments::parse(arguments).map_err(Error::Usage)?;
    match arguments.action {
        Action::Help => {
            print!("{}", cli::HELP);
            Ok(())
        }
        Action::Version => {
            println!("evctl {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Action::Query(source) => {
            let query = source.read(&mut io::stdin()).map_err(Error::Input)?;
            execute_query(&arguments.socket, &query, arguments.format)
        }
    }
}

fn execute_query(
    socket_path: &std::path::Path,
    query: &str,
    format: output::Format,
) -> Result<(), Error> {
    let stream = UnixStream::connect(socket_path).map_err(|source| Error::Connect {
        path: socket_path.to_owned(),
        source,
    })?;
    let mut connection = Connection::new(stream);
    connection.send_query(query).map_err(Error::Protocol)?;

    let stdout = io::stdout();
    let pretty = stdout.is_terminal();
    let mut output =
        TransactionalOutput::new(stdout.lock(), format, pretty).map_err(Error::Output)?;
    let mut watching = false;

    loop {
        let response = connection.next_response().map_err(Error::Protocol)?;
        match response {
            Response::Records(records) => {
                for record in records {
                    output.write_record(&record).map_err(Error::Output)?;
                }
            }
            Response::End if !watching => {
                output.commit().map_err(Error::Output)?;
                return Ok(());
            }
            Response::Watch if !watching => {
                output.commit().map_err(Error::Output)?;
                watching = true;
            }
            Response::Error(message) => {
                return Err(Error::Query {
                    message,
                    initial_results_complete: watching,
                });
            }
            Response::End | Response::Watch => {
                return Err(Error::Protocol(protocol::Error::UnexpectedStatus));
            }
        }
    }
}

#[derive(Debug)]
enum Error {
    Usage(cli::Error),
    Input(io::Error),
    Connect {
        path: std::path::PathBuf,
        source: io::Error,
    },
    Protocol(protocol::Error),
    Query {
        message: String,
        initial_results_complete: bool,
    },
    Output(io::Error),
}

impl Error {
    fn exit_code(&self) -> ExitCode {
        match self {
            Self::Usage(_) => ExitCode::from(2),
            Self::Input(_)
            | Self::Connect { .. }
            | Self::Protocol(_)
            | Self::Query { .. }
            | Self::Output(_) => ExitCode::from(1),
        }
    }

    fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::Output(error) if error.kind() == io::ErrorKind::BrokenPipe)
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Usage(error) => write!(formatter, "{error}\nTry 'evctl help' for usage."),
            Self::Input(error) => write!(formatter, "cannot read query: {error}"),
            Self::Connect { path, source } => {
                write!(formatter, "cannot connect to {}: {source}", path.display())
            }
            Self::Protocol(error) => write!(formatter, "query protocol failure: {error}"),
            Self::Query {
                message,
                initial_results_complete,
            } => {
                if *initial_results_complete {
                    write!(formatter, "stream ended: {message}")
                } else {
                    formatter.write_str(message)
                }
            }
            Self::Output(error) => write!(formatter, "cannot write results: {error}"),
        }
    }
}

impl std::error::Error for Error {}
