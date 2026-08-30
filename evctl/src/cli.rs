//! Strict argument parsing with a reserved standalone-command namespace.

use std::ffi::{OsStr, OsString};
use std::io::{self, Read};
use std::path::PathBuf;

use crate::output::Format;

pub const HELP: &str = r"Usage:
  evctl [OPTIONS] 'EVENTS ...'
  evctl [OPTIONS] 'LOGS ...'
  evctl [OPTIONS] 'METRIC ...'
  evctl [OPTIONS] --file PATH
  evctl [OPTIONS] -
  evctl help
  evctl version

Query eventd using the PSPU observability query language. The complete query
is one argument; quote it so the shell cannot expand patterns or split string
literals.

Options:
  -f, --format FORMAT  pretty, jsonl, or msgpack (default: pretty)
  -s, --socket PATH    query socket (default: /run/eventd/query.sock)
      --file PATH      read the query from PATH
  -h, --help           show this help
  -V, --version        show the version

The msgpack format is a sequence of records, each prefixed by its four-byte
little-endian payload length. Diagnostics are always written to stderr.
";

const DEFAULT_SOCKET: &str = "/run/eventd/query.sock";

#[derive(Debug)]
pub struct Arguments {
    pub action: Action,
    pub format: Format,
    pub socket: PathBuf,
}

#[derive(Debug)]
pub enum Action {
    Help,
    Version,
    Query(QuerySource),
}

#[derive(Debug)]
pub enum QuerySource {
    Argument(String),
    File(PathBuf),
    Stdin,
}

impl QuerySource {
    pub fn read(self, stdin: &mut impl Read) -> io::Result<String> {
        let mut query = match self {
            Self::Argument(query) => query,
            Self::File(path) => std::fs::read_to_string(path)?,
            Self::Stdin => {
                let mut query = String::new();
                stdin.read_to_string(&mut query)?;
                query
            }
        };
        while query.ends_with(char::is_whitespace) {
            query.pop();
        }
        Ok(query)
    }
}

impl Arguments {
    pub fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, Error> {
        let mut arguments = arguments.into_iter();
        let _program = arguments.next();
        let mut format = Format::Pretty;
        let mut socket = PathBuf::from(DEFAULT_SOCKET);
        let mut file = None;
        let mut positional = Vec::new();
        let mut options = true;

        while let Some(argument) = arguments.next() {
            if options && argument == "--" {
                options = false;
                continue;
            }
            if options && matches!(argument.to_str(), Some("-h" | "--help")) {
                positional.push(OsString::from("help"));
                continue;
            }
            if options && matches!(argument.to_str(), Some("-V" | "--version")) {
                positional.push(OsString::from("version"));
                continue;
            }
            if options && matches!(argument.to_str(), Some("-f" | "--format")) {
                let value = arguments.next().ok_or(Error::MissingValue("--format"))?;
                format = Format::parse(&value).ok_or(Error::BadFormat(value))?;
                continue;
            }
            if options && matches!(argument.to_str(), Some("-s" | "--socket")) {
                let value = arguments.next().ok_or(Error::MissingValue("--socket"))?;
                if value.is_empty() {
                    return Err(Error::EmptySocket);
                }
                socket = PathBuf::from(value);
                continue;
            }
            if options && argument == "--file" {
                let value = arguments.next().ok_or(Error::MissingValue("--file"))?;
                if file.replace(PathBuf::from(value)).is_some() {
                    return Err(Error::DuplicateFile);
                }
                continue;
            }
            if options && argument.to_string_lossy().starts_with('-') && argument != "-" {
                return Err(Error::UnknownOption(argument));
            }
            positional.push(argument);
        }

        let action = action(&positional, file)?;
        Ok(Self {
            action,
            format,
            socket,
        })
    }
}

fn action(positional: &[OsString], file: Option<PathBuf>) -> Result<Action, Error> {
    if let Some(file) = file {
        if !positional.is_empty() {
            return Err(Error::MultipleQuerySources);
        }
        return Ok(Action::Query(QuerySource::File(file)));
    }
    let [argument] = positional else {
        return match positional.len() {
            0 => Err(Error::MissingQuery),
            _ => Err(Error::TooManyArguments),
        };
    };
    if argument == "help" {
        return Ok(Action::Help);
    }
    if argument == "version" {
        return Ok(Action::Version);
    }
    if argument == "-" {
        return Ok(Action::Query(QuerySource::Stdin));
    }
    let query = argument
        .to_str()
        .ok_or(Error::QueryNotUtf8)?
        .trim()
        .to_owned();
    if query.is_empty() {
        return Err(Error::MissingQuery);
    }
    let mode = query.split_ascii_whitespace().next().unwrap_or_default();
    if !matches_ignore_ascii_case(mode, ["EVENTS", "LOGS", "METRIC"]) {
        return Err(Error::UnknownCommand(mode.to_owned()));
    }
    Ok(Action::Query(QuerySource::Argument(query)))
}

fn matches_ignore_ascii_case<const N: usize>(value: &str, choices: [&str; N]) -> bool {
    choices
        .into_iter()
        .any(|choice| value.eq_ignore_ascii_case(choice))
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    MissingQuery,
    TooManyArguments,
    MissingValue(&'static str),
    UnknownOption(OsString),
    BadFormat(OsString),
    EmptySocket,
    DuplicateFile,
    MultipleQuerySources,
    QueryNotUtf8,
    UnknownCommand(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingQuery => formatter.write_str("missing query"),
            Self::TooManyArguments => {
                formatter.write_str("the complete query must be passed as one quoted argument")
            }
            Self::MissingValue(option) => write!(formatter, "{option} requires a value"),
            Self::UnknownOption(option) => write!(formatter, "unknown option {}", quote(option)),
            Self::BadFormat(value) => write!(formatter, "unknown output format {}", quote(value)),
            Self::EmptySocket => formatter.write_str("query socket path is empty"),
            Self::DuplicateFile => formatter.write_str("--file was specified more than once"),
            Self::MultipleQuerySources => {
                formatter.write_str("--file cannot be combined with a query argument")
            }
            Self::QueryNotUtf8 => formatter.write_str("query argument is not valid UTF-8"),
            Self::UnknownCommand(command) => write!(formatter, "unknown command {command:?}"),
        }
    }
}

impl std::error::Error for Error {}

fn quote(value: &OsStr) -> String {
    format!("{:?}", value.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn parse(arguments: &[&str]) -> Result<Arguments, Error> {
        Arguments::parse(arguments.iter().map(OsString::from))
    }

    #[test]
    fn accepts_a_query_as_the_only_positional_argument() {
        let arguments = parse(&["evctl", "EVENTS kacs.* TAKE 1"]).unwrap();
        assert!(matches!(
            arguments.action,
            Action::Query(QuerySource::Argument(ref query)) if query == "EVENTS kacs.* TAKE 1"
        ));
    }

    #[test]
    fn recognises_commands_without_stealing_query_modes() {
        assert!(matches!(
            parse(&["evctl", "help"]).unwrap().action,
            Action::Help
        ));
        assert!(matches!(
            parse(&["evctl", "logs stream"]).unwrap().action,
            Action::Query(_)
        ));
        assert!(matches!(
            parse(&["evctl", "status"]),
            Err(Error::UnknownCommand(command)) if command == "status"
        ));
    }

    #[test]
    fn refuses_shell_split_queries() {
        assert!(matches!(
            parse(&["evctl", "EVENTS", "kacs.*"]),
            Err(Error::TooManyArguments)
        ));
    }

    #[test]
    fn parses_global_options() {
        let arguments = parse(&[
            "evctl",
            "--format",
            "jsonl",
            "--socket",
            "/tmp/eventd.sock",
            "LOGS",
        ])
        .unwrap();
        assert_eq!(arguments.format, Format::JsonLines);
        assert_eq!(arguments.socket, Path::new("/tmp/eventd.sock"));
    }

    #[test]
    fn file_and_argument_are_mutually_exclusive() {
        assert!(matches!(
            parse(&["evctl", "--file", "query.evq", "EVENTS"]),
            Err(Error::MultipleQuerySources)
        ));
    }
}
