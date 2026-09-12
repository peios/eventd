//! Parser and semantic model for the PSPU observability query language.

use core::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub source: Source,
    pub since: Option<TimeExpr>,
    pub until: Option<TimeExpr>,
    pub predicates: Vec<Expr>,
    pub cross_filters: Vec<CrossFilter>,
    pub sort: Vec<SortKey>,
    pub take: Option<u64>,
    pub skip: u64,
    pub select: Vec<String>,
    pub aggregate: Option<RecordAggregate>,
    pub transform: Option<Transform>,
    pub metric_aggregate: Option<MetricAggregate>,
    pub stream: bool,
    pub index: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CrossFilter {
    Metric {
        name: String,
        labels: Option<Vec<Expr>>,
        operator: Operator,
        value: Literal,
    },
    EventExists {
        pattern: String,
    },
    LogExists {
        origin: String,
        containing: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    Events {
        pattern: Option<String>,
    },
    Logs {
        origins: Vec<String>,
        error_only: bool,
        containing: Option<String>,
    },
    Metric {
        name: String,
        labels: Option<Vec<Expr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Compare {
        field: String,
        operator: Operator,
        value: Literal,
    },
    In {
        field: String,
        negated: bool,
        values: Vec<Literal>,
    },
    Null {
        field: String,
        negated: bool,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl Expr {
    pub fn fields(&self, output: &mut Vec<String>) {
        match self {
            Self::Compare { field, .. } | Self::In { field, .. } | Self::Null { field, .. } => {
                if !output.contains(field) {
                    output.push(field.clone());
                }
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.fields(output);
                right.fields(output);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    StartsWith,
    EndsWith,
    Contains,
    /// Array containment: true when the field holds an array with an
    /// element equal to the value. `CONTAINS` is string containment and
    /// cannot serve here.
    Has,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeExpr {
    Relative { nanoseconds: u64, future: bool },
    Today,
    Yesterday,
    Absolute(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordAggregate {
    CountBy(String),
    TopBy {
        count: u64,
        field: String,
    },
    Distinct(String),
    Group {
        fields: Vec<String>,
        function: GroupFunction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupFunction {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transform {
    Rate,
    Delta,
    Percentile(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricAggregate {
    Scalar(AggregateFunction),
    Window(AggregateFunction, u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Avg,
    Min,
    Max,
    Sum,
}

pub fn parse(text: &str) -> Result<Query, ParseError> {
    Parser::new(tokenize(text)?).parse()
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    String(String),
    Binary(Vec<u8>),
    Symbol(Symbol),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Symbol {
    LeftBracket,
    RightBracket,
    LeftParen,
    RightParen,
    Comma,
    Equal,
    EqualEqual,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

fn tokenize(text: &str) -> Result<Vec<Token>, ParseError> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        if bytes[cursor] == b'x' && bytes.get(cursor + 1) == Some(&b'"') {
            let (binary, next) = binary_literal(bytes, cursor)?;
            tokens.push(Token::Binary(binary));
            cursor = next;
            continue;
        }
        if bytes[cursor] == b'"' {
            let (string, next) = string_literal(text, cursor)?;
            tokens.push(Token::String(string));
            cursor = next;
            continue;
        }
        let (symbol, length) = match bytes[cursor..] {
            [b'=', b'=', ..] => (Some(Symbol::EqualEqual), 2),
            [b'!', b'=', ..] => (Some(Symbol::NotEqual), 2),
            [b'>', b'=', ..] => (Some(Symbol::GreaterEqual), 2),
            [b'<', b'=', ..] => (Some(Symbol::LessEqual), 2),
            [b'[', ..] => (Some(Symbol::LeftBracket), 1),
            [b']', ..] => (Some(Symbol::RightBracket), 1),
            [b'(', ..] => (Some(Symbol::LeftParen), 1),
            [b')', ..] => (Some(Symbol::RightParen), 1),
            [b',', ..] => (Some(Symbol::Comma), 1),
            [b'=', ..] => (Some(Symbol::Equal), 1),
            [b'>', ..] => (Some(Symbol::Greater), 1),
            [b'<', ..] => (Some(Symbol::Less), 1),
            _ => (None, 0),
        };
        if let Some(symbol) = symbol {
            tokens.push(Token::Symbol(symbol));
            cursor += length;
            continue;
        }
        let start = cursor;
        while cursor < bytes.len()
            && !bytes[cursor].is_ascii_whitespace()
            && !b"[](),=!<>\"".contains(&bytes[cursor])
        {
            cursor += 1;
        }
        if cursor == start {
            return Err(ParseError::new("invalid character in query"));
        }
        tokens.push(Token::Word(text[start..cursor].to_owned()));
    }
    Ok(tokens)
}

fn string_literal(text: &str, start: usize) -> Result<(String, usize), ParseError> {
    let bytes = text.as_bytes();
    let mut output = String::new();
    let mut cursor = start + 1;
    let mut segment = cursor;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'"' => {
                output.push_str(&text[segment..cursor]);
                return Ok((output, cursor + 1));
            }
            b'\\' => {
                output.push_str(&text[segment..cursor]);
                cursor += 1;
                let escaped = *bytes
                    .get(cursor)
                    .ok_or_else(|| ParseError::new("unterminated string escape"))?;
                match escaped {
                    b'"' => output.push('"'),
                    b'\\' => output.push('\\'),
                    b'n' => output.push('\n'),
                    b'r' => output.push('\r'),
                    b't' => output.push('\t'),
                    b'u' => {
                        let end = cursor + 5;
                        let digits = bytes
                            .get(cursor + 1..end)
                            .ok_or_else(|| ParseError::new("short Unicode escape"))?;
                        let scalar = parse_hex(digits)?;
                        if (0xd800..=0xdfff).contains(&scalar) {
                            return Err(ParseError::new("surrogate Unicode escape"));
                        }
                        output.push(
                            char::from_u32(scalar)
                                .ok_or_else(|| ParseError::new("invalid Unicode escape"))?,
                        );
                        cursor = end - 1;
                    }
                    _ => return Err(ParseError::new("unknown string escape")),
                }
                cursor += 1;
                segment = cursor;
            }
            _ => cursor += 1,
        }
    }
    Err(ParseError::new("unterminated string"))
}

fn binary_literal(bytes: &[u8], start: usize) -> Result<(Vec<u8>, usize), ParseError> {
    let content_start = start + 2;
    let relative_end = bytes[content_start..]
        .iter()
        .position(|byte| *byte == b'"')
        .ok_or_else(|| ParseError::new("unterminated binary literal"))?;
    let end = content_start + relative_end;
    let digits = &bytes[content_start..end];
    if !digits.len().is_multiple_of(2) {
        return Err(ParseError::new("binary literal has an odd digit count"));
    }
    let mut output = Vec::with_capacity(digits.len() / 2);
    for pair in digits.chunks_exact(2) {
        output.push(u8::try_from(parse_hex(pair)?).expect("two hex digits fit u8"));
    }
    Ok((output, end + 1))
}

fn parse_hex(bytes: &[u8]) -> Result<u32, ParseError> {
    let mut value = 0_u32;
    for &byte in bytes {
        let nibble = match byte {
            b'0'..=b'9' => u32::from(byte - b'0'),
            b'a'..=b'f' => u32::from(byte - b'a' + 10),
            b'A'..=b'F' => u32::from(byte - b'A' + 10),
            _ => return Err(ParseError::new("invalid hexadecimal digit")),
        };
        value = value
            .checked_mul(16)
            .and_then(|item| item.checked_add(nibble))
            .ok_or_else(|| ParseError::new("hexadecimal value is too large"))?;
    }
    Ok(value)
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0 }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "clause uniqueness and mode validation are clearest in one dispatch loop"
    )]
    fn parse(mut self) -> Result<Query, ParseError> {
        let mode = self.word()?.to_ascii_uppercase();
        let source = match mode.as_str() {
            "EVENTS" => Source::Events {
                pattern: self.take_primary_identifier(),
            },
            "LOGS" => Source::Logs {
                origins: Vec::new(),
                error_only: false,
                containing: None,
            },
            "METRIC" => Source::Metric {
                name: self.pattern_identifier()?,
                labels: self.label_selector()?,
            },
            _ => {
                return Err(ParseError::new(
                    "query must begin with EVENTS, LOGS or METRIC",
                ));
            }
        };
        let mut query = Query {
            source,
            since: None,
            until: None,
            predicates: Vec::new(),
            cross_filters: Vec::new(),
            sort: Vec::new(),
            take: None,
            skip: 0,
            select: Vec::new(),
            aggregate: None,
            transform: None,
            metric_aggregate: None,
            stream: false,
            index: None,
        };
        let mut seen_skip = false;
        while self.cursor < self.tokens.len() {
            let keyword = self.word()?.to_ascii_uppercase();
            match keyword.as_str() {
                "SINCE" => {
                    ensure_absent(query.since.as_ref(), "SINCE")?;
                    query.since = Some(self.time()?);
                }
                "UNTIL" => {
                    ensure_absent(query.until.as_ref(), "UNTIL")?;
                    query.until = Some(self.time()?);
                }
                "WHERE" => {
                    if self.peek_keyword("METRIC")
                        || self.peek_keyword("EVENT")
                        || self.peek_keyword("LOG")
                    {
                        query.cross_filters.push(self.cross_filter()?);
                    } else {
                        query.predicates.push(self.expression()?);
                    }
                }
                "SORT" => {
                    if !query.sort.is_empty() {
                        return Err(ParseError::new("SORT appears more than once"));
                    }
                    query.sort = self.sort_keys()?;
                }
                "TAKE" => {
                    ensure_absent(query.take.as_ref(), "TAKE")?;
                    query.take = Some(self.count()?);
                }
                "SKIP" => {
                    if seen_skip {
                        return Err(ParseError::new("SKIP appears more than once"));
                    }
                    seen_skip = true;
                    query.skip = self.count()?;
                }
                "SELECT" => {
                    ensure_record_mode(&query.source, "SELECT")?;
                    query.select.extend(self.field_list()?);
                }
                "COUNT" if self.consume_keyword("BY") => {
                    let field = self.field()?;
                    Self::set_record_aggregate(&mut query, RecordAggregate::CountBy(field))?;
                }
                "TOP" => {
                    let count = self.count()?;
                    self.expect_keyword("BY")?;
                    let field = self.field()?;
                    Self::set_record_aggregate(
                        &mut query,
                        RecordAggregate::TopBy { count, field },
                    )?;
                }
                "DISTINCT" => {
                    let field = self.field()?;
                    Self::set_record_aggregate(&mut query, RecordAggregate::Distinct(field))?;
                }
                "GROUP" => {
                    let fields = self.field_list()?;
                    let function = self.group_function()?;
                    Self::set_record_aggregate(
                        &mut query,
                        RecordAggregate::Group { fields, function },
                    )?;
                }
                "STREAM" => {
                    if query.stream {
                        return Err(ParseError::new("STREAM appears more than once"));
                    }
                    query.stream = true;
                }
                "FROM" => self.parse_origins(&mut query)?,
                "ERROR" => {
                    self.expect_keyword("ONLY")?;
                    Self::set_error_only(&mut query)?;
                }
                "CONTAINING" => self.set_containing(&mut query)?,
                "INDEX" => {
                    if !matches!(query.source, Source::Events { .. }) || query.index.is_some() {
                        return Err(ParseError::new("INDEX is valid once in EVENTS mode"));
                    }
                    query.index = Some(self.field()?);
                }
                "RATE" | "DELTA" | "P50" | "P95" | "P99" => {
                    if query.transform.is_some() || !matches!(query.source, Source::Metric { .. }) {
                        return Err(ParseError::new("invalid or repeated metric transform"));
                    }
                    query.transform = Some(match keyword.as_str() {
                        "RATE" => Transform::Rate,
                        "DELTA" => Transform::Delta,
                        "P50" => Transform::Percentile(50),
                        "P95" => Transform::Percentile(95),
                        _ => Transform::Percentile(99),
                    });
                }
                "AVG" | "MIN" | "MAX" | "SUM" => {
                    Self::set_metric_aggregate(
                        &mut query,
                        MetricAggregate::Scalar(parse_aggregate_function(&keyword)),
                    )?;
                }
                "AVG_OVER" | "MIN_OVER" | "MAX_OVER" | "SUM_OVER" => {
                    let duration = self.duration()?;
                    Self::set_metric_aggregate(
                        &mut query,
                        MetricAggregate::Window(
                            parse_aggregate_function(keyword.trim_end_matches("_OVER")),
                            duration,
                        ),
                    )?;
                }
                _ => return Err(ParseError::new(format!("unknown query clause {keyword}"))),
            }
        }
        validate_combinations(&query)?;
        Ok(query)
    }

    fn take_primary_identifier(&mut self) -> Option<String> {
        let token = self.tokens.get(self.cursor)?;
        let value = match token {
            Token::Word(value) if !is_clause(value) => value.clone(),
            Token::String(value) => value.clone(),
            _ => return None,
        };
        self.cursor += 1;
        Some(value)
    }

    fn label_selector(&mut self) -> Result<Option<Vec<Expr>>, ParseError> {
        if !self.consume_symbol(Symbol::LeftBracket) {
            return Ok(None);
        }
        let mut predicates = Vec::new();
        if self.consume_symbol(Symbol::RightBracket) {
            return Ok(Some(predicates));
        }
        loop {
            predicates.push(self.simple_predicate(true)?);
            if self.consume_symbol(Symbol::RightBracket) {
                break;
            }
            self.expect_symbol(Symbol::Comma)?;
        }
        Ok(Some(predicates))
    }

    fn cross_filter(&mut self) -> Result<CrossFilter, ParseError> {
        let kind = self.word()?.to_ascii_uppercase();
        match kind.as_str() {
            "METRIC" => {
                let name = self.pattern_identifier()?;
                let labels = self.label_selector()?;
                let operator = self.comparison_operator(false)?;
                if matches!(
                    operator,
                    Operator::StartsWith
                        | Operator::EndsWith
                        | Operator::Contains
                        | Operator::Has
                ) {
                    return Err(ParseError::new(
                        "cross-type metric comparison requires a numeric operator",
                    ));
                }
                let value = self.literal()?;
                if !matches!(
                    value,
                    Literal::Signed(_) | Literal::Unsigned(_) | Literal::Float(_)
                ) {
                    return Err(ParseError::new(
                        "cross-type metric comparison requires a number",
                    ));
                }
                Ok(CrossFilter::Metric {
                    name,
                    labels,
                    operator,
                    value,
                })
            }
            "EVENT" => {
                let pattern = self.pattern_identifier()?;
                self.expect_keyword("EXISTS")?;
                Ok(CrossFilter::EventExists { pattern })
            }
            "LOG" => {
                let origin = self.identifier()?;
                let containing = if self.consume_keyword("CONTAINING") {
                    Some(self.identifier()?)
                } else {
                    None
                };
                self.expect_keyword("EXISTS")?;
                Ok(CrossFilter::LogExists { origin, containing })
            }
            _ => unreachable!("caller checked cross-filter keyword"),
        }
    }

    fn expression(&mut self) -> Result<Expr, ParseError> {
        self.or_expression()
    }

    fn or_expression(&mut self) -> Result<Expr, ParseError> {
        let mut expression = self.and_expression()?;
        while self.consume_keyword("OR") {
            expression = Expr::Or(Box::new(expression), Box::new(self.and_expression()?));
        }
        Ok(expression)
    }

    fn and_expression(&mut self) -> Result<Expr, ParseError> {
        let mut expression = self.predicate_atom()?;
        while self.consume_keyword("AND") {
            expression = Expr::And(Box::new(expression), Box::new(self.predicate_atom()?));
        }
        Ok(expression)
    }

    fn predicate_atom(&mut self) -> Result<Expr, ParseError> {
        if self.consume_symbol(Symbol::LeftParen) {
            let expression = self.expression()?;
            self.expect_symbol(Symbol::RightParen)?;
            return Ok(expression);
        }
        self.simple_predicate(false)
    }

    fn simple_predicate(&mut self, label: bool) -> Result<Expr, ParseError> {
        let field = self.field()?;
        if self.consume_keyword("IS") {
            let negated = self.consume_keyword("NOT");
            self.expect_keyword("NULL")?;
            return Ok(Expr::Null { field, negated });
        }
        if self.consume_keyword("IN") || self.consume_keyword("NOT_IN") {
            let negated = matches!(self.tokens.get(self.cursor - 1), Some(Token::Word(word)) if word.eq_ignore_ascii_case("NOT_IN"));
            self.expect_symbol(Symbol::LeftParen)?;
            let mut values = Vec::new();
            loop {
                values.push(self.literal()?);
                if self.consume_symbol(Symbol::RightParen) {
                    break;
                }
                self.expect_symbol(Symbol::Comma)?;
            }
            return Ok(Expr::In {
                field,
                negated,
                values,
            });
        }
        let operator = self.comparison_operator(label)?;
        let value = self.literal()?;
        if matches!(value, Literal::Binary(_))
            && matches!(
                operator,
                Operator::Greater | Operator::GreaterEqual | Operator::Less | Operator::LessEqual
            )
        {
            return Err(ParseError::new("binary values cannot be ordered"));
        }
        Ok(Expr::Compare {
            field,
            operator,
            value,
        })
    }

    fn comparison_operator(&mut self, label: bool) -> Result<Operator, ParseError> {
        let operator = if self.consume_symbol(Symbol::EqualEqual)
            || (label && self.consume_symbol(Symbol::Equal))
        {
            Operator::Equal
        } else if self.consume_symbol(Symbol::NotEqual) {
            Operator::NotEqual
        } else if self.consume_symbol(Symbol::GreaterEqual) {
            Operator::GreaterEqual
        } else if self.consume_symbol(Symbol::Greater) {
            Operator::Greater
        } else if self.consume_symbol(Symbol::LessEqual) {
            Operator::LessEqual
        } else if self.consume_symbol(Symbol::Less) {
            Operator::Less
        } else if self.consume_keyword("STARTS_WITH") {
            Operator::StartsWith
        } else if self.consume_keyword("ENDS_WITH") {
            Operator::EndsWith
        } else if self.consume_keyword("CONTAINS") {
            Operator::Contains
        } else if self.consume_keyword("HAS") {
            Operator::Has
        } else {
            return Err(ParseError::new("expected comparison operator"));
        };
        Ok(operator)
    }

    fn literal(&mut self) -> Result<Literal, ParseError> {
        match self
            .next()
            .ok_or_else(|| ParseError::new("missing literal"))?
        {
            Token::String(value) => Ok(Literal::String(value)),
            Token::Binary(value) => Ok(Literal::Binary(value)),
            Token::Word(value) if value.eq_ignore_ascii_case("true") => Ok(Literal::Bool(true)),
            Token::Word(value) if value.eq_ignore_ascii_case("false") => Ok(Literal::Bool(false)),
            Token::Word(value) if value.eq_ignore_ascii_case("NULL") => {
                Err(ParseError::new("NULL is valid only with IS"))
            }
            Token::Word(value) => parse_number(&value).or(Ok(Literal::String(value))),
            Token::Symbol(_) => Err(ParseError::new("expected literal")),
        }
    }

    fn time(&mut self) -> Result<TimeExpr, ParseError> {
        let value = self.word()?;
        if value.eq_ignore_ascii_case("today") {
            return Ok(TimeExpr::Today);
        }
        if value.eq_ignore_ascii_case("yesterday") {
            return Ok(TimeExpr::Yesterday);
        }
        if let Ok(nanoseconds) = parse_duration(&value) {
            let direction = self.word()?;
            return match direction.to_ascii_lowercase().as_str() {
                "ago" => Ok(TimeExpr::Relative {
                    nanoseconds,
                    future: false,
                }),
                "hence" => Ok(TimeExpr::Relative {
                    nanoseconds,
                    future: true,
                }),
                _ => Err(ParseError::new("duration time needs ago or hence")),
            };
        }
        validate_absolute_time(&value)?;
        Ok(TimeExpr::Absolute(value))
    }

    fn duration(&mut self) -> Result<u64, ParseError> {
        parse_duration(&self.word()?)
    }

    fn sort_keys(&mut self) -> Result<Vec<SortKey>, ParseError> {
        let mut keys = Vec::new();
        loop {
            let field = self.field()?;
            let descending = if self.consume_keyword("DESC") {
                true
            } else {
                self.consume_keyword("ASC");
                false
            };
            keys.push(SortKey { field, descending });
            if !self.consume_symbol(Symbol::Comma) {
                break;
            }
        }
        Ok(keys)
    }

    fn field_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut fields = vec![self.field()?];
        while self.consume_symbol(Symbol::Comma) {
            fields.push(self.field()?);
        }
        Ok(fields)
    }

    fn group_function(&mut self) -> Result<GroupFunction, ParseError> {
        let function = self.word()?.to_ascii_uppercase();
        Ok(match function.as_str() {
            "COUNT" => GroupFunction::Count,
            "SUM" => GroupFunction::Sum(self.field()?),
            "AVG" => GroupFunction::Avg(self.field()?),
            "MIN" => GroupFunction::Min(self.field()?),
            "MAX" => GroupFunction::Max(self.field()?),
            _ => return Err(ParseError::new("invalid GROUP function")),
        })
    }

    fn parse_origins(&mut self, query: &mut Query) -> Result<(), ParseError> {
        let Source::Logs { origins, .. } = &mut query.source else {
            return Err(ParseError::new("FROM is valid only in LOGS mode"));
        };
        if !origins.is_empty() {
            return Err(ParseError::new("FROM appears more than once"));
        }
        origins.push(self.identifier()?);
        while self.consume_symbol(Symbol::Comma) {
            origins.push(self.identifier()?);
        }
        Ok(())
    }

    fn set_error_only(query: &mut Query) -> Result<(), ParseError> {
        let Source::Logs { error_only, .. } = &mut query.source else {
            return Err(ParseError::new("ERROR ONLY is valid only in LOGS mode"));
        };
        if *error_only {
            return Err(ParseError::new("ERROR ONLY appears more than once"));
        }
        *error_only = true;
        Ok(())
    }

    fn set_containing(&mut self, query: &mut Query) -> Result<(), ParseError> {
        let value = self.identifier()?;
        let Source::Logs { containing, .. } = &mut query.source else {
            return Err(ParseError::new("CONTAINING is valid only in LOGS mode"));
        };
        if containing.replace(value).is_some() {
            return Err(ParseError::new("CONTAINING appears more than once"));
        }
        Ok(())
    }

    fn set_record_aggregate(
        query: &mut Query,
        aggregate: RecordAggregate,
    ) -> Result<(), ParseError> {
        ensure_record_mode(&query.source, "record aggregation")?;
        if query.aggregate.replace(aggregate).is_some() {
            return Err(ParseError::new("record aggregation appears more than once"));
        }
        Ok(())
    }

    fn set_metric_aggregate(
        query: &mut Query,
        aggregate: MetricAggregate,
    ) -> Result<(), ParseError> {
        if !matches!(query.source, Source::Metric { .. })
            || query.metric_aggregate.replace(aggregate).is_some()
        {
            return Err(ParseError::new("invalid or repeated metric aggregation"));
        }
        Ok(())
    }

    fn count(&mut self) -> Result<u64, ParseError> {
        let word = self.word()?;
        if word.starts_with('-')
            || word.starts_with("0x")
            || word.bytes().any(|byte| !byte.is_ascii_digit())
        {
            return Err(ParseError::new("count must be an unsigned decimal integer"));
        }
        word.parse()
            .map_err(|_| ParseError::new("count exceeds u64"))
    }

    fn field(&mut self) -> Result<String, ParseError> {
        let value = self.identifier()?;
        if valid_identifier(&value) {
            Ok(value)
        } else {
            Err(ParseError::new("invalid field identifier"))
        }
    }

    fn identifier(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Token::Word(value) | Token::String(value)) => Ok(value),
            _ => Err(ParseError::new("expected identifier or string")),
        }
    }

    fn pattern_identifier(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Token::Word(value)) if !is_clause(&value) => Ok(value),
            Some(Token::String(value)) => Ok(value),
            _ => Err(ParseError::new("expected identifier pattern")),
        }
    }

    fn word(&mut self) -> Result<String, ParseError> {
        match self.next() {
            Some(Token::Word(value)) => Ok(value),
            _ => Err(ParseError::new("expected keyword")),
        }
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<(), ParseError> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(ParseError::new(format!("expected {expected}")))
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        if matches!(self.tokens.get(self.cursor), Some(Token::Word(word)) if word.eq_ignore_ascii_case(expected))
        {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_symbol(&mut self, expected: Symbol) -> Result<(), ParseError> {
        if self.consume_symbol(expected) {
            Ok(())
        } else {
            Err(ParseError::new("expected punctuation"))
        }
    }

    fn consume_symbol(&mut self, expected: Symbol) -> bool {
        if self.tokens.get(self.cursor) == Some(&Token::Symbol(expected)) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor)?.clone();
        self.cursor += 1;
        Some(token)
    }

    fn peek_keyword(&self, expected: &str) -> bool {
        matches!(self.tokens.get(self.cursor), Some(Token::Word(word)) if word.eq_ignore_ascii_case(expected))
    }
}

fn ensure_absent<T>(value: Option<&T>, clause: &str) -> Result<(), ParseError> {
    if value.is_some() {
        Err(ParseError::new(format!("{clause} appears more than once")))
    } else {
        Ok(())
    }
}

fn ensure_record_mode(source: &Source, clause: &str) -> Result<(), ParseError> {
    if matches!(source, Source::Metric { .. }) {
        Err(ParseError::new(format!(
            "{clause} is not valid in METRIC mode"
        )))
    } else {
        Ok(())
    }
}

fn validate_combinations(query: &Query) -> Result<(), ParseError> {
    validate_fixed_fields(query)?;
    if query.index.is_some()
        && (query.since.is_some()
            || query.until.is_some()
            || !query.predicates.is_empty()
            || !query.cross_filters.is_empty()
            || !query.sort.is_empty()
            || query.take.is_some()
            || query.skip != 0
            || !query.select.is_empty()
            || query.aggregate.is_some()
            || query.stream)
    {
        return Err(ParseError::new(
            "INDEX cannot be combined with query clauses",
        ));
    }
    if !query.cross_filters.is_empty() && query.since.is_none() {
        return Err(ParseError::new("cross-type filters require SINCE"));
    }
    for filter in &query.cross_filters {
        let valid = matches!(
            (&query.source, filter),
            (
                Source::Events { .. } | Source::Logs { .. },
                CrossFilter::Metric { .. }
            ) | (
                Source::Logs { .. } | Source::Metric { .. },
                CrossFilter::EventExists { .. }
            ) | (
                Source::Events { .. } | Source::Metric { .. },
                CrossFilter::LogExists { .. }
            )
        );
        if !valid {
            return Err(ParseError::new(
                "cross-type filter is not valid for this source",
            ));
        }
    }
    if query.aggregate.is_some() && !query.select.is_empty() {
        return Err(ParseError::new(
            "SELECT cannot be combined with aggregation",
        ));
    }
    if query.stream {
        if matches!(query.source, Source::Metric { .. }) {
            return Err(ParseError::new("METRIC queries cannot stream"));
        }
        if query.until.is_some() {
            return Err(ParseError::new("STREAM cannot be combined with UNTIL"));
        }
        if matches!(
            query.aggregate,
            Some(
                RecordAggregate::CountBy(_)
                    | RecordAggregate::TopBy { .. }
                    | RecordAggregate::Group { .. }
            )
        ) {
            return Err(ParseError::new("this aggregation cannot stream"));
        }
        if matches!(query.aggregate, Some(RecordAggregate::Distinct(_)))
            && (!query.sort.is_empty() || query.take.is_some() || query.skip != 0)
        {
            return Err(ParseError::new("DISTINCT STREAM cannot sort, take or skip"));
        }
    }
    if matches!(query.metric_aggregate, Some(MetricAggregate::Window(_, _)))
        && query.since.is_none()
    {
        return Err(ParseError::new("window aggregation requires SINCE"));
    }
    Ok(())
}

fn validate_fixed_fields(query: &Query) -> Result<(), ParseError> {
    const LOG_FIELDS: [&str; 6] = [
        "timestamp",
        "origin",
        "is_error",
        "message",
        "boot_id",
        "job_id",
    ];
    let Source::Logs { .. } = query.source else {
        return Ok(());
    };
    let mut fields = Vec::new();
    for predicate in &query.predicates {
        predicate.fields(&mut fields);
    }
    fields.extend(query.sort.iter().map(|key| key.field.clone()));
    fields.extend(query.select.iter().cloned());
    if let Some(aggregate) = &query.aggregate {
        match aggregate {
            RecordAggregate::CountBy(field)
            | RecordAggregate::Distinct(field)
            | RecordAggregate::TopBy { field, .. } => fields.push(field.clone()),
            RecordAggregate::Group {
                fields: group_fields,
                function,
            } => {
                fields.extend(group_fields.iter().cloned());
                if let GroupFunction::Sum(field)
                | GroupFunction::Avg(field)
                | GroupFunction::Min(field)
                | GroupFunction::Max(field) = function
                {
                    fields.push(field.clone());
                }
            }
        }
    }
    if let Some(field) = fields
        .iter()
        .find(|field| !LOG_FIELDS.contains(&field.as_str()))
    {
        return Err(ParseError::new(format!("unknown log field {field}")));
    }
    Ok(())
}

fn is_clause(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "SINCE"
            | "UNTIL"
            | "WHERE"
            | "SORT"
            | "TAKE"
            | "SKIP"
            | "SELECT"
            | "COUNT"
            | "TOP"
            | "DISTINCT"
            | "GROUP"
            | "STREAM"
            | "INDEX"
    )
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.split('.').all(valid_identifier_segment)
}

fn valid_identifier_segment(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn parse_number(value: &str) -> Result<Literal, ParseError> {
    if let Some(hex) = value.strip_prefix("0x") {
        if hex.is_empty() {
            return Err(ParseError::new("empty hexadecimal integer"));
        }
        return u64::from_str_radix(hex, 16)
            .map(Literal::Unsigned)
            .map_err(|_| ParseError::new("invalid hexadecimal integer"));
    }
    if value.contains('.') || value.contains('e') || value.contains('E') {
        let number: f64 = value
            .parse()
            .map_err(|_| ParseError::new("invalid floating-point literal"))?;
        if !number.is_finite() {
            return Err(ParseError::new("floating-point literal is not finite"));
        }
        return Ok(Literal::Float(number));
    }
    if value.starts_with('-') {
        value
            .parse()
            .map(Literal::Signed)
            .map_err(|_| ParseError::new("signed integer exceeds i64"))
    } else {
        value
            .parse()
            .map(Literal::Unsigned)
            .map_err(|_| ParseError::new("not a numeric literal"))
    }
}

fn parse_duration(value: &str) -> Result<u64, ParseError> {
    let (digits, unit) = value.split_at(value.len().saturating_sub(1));
    if digits.is_empty() || digits.bytes().any(|byte| !byte.is_ascii_digit()) {
        return Err(ParseError::new("invalid duration"));
    }
    let count: u64 = digits
        .parse()
        .map_err(|_| ParseError::new("duration is too large"))?;
    if count == 0 {
        return Err(ParseError::new("duration must be non-zero"));
    }
    let seconds = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return Err(ParseError::new("invalid duration unit")),
    };
    count
        .checked_mul(seconds)
        .and_then(|item| item.checked_mul(1_000_000_000))
        .ok_or_else(|| ParseError::new("duration is too large"))
}

fn validate_absolute_time(value: &str) -> Result<(), ParseError> {
    let valid_shape = (value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-'))
        || (value.len() == 19
            && value.as_bytes().get(4) == Some(&b'-')
            && value.as_bytes().get(7) == Some(&b'-')
            && value.as_bytes().get(10) == Some(&b'T')
            && value.as_bytes().get(13) == Some(&b':')
            && value.as_bytes().get(16) == Some(&b':'));
    if valid_shape {
        Ok(())
    } else {
        Err(ParseError::new("invalid absolute time"))
    }
}

const fn parse_aggregate_function(value: &str) -> AggregateFunction {
    match value.as_bytes() {
        b"AVG" => AggregateFunction::Avg,
        b"MIN" => AggregateFunction::Min,
        b"MAX" => AggregateFunction::Max,
        _ => AggregateFunction::Sum,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_event_query_with_reordered_clauses() {
        let query = parse(
            "EVENTS kacs.* TAKE 100 WHERE origin_class == kacs SINCE 1h ago SORT timestamp DESC",
        )
        .unwrap();
        assert_eq!(query.take, Some(100));
        assert!(matches!(query.since, Some(TimeExpr::Relative { .. })));
        assert_eq!(query.sort[0].field, "timestamp");
    }

    #[test]
    fn parses_log_selectors_and_escaped_string() {
        let query =
            parse("LOGS CONTAINING \"failed\\nopen\" FROM loregd, peinit ERROR ONLY").unwrap();
        let Source::Logs {
            origins,
            error_only,
            containing,
        } = query.source
        else {
            panic!("log source")
        };
        assert_eq!(origins, ["loregd", "peinit"]);
        assert!(error_only);
        assert_eq!(containing.as_deref(), Some("failed\nopen"));
    }

    #[test]
    fn parses_array_containment_alongside_string_containment() {
        let query = parse("EVENTS WHERE subject.token.groups HAS x\"010200000000000520\"").unwrap();
        let Expr::Compare {
            field,
            operator,
            value,
        } = &query.predicates[0]
        else {
            panic!("comparison")
        };
        assert_eq!(field, "subject.token.groups");
        assert_eq!(*operator, Operator::Has);
        assert_eq!(*value, Literal::Binary(vec![1, 2, 0, 0, 0, 0, 0, 5, 32]));

        // CONTAINS keeps its own meaning, and neither operator is
        // acceptable in a cross-type metric comparison.
        let string = parse("LOGS WHERE message CONTAINS \"denied\"").unwrap();
        assert!(matches!(
            &string.predicates[0],
            Expr::Compare {
                operator: Operator::Contains,
                ..
            }
        ));
        assert!(parse("EVENTS SINCE 1h ago WHERE METRIC cpu HAS 1").is_err());
    }

    #[test]
    fn a_producer_origin_is_selectable_as_a_quoted_string() {
        // `jobs/<guid>` and `svc/ExecStartPre[0]` are accepted origins
        // (PSPU §3.7) but not identifiers, so FROM takes them quoted.
        let query = parse(
            "LOGS FROM \"jellyfin/ExecStartPre[0]\", \
             \"jobs/0f8fad5b-d9cb-469f-a165-70867728950e\"",
        )
        .unwrap();
        let Source::Logs { origins, .. } = query.source else {
            panic!("log source")
        };
        assert_eq!(
            origins,
            [
                "jellyfin/ExecStartPre[0]",
                "jobs/0f8fad5b-d9cb-469f-a165-70867728950e"
            ]
        );
    }

    #[test]
    fn rejects_unknown_log_fields_during_parsing() {
        assert!(parse("LOGS WHERE payload.secret == 1").is_err());
        assert!(parse("LOGS SELECT message, imaginary").is_err());
    }

    #[test]
    fn field_paths_require_valid_flattened_segments() {
        assert!(parse("EVENTS WHERE source.name == value").is_ok());
        assert!(parse("EVENTS WHERE source..name == value").is_err());
        assert!(parse("EVENTS WHERE source.1name == value").is_err());
    }

    #[test]
    fn parses_metric_pipeline() {
        let query = parse(
            "METRIC cpu.usage[core=\"0\"] RATE SINCE 1h ago SUM_OVER 5m WHERE boot_id IS NOT NULL",
        )
        .unwrap();
        assert_eq!(query.transform, Some(Transform::Rate));
        assert!(matches!(
            query.metric_aggregate,
            Some(MetricAggregate::Window(AggregateFunction::Sum, _))
        ));
    }

    #[test]
    fn parses_and_validates_cross_type_filters() {
        let query = parse(
            "EVENTS kacs.* SINCE 1h ago WHERE METRIC cpu.usage[core=\"0\"] > 80 \
             WHERE LOG loregd CONTAINING \"error\" EXISTS",
        )
        .unwrap();
        assert!(matches!(
            &query.cross_filters[0],
            CrossFilter::Metric { name, labels: Some(labels), operator: Operator::Greater, .. }
                if name == "cpu.usage" && labels.len() == 1
        ));
        assert!(matches!(
            &query.cross_filters[1],
            CrossFilter::LogExists { origin, containing: Some(text) }
                if origin == "loregd" && text == "error"
        ));

        assert!(parse("LOGS WHERE EVENT kacs.denied EXISTS").is_err());
        assert!(parse("EVENTS SINCE 1h ago WHERE EVENT kacs.denied EXISTS").is_err());
        assert!(parse("LOGS SINCE 1h ago WHERE METRIC cpu CONTAINS 1").is_err());
        assert!(parse("LOGS SINCE 1h ago WHERE EVENT kacs.* EXISTS").is_ok());
        assert!(parse("METRIC cpu.* SINCE 1h ago").is_ok());
    }

    #[test]
    fn rejects_invalid_combinations_and_binary_ordering() {
        assert!(parse("METRIC cpu STREAM").is_err());
        assert!(parse("EVENTS DISTINCT event_type STREAM TAKE 1").is_err());
        assert!(parse("EVENTS WHERE payload > x\"01\"").is_err());
    }
}
