//! Prometheus text exposition format (`text/plain; version=0.0.4`): the
//! decoder/encoder pair around `github.com/prometheus/common/expfmt`.
//!
//! `parse_text` is `TextParser.TextToMetricFamilies` and `encode_family` is
//! `MetricFamilyToText`; together they let the executors metric store keep its
//! wire format: `internal/metrics/store` stores text blobs produced by
//! `expfmt` and re-encodes the decoded families when Prometheus scrapes the
//! worker's metrics server.
//!
//! Deliberate differences from the baseline (recorded in
//! docs/modules/06_worker.md §22):
//!
//! * families come back in first-appearance order instead of map iteration
//!   order (Go's `Decoder` walks a map, so its output order is random);
//! * UTF-8 quoted metric/label names (`{"foo"}`) are not parsed. The store's
//!   `escaping=values` encoder only produces them for names that are invalid
//!   under the legacy scheme, which the executors' metrics never are;
//! * `EscapeMetricFamily` is not applied: legacy-valid names are the identity
//!   under the `values` escaping scheme;
//! * error messages that embed a proto message (`MetricFamily has no
//!   metrics: %s`) are abbreviated.

use std::collections::HashMap;
use std::fmt;

/// `dto.MetricType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Summary,
    Untyped,
    Histogram,
}

impl MetricType {
    /// The `# TYPE` spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Summary => "summary",
            Self::Untyped => "untyped",
            Self::Histogram => "histogram",
        }
    }

    /// `dto.MetricType_value[strings.ToUpper(token)]`.
    fn from_token(token: &str) -> Option<Self> {
        match token.to_ascii_uppercase().as_str() {
            "COUNTER" => Some(Self::Counter),
            "GAUGE" => Some(Self::Gauge),
            "SUMMARY" => Some(Self::Summary),
            "UNTYPED" => Some(Self::Untyped),
            "HISTOGRAM" => Some(Self::Histogram),
            _ => None,
        }
    }
}

/// `dto.LabelPair`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelPair {
    pub name: String,
    pub value: String,
}

/// `dto.Quantile`.
#[derive(Debug, Clone, PartialEq)]
pub struct Quantile {
    pub quantile: f64,
    pub value: f64,
}

/// `dto.Bucket`.
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    pub upper_bound: f64,
    pub cumulative_count: u64,
}

/// `dto.Summary`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Summary {
    pub quantiles: Vec<Quantile>,
    pub sample_sum: f64,
    pub sample_count: u64,
}

/// `dto.Histogram`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Histogram {
    pub buckets: Vec<Bucket>,
    pub sample_sum: f64,
    pub sample_count: u64,
}

/// `dto.Metric`. `summary`/`histogram` are set for those family types,
/// otherwise exactly one of `counter`/`gauge`/`untyped`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Metric {
    pub labels: Vec<LabelPair>,
    pub timestamp_ms: Option<i64>,
    pub counter: Option<f64>,
    pub gauge: Option<f64>,
    pub untyped: Option<f64>,
    pub summary: Option<Summary>,
    pub histogram: Option<Histogram>,
}

/// `dto.MetricFamily`.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricFamily {
    pub name: String,
    pub help: Option<String>,
    pub metric_type: MetricType,
    pub metrics: Vec<Metric>,
}

/// `expfmt.ParseError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "text format parsing error in line {}: {}",
            self.line, self.message
        )
    }
}

impl std::error::Error for ParseError {}

/// `TextParser.TextToMetricFamilies`, returning families in first-appearance
/// order.
pub fn parse_text(input: &str) -> Result<Vec<MetricFamily>, ParseError> {
    // The baseline's byte parser turns EOF in the middle of a line into
    // `unexpected end of input stream`; only a newline-terminated stream is
    // complete.
    let body = match input.strip_suffix('\n') {
        Some(body) => body,
        None if input.is_empty() => return Ok(Vec::new()),
        None => {
            let line = input.bytes().filter(|byte| *byte == b'\n').count() + 1;
            return Err(ParseError {
                line,
                message: "unexpected end of input stream".to_owned(),
            });
        }
    };

    let mut parser = Parser::new();
    for raw in body.split('\n') {
        parser.line += 1;
        let line = raw.trim_start_matches([' ', '\t']);
        if line.is_empty() {
            continue; // `startOfLine`'s empty-line case.
        }
        if let Some(comment) = line.strip_prefix('#') {
            parser.parse_comment(comment)?;
        } else {
            parser.parse_sample(line)?;
        }
    }

    let mut families = Vec::new();
    for family in parser.families {
        if family.metrics.is_empty() {
            // `TextToMetricFamilies` deletes empty metric families.
            continue;
        }
        families.push(MetricFamily {
            name: family.name,
            help: family.help,
            metric_type: family.metric_type.unwrap_or(MetricType::Untyped),
            metrics: family.metrics,
        });
    }
    Ok(families)
}

/// `MetricFamilyToText` for one family.
pub fn encode_family(family: &MetricFamily) -> Result<String, String> {
    if family.metrics.is_empty() {
        return Err(format!("MetricFamily has no metrics: {}", family.name));
    }
    if family.name.is_empty() {
        return Err("MetricFamily has no name".to_owned());
    }

    let mut out = String::new();
    if let Some(help) = &family.help {
        out.push_str("# HELP ");
        write_name(&mut out, &family.name);
        out.push(' ');
        out.push_str(&escape_string(help, false));
        out.push('\n');
    }
    out.push_str("# TYPE ");
    write_name(&mut out, &family.name);
    out.push(' ');
    out.push_str(family.metric_type.as_str());
    out.push('\n');

    for metric in &family.metrics {
        match family.metric_type {
            MetricType::Counter => {
                let value = metric
                    .counter
                    .ok_or_else(|| format!("expected counter in metric {}", family.name))?;
                write_sample(
                    &mut out,
                    &family.name,
                    "",
                    &metric.labels,
                    None,
                    value,
                    metric.timestamp_ms,
                );
            }
            MetricType::Gauge => {
                let value = metric
                    .gauge
                    .ok_or_else(|| format!("expected gauge in metric {}", family.name))?;
                write_sample(
                    &mut out,
                    &family.name,
                    "",
                    &metric.labels,
                    None,
                    value,
                    metric.timestamp_ms,
                );
            }
            MetricType::Untyped => {
                let value = metric
                    .untyped
                    .ok_or_else(|| format!("expected untyped in metric {}", family.name))?;
                write_sample(
                    &mut out,
                    &family.name,
                    "",
                    &metric.labels,
                    None,
                    value,
                    metric.timestamp_ms,
                );
            }
            MetricType::Summary => {
                let summary = metric
                    .summary
                    .as_ref()
                    .ok_or_else(|| format!("expected summary in metric {}", family.name))?;
                for quantile in &summary.quantiles {
                    write_sample(
                        &mut out,
                        &family.name,
                        "",
                        &metric.labels,
                        Some(("quantile", quantile.quantile)),
                        quantile.value,
                        metric.timestamp_ms,
                    );
                }
                write_sample(
                    &mut out,
                    &family.name,
                    "_sum",
                    &metric.labels,
                    None,
                    summary.sample_sum,
                    metric.timestamp_ms,
                );
                write_sample(
                    &mut out,
                    &family.name,
                    "_count",
                    &metric.labels,
                    None,
                    summary.sample_count as f64,
                    metric.timestamp_ms,
                );
            }
            MetricType::Histogram => {
                let histogram = metric
                    .histogram
                    .as_ref()
                    .ok_or_else(|| format!("expected histogram in metric {}", family.name))?;
                let mut inf_seen = false;
                for bucket in &histogram.buckets {
                    write_sample(
                        &mut out,
                        &family.name,
                        "_bucket",
                        &metric.labels,
                        Some(("le", bucket.upper_bound)),
                        bucket.cumulative_count as f64,
                        metric.timestamp_ms,
                    );
                    if bucket.upper_bound == f64::INFINITY {
                        inf_seen = true;
                    }
                }
                if !inf_seen {
                    write_sample(
                        &mut out,
                        &family.name,
                        "_bucket",
                        &metric.labels,
                        Some(("le", f64::INFINITY)),
                        histogram.sample_count as f64,
                        metric.timestamp_ms,
                    );
                }
                write_sample(
                    &mut out,
                    &family.name,
                    "_sum",
                    &metric.labels,
                    None,
                    histogram.sample_sum,
                    metric.timestamp_ms,
                );
                write_sample(
                    &mut out,
                    &family.name,
                    "_count",
                    &metric.labels,
                    None,
                    histogram.sample_count as f64,
                    metric.timestamp_ms,
                );
            }
        }
    }
    Ok(out)
}

/// `expfmt`'s `parseFloat`: `p`, `P` and `_` are rejected before
/// `strconv.ParseFloat`. The error carries no payload because every caller
/// substitutes the Go text that embeds the offending token.
fn parse_float(token: &str) -> Result<f64, ()> {
    if token.contains(['p', 'P', '_']) {
        return Err(());
    }
    // Rust's parser accepts the same decimal/Inf/NaN spellings as
    // `strconv.ParseFloat` (case-insensitive, optional sign).
    match token.to_ascii_lowercase().as_str() {
        "nan" | "+nan" | "-nan" => Ok(f64::NAN),
        "inf" | "+inf" | "infinity" | "+infinity" => Ok(f64::INFINITY),
        "-inf" | "-infinity" => Ok(f64::NEG_INFINITY),
        _ => token.parse::<f64>().map_err(|_| ()),
    }
}

/// `strconv.FormatFloat(value, 'g', -1, 64)`, plus `writeFloat`'s hardcoded
/// `1`/`0`/`-1` and its `NaN`/`+Inf`/`-Inf` spellings.
pub fn format_go_float(value: f64) -> String {
    if value == 1.0 {
        return "1".to_owned();
    }
    if value == 0.0 {
        return "0".to_owned();
    }
    if value == -1.0 {
        return "-1".to_owned();
    }
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "+Inf".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_owned();
    }

    let sign = if value.is_sign_negative() { "-" } else { "" };
    // `LowerExp` prints the same shortest round-trip mantissa `Display` does,
    // which is the digit string `strconv` uses for prec=-1.
    let rendered = format!("{:e}", value.abs());
    let (mantissa, exponent) = rendered.split_once('e').expect("LowerExp includes e");
    let exponent: i32 = exponent.parse().expect("LowerExp exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    // `digs.dp` = decimal point position relative to the digit string.
    let decimal_point = exponent + 1;
    let exp10 = decimal_point - 1;

    // Shortest 'g' formatting uses the exponent form when exp < -4 or exp > 5.
    if !(-4..6).contains(&exp10) {
        let mut out = String::from(sign);
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp10 < 0 { '-' } else { '+' });
        let magnitude = exp10.unsigned_abs();
        if magnitude < 10 {
            out.push('0');
        }
        out.push_str(&magnitude.to_string());
        return out;
    }

    let mut out = String::from(sign);
    if decimal_point <= 0 {
        out.push_str("0.");
        for _ in 0..-decimal_point {
            out.push('0');
        }
        out.push_str(digits);
    } else if decimal_point as usize >= digits.len() {
        out.push_str(digits);
        for _ in 0..(decimal_point as usize - digits.len()) {
            out.push('0');
        }
    } else {
        let split = decimal_point as usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

/// Parser-internal family: the type stays `None` until a `# TYPE` line or the
/// first sample (which defaults it to untyped).
struct PFamilly {
    name: String,
    help: Option<String>,
    metric_type: Option<MetricType>,
    metrics: Vec<Metric>,
}

/// Which `_sum`/`_count` the current line is, when `setOrCreateCurrentMF`
/// resolved it against a summary/histogram family.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SuffixKind {
    None,
    SummaryCount,
    SummarySum,
    HistogramCount,
    HistogramSum,
}

struct Parser {
    families: Vec<PFamilly>,
    /// `metricFamiliesByName`: name → family index.
    index: HashMap<String, usize>,
    /// `summaries`: signature → (family index, metric index).
    summaries: HashMap<String, (usize, usize)>,
    /// `histograms`: signature → (family index, metric index).
    histograms: HashMap<String, (usize, usize)>,
    line: usize,
}

impl Parser {
    fn new() -> Self {
        Self {
            families: Vec::new(),
            index: HashMap::new(),
            summaries: HashMap::new(),
            histograms: HashMap::new(),
            line: 0,
        }
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError {
            line: self.line,
            message: message.into(),
        }
    }

    /// `setOrCreateCurrentMF`.
    fn set_or_create_current_mf(&mut self, name: &str) -> (usize, SuffixKind) {
        if let Some(&idx) = self.index.get(name) {
            return (idx, SuffixKind::None);
        }

        let summary_name = summary_metric_name(name);
        if let Some(&idx) = self.index.get(summary_name) {
            if self.families[idx].metric_type == Some(MetricType::Summary) {
                return (idx, summary_suffix_kind(name));
            }
        }

        let histogram_name = histogram_metric_name(name);
        if let Some(&idx) = self.index.get(histogram_name) {
            if self.families[idx].metric_type == Some(MetricType::Histogram) {
                return (idx, histogram_suffix_kind(name));
            }
        }

        let idx = self.families.len();
        self.families.push(PFamilly {
            name: name.to_owned(),
            help: None,
            metric_type: None,
            metrics: Vec::new(),
        });
        self.index.insert(name.to_owned(), idx);
        (idx, SuffixKind::None)
    }

    /// `startComment` + `readingHelp` / `readingType`. `comment` starts right
    /// after the `#` and excludes the newline.
    fn parse_comment(&mut self, comment: &str) -> Result<(), ParseError> {
        let mut cur = Cursor::new(comment);
        cur.skip_blank_tab();
        if cur.at_end() {
            return Ok(());
        }

        let keyword_start = cur.pos;
        let keyword_end = cur.take_while(is_valid_any_token_byte);
        let keyword = &comment[keyword_start..keyword_end];
        if keyword != "HELP" && keyword != "TYPE" {
            return Ok(()); // Generic comment.
        }

        cur.skip_blank_tab();
        if cur.at_end() {
            return Ok(());
        }
        let name_start = cur.pos;
        if !is_valid_metric_name_start(comment.as_bytes()[name_start]) {
            return Err(self.error("invalid metric name in comment"));
        }
        let name_end = cur.take_while(is_valid_metric_name_continuation);
        let name = &comment[name_start..name_end];
        if cur.at_end() {
            // `# TYPE foo` at end of line: no family, no error.
            return Ok(());
        }
        if !is_blank_or_tab(comment.as_bytes()[cur.pos]) {
            return Err(self.error("invalid metric name in comment"));
        }

        let (idx, _) = self.set_or_create_current_mf(name);
        cur.skip_blank_tab();
        if cur.at_end() {
            // `# HELP foo` / `# TYPE foo` without text: nothing is set.
            return Ok(());
        }
        let rest = &comment[cur.pos..];

        match keyword {
            "HELP" => {
                if self.families[idx].help.is_some() {
                    return Err(self.error(format!(
                        "second HELP line for metric name {}",
                        go_quote(name)
                    )));
                }
                let help = unescape_help(rest, self)?;
                self.families[idx].help = Some(help);
            }
            "TYPE" => {
                if self.families[idx].metric_type.is_some() {
                    return Err(self.error(format!(
                        "second TYPE line for metric name {}, or TYPE reported after samples",
                        go_quote(name)
                    )));
                }
                let metric_type = MetricType::from_token(rest)
                    .ok_or_else(|| self.error(format!("unknown metric type {}", go_quote(rest))))?;
                self.families[idx].metric_type = Some(metric_type);
            }
            _ => unreachable!("keyword checked above"),
        }
        Ok(())
    }

    /// `readingMetricName` through `startTimestamp`.
    fn parse_sample(&mut self, line: &str) -> Result<(), ParseError> {
        let bytes = line.as_bytes();
        let mut cur = Cursor::new(line);

        if !is_valid_metric_name_start(bytes[0]) {
            return Err(self.error("invalid metric name"));
        }
        let name_end = cur.take_while(is_valid_metric_name_continuation);
        let name = &line[..name_end];

        let (family_idx, suffix) = self.set_or_create_current_mf(name);
        let family = &mut self.families[family_idx];
        if family.metric_type.is_none() {
            family.metric_type = Some(MetricType::Untyped);
        }
        let metric_type = family.metric_type.expect("type set above");
        let family_name = family.name.clone();

        let mut metric = Metric::default();
        // `currentLabels`: all labels including `__name__` but excluding
        // `quantile` / `le`, used to merge summary/histogram samples.
        let mut signature_labels: Vec<(String, String)> = Vec::new();
        if matches!(metric_type, MetricType::Summary | MetricType::Histogram) {
            signature_labels.push(("__name__".to_owned(), family_name.clone()));
        }
        let mut quantile: Option<f64> = None;
        let mut bucket: Option<f64> = None;

        cur.skip_blank_tab();
        if cur.peek() == Some(b'{') {
            cur.pos += 1;
            loop {
                cur.skip_blank_tab();
                let Some(byte) = cur.peek() else {
                    return Err(self.error(format!(
                        "invalid label name for metric {}",
                        go_quote(&family_name)
                    )));
                };
                if byte == b'}' {
                    cur.pos += 1;
                    cur.skip_blank_tab();
                    break;
                }
                if !is_valid_label_name_start(byte) {
                    return Err(self.error(format!(
                        "invalid label name for metric {}",
                        go_quote(&family_name)
                    )));
                }
                let label_start = cur.pos;
                let label_end = cur.take_while(is_valid_label_name_continuation);
                let label_name = &line[label_start..label_end];
                if label_name == "__name__" {
                    return Err(
                        self.error(format!("label name {} is reserved", go_quote(label_name)))
                    );
                }

                let is_quantile = metric_type == MetricType::Summary && label_name == "quantile";
                let is_le = metric_type == MetricType::Histogram && label_name == "le";
                let pushed = !is_quantile && !is_le;
                if pushed {
                    metric.labels.push(LabelPair {
                        name: label_name.to_owned(),
                        value: String::new(),
                    });
                }

                cur.skip_blank_tab();
                match cur.peek() {
                    Some(b'=') => cur.pos += 1,
                    other => {
                        return Err(self.error(format!(
                            "expected '=' after label name, found {}",
                            go_quote_byte(other.unwrap_or(b'\n'))
                        )))
                    }
                }

                // Duplicate detection over the label set collected so far
                // (the baseline appends the current pair first, then scans).
                if pushed {
                    let mut seen = std::collections::HashSet::new();
                    for pair in &metric.labels {
                        if !seen.insert(pair.name.as_str()) {
                            return Err(self.error(format!(
                                "duplicate label names for metric {}",
                                go_quote(&family_name)
                            )));
                        }
                    }
                }

                cur.skip_blank_tab();
                match cur.peek() {
                    Some(b'"') => cur.pos += 1,
                    other => {
                        return Err(self.error(format!(
                            "expected '\"' at start of label value, found {}",
                            go_quote_byte(other.unwrap_or(b'\n'))
                        )))
                    }
                }
                let (value, next) = self.read_label_value(line, cur.pos)?;
                cur.pos = next;

                if is_quantile {
                    quantile = Some(parse_float(&value).map_err(|_| {
                        self.error(format!(
                            "expected float as value for 'quantile' label, got {}",
                            go_quote(&value)
                        ))
                    })?);
                } else if is_le {
                    bucket = Some(parse_float(&value).map_err(|_| {
                        self.error(format!(
                            "expected float as value for 'le' label, got {}",
                            go_quote(&value)
                        ))
                    })?);
                } else {
                    if matches!(metric_type, MetricType::Summary | MetricType::Histogram) {
                        signature_labels.push((label_name.to_owned(), value.clone()));
                    }
                    metric.labels.last_mut().expect("pushed above").value = value;
                }

                cur.skip_blank_tab();
                match cur.peek() {
                    Some(b',') => cur.pos += 1,
                    Some(b'}') => {
                        cur.pos += 1;
                        cur.skip_blank_tab();
                        break;
                    }
                    _ => {
                        let value = metric
                            .labels
                            .last()
                            .map(|pair| pair.value.clone())
                            .unwrap_or_default();
                        return Err(self.error(format!(
                            "unexpected end of label value {}",
                            go_quote(&value)
                        )));
                    }
                }
            }
        }

        // `readingValue`: resolve the (possibly merged) metric first.
        let metric_index = match metric_type {
            MetricType::Summary | MetricType::Histogram => {
                let signature = labels_to_signature(&signature_labels);
                let table = if metric_type == MetricType::Summary {
                    &mut self.summaries
                } else {
                    &mut self.histograms
                };
                if let Some(&(_, idx)) = table.get(&signature) {
                    idx
                } else {
                    let idx = self.families[family_idx].metrics.len();
                    self.families[family_idx].metrics.push(metric);
                    table.insert(signature, (family_idx, idx));
                    idx
                }
            }
            _ => {
                let idx = self.families[family_idx].metrics.len();
                self.families[family_idx].metrics.push(metric);
                idx
            }
        };

        let value_start = cur.pos;
        let value_end = cur.take_while(is_not_blank_or_tab);
        let value_token = &line[value_start..value_end];
        let value = parse_float(value_token).map_err(|_| {
            self.error(format!(
                "expected float as value, got {}",
                go_quote(value_token)
            ))
        })?;

        {
            let target = &mut self.families[family_idx].metrics[metric_index];
            match metric_type {
                MetricType::Counter => target.counter = Some(value),
                MetricType::Gauge => target.gauge = Some(value),
                MetricType::Untyped => target.untyped = Some(value),
                MetricType::Summary => {
                    let summary = target.summary.get_or_insert_with(Summary::default);
                    match suffix {
                        SuffixKind::SummaryCount => summary.sample_count = value as u64,
                        SuffixKind::SummarySum => summary.sample_sum = value,
                        _ => {
                            if let Some(quantile) = quantile {
                                summary.quantiles.push(Quantile { quantile, value });
                            }
                        }
                    }
                }
                MetricType::Histogram => {
                    let histogram = target.histogram.get_or_insert_with(Histogram::default);
                    match suffix {
                        SuffixKind::HistogramCount => histogram.sample_count = value as u64,
                        SuffixKind::HistogramSum => histogram.sample_sum = value,
                        _ => {
                            if let Some(upper_bound) = bucket {
                                histogram.buckets.push(Bucket {
                                    upper_bound,
                                    cumulative_count: value as u64,
                                });
                            }
                        }
                    }
                }
            }
        }

        cur.skip_blank_tab();
        if cur.at_end() {
            return Ok(());
        }

        // `startTimestamp`.
        let ts_start = cur.pos;
        let ts_end = cur.take_while(is_not_blank_or_tab);
        let ts_token = &line[ts_start..ts_end];
        let timestamp = ts_token.parse::<i64>().map_err(|_| {
            self.error(format!(
                "expected integer as timestamp, got {}",
                go_quote(ts_token)
            ))
        })?;
        cur.skip_blank_tab();
        if !cur.at_end() {
            return Err(self.error(format!(
                "spurious string after timestamp: {}",
                go_quote(&line[cur.pos..])
            )));
        }
        self.families[family_idx].metrics[metric_index].timestamp_ms = Some(timestamp);
        Ok(())
    }

    /// `readTokenAsLabelValue`: the value (unescaped) and the position after
    /// the closing quote. Running off the end of the line is the baseline's
    /// unescaped-newline error.
    fn read_label_value(&self, line: &str, start: usize) -> Result<(String, usize), ParseError> {
        let mut value = String::new();
        let mut pos = start;
        let mut escaped = false;
        while pos < line.len() {
            let ch = line[pos..].chars().next().expect("valid char");
            pos += ch.len_utf8();
            if escaped {
                match ch {
                    '"' => value.push('"'),
                    '\\' => value.push('\\'),
                    'n' => value.push('\n'),
                    other => {
                        return Err(self.error(format!("invalid escape sequence '\\{other}'")));
                    }
                }
                escaped = false;
                continue;
            }
            match ch {
                '"' => return Ok((value, pos)),
                '\\' => escaped = true,
                other => value.push(other),
            }
        }
        if escaped {
            // The newline is read as the escaped character.
            return Err(self.error("invalid escape sequence '\\\n'"));
        }
        Err(self.error(format!(
            "label value {} contains unescaped new-line",
            go_quote(&value)
        )))
    }
}

/// `summaryMetricName`.
fn summary_metric_name(name: &str) -> &str {
    if is_count(name) {
        &name[..name.len() - 6]
    } else if is_sum(name) {
        &name[..name.len() - 4]
    } else {
        name
    }
}

/// `histogramMetricName`.
fn histogram_metric_name(name: &str) -> &str {
    if is_count(name) {
        &name[..name.len() - 6]
    } else if is_sum(name) {
        &name[..name.len() - 4]
    } else if is_bucket(name) {
        &name[..name.len() - 7]
    } else {
        name
    }
}

fn summary_suffix_kind(name: &str) -> SuffixKind {
    if is_count(name) {
        SuffixKind::SummaryCount
    } else if is_sum(name) {
        SuffixKind::SummarySum
    } else {
        SuffixKind::None
    }
}

fn histogram_suffix_kind(name: &str) -> SuffixKind {
    if is_count(name) {
        SuffixKind::HistogramCount
    } else if is_sum(name) {
        SuffixKind::HistogramSum
    } else {
        SuffixKind::None
    }
}

fn is_count(name: &str) -> bool {
    name.len() > 6 && name.ends_with("_count")
}

fn is_sum(name: &str) -> bool {
    name.len() > 4 && name.ends_with("_sum")
}

fn is_bucket(name: &str) -> bool {
    name.len() > 7 && name.ends_with("_bucket")
}

/// `model.LabelsToSignature`: sorted name/value pairs, each terminated by
/// `0xff`.
fn labels_to_signature(labels: &[(String, String)]) -> String {
    let mut sorted = labels.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut signature = String::new();
    for (name, value) in sorted {
        signature.push_str(&name);
        signature.push('\u{ff}');
        signature.push_str(&value);
        signature.push('\u{ff}');
    }
    signature
}

/// `readTokenUntilNewline(true)`: `\\` and `\n` are recognised; any other
/// escaped character is an error.
fn unescape_help(raw: &str, parser: &Parser) -> Result<String, ParseError> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some(other) => return Err(parser.error(format!("invalid escape sequence '\\{other}'"))),
            None => return Err(parser.error("invalid escape sequence '\\'")),
        }
    }
    Ok(out)
}

/// A byte cursor over one line (the newline itself is not part of it).
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(line: &'a str) -> Self {
        Self {
            bytes: line.as_bytes(),
            pos: 0,
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_blank_tab(&mut self) {
        while !self.at_end() && is_blank_or_tab(self.bytes[self.pos]) {
            self.pos += 1;
        }
    }

    /// Advance past bytes matching `predicate`, returning the new position.
    fn take_while(&mut self, predicate: impl Fn(u8) -> bool) -> usize {
        while !self.at_end() && predicate(self.bytes[self.pos]) {
            self.pos += 1;
        }
        self.pos
    }
}

fn is_blank_or_tab(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

fn is_not_blank_or_tab(byte: u8) -> bool {
    !is_blank_or_tab(byte)
}

fn is_valid_any_token_byte(byte: u8) -> bool {
    !is_blank_or_tab(byte)
}

fn is_valid_label_name_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_valid_label_name_continuation(byte: u8) -> bool {
    is_valid_label_name_start(byte) || byte.is_ascii_digit()
}

fn is_valid_metric_name_start(byte: u8) -> bool {
    is_valid_label_name_start(byte) || byte == b':'
}

fn is_valid_metric_name_continuation(byte: u8) -> bool {
    is_valid_label_name_continuation(byte) || byte == b':'
}

/// `model.IsValidLegacyMetricName`; the encoder uses it for metric *and* label
/// names (the baseline's `writeName` calls it for both).
fn is_valid_legacy_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    match bytes.first() {
        Some(&first) if is_valid_metric_name_start(first) => {}
        _ => return false,
    }
    bytes[1..]
        .iter()
        .all(|byte| is_valid_metric_name_continuation(*byte))
}

/// `writeEscapedString`.
fn escape_string(value: &str, include_double_quote: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' if include_double_quote => out.push_str("\\\""),
            other => out.push(other),
        }
    }
    out
}

/// `writeName`: as-is when legacy-valid, quoted otherwise.
fn write_name(out: &mut String, name: &str) {
    if is_valid_legacy_name(name) {
        out.push_str(name);
    } else {
        out.push('"');
        out.push_str(&escape_string(name, true));
        out.push('"');
    }
}

/// `writeNameAndLabelPairs` + value + optional timestamp.
fn write_sample(
    out: &mut String,
    name: &str,
    suffix: &str,
    labels: &[LabelPair],
    additional_label: Option<(&str, f64)>,
    value: f64,
    timestamp_ms: Option<i64>,
) {
    let full_name = format!("{name}{suffix}");
    let mut separator = '{';
    let mut metric_inside_braces = false;
    if !is_valid_legacy_name(&full_name) {
        metric_inside_braces = true;
        out.push(separator);
        separator = ',';
    }
    write_name(out, &full_name);

    if labels.is_empty() && additional_label.is_none() {
        if metric_inside_braces {
            out.push('}');
        }
    } else {
        for pair in labels {
            out.push(separator);
            separator = ',';
            write_name(out, &pair.name);
            out.push_str("=\"");
            out.push_str(&escape_string(&pair.value, true));
            out.push('"');
        }
        if let Some((extra_name, extra_value)) = additional_label {
            out.push(separator);
            out.push_str(extra_name);
            out.push_str("=\"");
            out.push_str(&format_go_float(extra_value));
            out.push('"');
        }
        out.push('}');
    }

    out.push(' ');
    out.push_str(&format_go_float(value));
    if let Some(timestamp) = timestamp_ms {
        out.push(' ');
        out.push_str(&timestamp.to_string());
    }
    out.push('\n');
}

/// `strconv.Quote` for the error messages that embed a token.
fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{b}' => out.push_str("\\v"),
            '\u{c}' => out.push_str("\\f"),
            ch if (ch as u32) < 0x20 || ch == '\u{7f}' => {
                out.push_str(&format!("\\x{:02x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// `strconv.QuoteRune` for `fmt %q` of the single byte the baseline reports.
fn go_quote_byte(byte: u8) -> String {
    match byte {
        b'\'' => "'\\''".to_owned(),
        b'\\' => "'\\\\'".to_owned(),
        b'\n' => "'\\n'".to_owned(),
        b'\r' => "'\\r'".to_owned(),
        b'\t' => "'\\t'".to_owned(),
        0x20..=0x7e => format!("'{}'", byte as char),
        _ => format!("'\\x{byte:02x}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_float_formatting_matches_strconv() {
        // Reference values produced by
        // `strconv.FormatFloat(v, 'g', -1, 64)` (see the Go harness in
        // test_ws/p9-go for the generator).
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (1.0, "1"),
            (-1.0, "-1"),
            (0.5, "0.5"),
            (1.5, "1.5"),
            (2.0, "2"),
            (-5.0, "-5"),
            (100.0, "100"),
            (150.0, "150"),
            (999.0, "999"),
            (28800.0, "28800"),
            (3600.0, "3600"),
            (6000.0, "6000"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (0.000001234, "1.234e-06"),
            (1234567.0, "1.234567e+06"),
            (123456.0, "123456"),
            (999999.0, "999999"),
            (1000000.0, "1e+06"),
            (1e21, "1e+21"),
            (1.2345678901234568e20, "1.2345678901234568e+20"),
            (std::f64::consts::PI, "3.141592653589793"),
            (1e-10, "1e-10"),
            (2.5e-7, "2.5e-07"),
            (1e300, "1e+300"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (0.3333333333333333, "0.3333333333333333"),
            (9.007199254740992e15, "9.007199254740992e+15"),
            (1.234e6, "1.234e+06"),
            (123400.0, "123400"),
            (0.000123456789, "0.000123456789"),
            (-0.5, "-0.5"),
            (-1234567.0, "-1.234567e+06"),
        ];
        for (value, expected) in cases {
            assert_eq!(&format_go_float(*value), expected, "value={value}");
        }
        assert_eq!(format_go_float(f64::NAN), "NaN");
        assert_eq!(format_go_float(f64::INFINITY), "+Inf");
        assert_eq!(format_go_float(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn floats_parse_like_go() {
        assert_eq!(parse_float("1"), Ok(1.0));
        assert_eq!(parse_float("+Inf"), Ok(f64::INFINITY));
        assert_eq!(parse_float("-Inf"), Ok(f64::NEG_INFINITY));
        assert!(parse_float("NaN").expect("nan").is_nan());
        assert_eq!(parse_float("1e+06"), Ok(1e6));
        assert!(parse_float("1_0").is_err());
        assert!(parse_float("0x10").is_err());
        assert!(parse_float("nope").is_err());
        assert!(parse_float("1p3").is_err());
    }

    #[test]
    fn decodes_and_reencodes_canonical_text_unchanged() {
        let input = "\
# HELP http_requests_total The total number of HTTP requests.\n\
# TYPE http_requests_total counter\n\
http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n\
http_requests_total{method=\"post\",code=\"400\"} 3 1395066363000\n\
# HELP rpc_duration_seconds A summary of the RPC duration in seconds.\n\
# TYPE rpc_duration_seconds summary\n\
rpc_duration_seconds{quantile=\"0.01\"} 5.4e-05 1395066363000\n\
rpc_duration_seconds{quantile=\"0.5\"} 0.001 1395066363000\n\
rpc_duration_seconds{quantile=\"0.99\"} 0.0015 1395066363000\n\
rpc_duration_seconds_sum 175.60473 1395066363000\n\
rpc_duration_seconds_count 2693 1395066363000\n\
# HELP http_request_duration_seconds A histogram.\n\
# TYPE http_request_duration_seconds histogram\n\
http_request_duration_seconds_bucket{le=\"0.05\"} 24054\n\
http_request_duration_seconds_bucket{le=\"0.1\"} 33444\n\
http_request_duration_seconds_bucket{le=\"+Inf\"} 144320\n\
http_request_duration_seconds_sum 53423\n\
http_request_duration_seconds_count 144320\n\
";
        let families = parse_text(input).expect("parse");
        assert_eq!(families.len(), 3);

        assert_eq!(families[0].metric_type, MetricType::Counter);
        assert_eq!(families[0].metrics.len(), 2);
        assert_eq!(families[0].metrics[0].labels.len(), 2);
        assert_eq!(families[0].metrics[0].timestamp_ms, Some(1395066363000));

        assert_eq!(families[1].metric_type, MetricType::Summary);
        assert_eq!(families[1].metrics.len(), 1);
        let summary = families[1].metrics[0].summary.as_ref().expect("summary");
        assert_eq!(summary.quantiles.len(), 3);
        assert_eq!(summary.sample_sum, 175.60473);
        assert_eq!(summary.sample_count, 2693);
        assert!(families[1].metrics[0]
            .labels
            .iter()
            .all(|pair| pair.name != "quantile"));

        assert_eq!(families[2].metric_type, MetricType::Histogram);
        assert_eq!(families[2].metrics.len(), 1);
        let histogram = families[2].metrics[0]
            .histogram
            .as_ref()
            .expect("histogram");
        assert_eq!(histogram.buckets.len(), 3);
        assert_eq!(histogram.sample_count, 144320);

        let mut encoded = String::new();
        for family in &families {
            encoded.push_str(&encode_family(family).expect("encode"));
        }
        assert_eq!(encoded, input, "re-encoding canonical text is the identity");
    }

    #[test]
    fn histogram_without_inf_bucket_gains_one() {
        let input = "\
# TYPE h histogram\n\
h_bucket{le=\"1\"} 2\n\
h_sum 3\n\
h_count 4\n\
";
        let families = parse_text(input).expect("parse");
        let encoded = encode_family(&families[0]).expect("encode");
        assert_eq!(
            encoded,
            "# TYPE h histogram\n\
h_bucket{le=\"1\"} 2\n\
h_bucket{le=\"+Inf\"} 4\n\
h_sum 3\n\
h_count 4\n"
        );
    }

    #[test]
    fn summary_metrics_are_merged_by_label_set() {
        let input = "\
# TYPE s summary\n\
s{a=\"1\",quantile=\"0.5\"} 2\n\
s_count{a=\"1\"} 4\n\
s{a=\"2\",quantile=\"0.5\"} 3\n\
s_count{a=\"2\"} 4\n\
";
        let families = parse_text(input).expect("parse");
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].metrics.len(), 2);
        assert_eq!(
            families[0].metrics[0].labels,
            vec![LabelPair {
                name: "a".to_owned(),
                value: "1".to_owned(),
            }]
        );
        let summary = families[0].metrics[0].summary.as_ref().expect("summary");
        assert_eq!(summary.quantiles.len(), 1);
        assert_eq!(summary.sample_count, 4);
    }

    #[test]
    fn help_and_label_escapes_round_trip() {
        let input = "\
# HELP weird A \\\\ backslash and a \\n newline and a \"quote\"\n\
# TYPE weird gauge\n\
weird{path=\"a\\\\b\",quote=\"a\\\"b\",line=\"a\\nb\"} 1\n\
";
        let families = parse_text(input).expect("parse");
        assert_eq!(
            families[0].help.as_deref(),
            Some("A \\ backslash and a \n newline and a \"quote\"")
        );
        assert_eq!(families[0].metrics[0].labels[2].value, "a\nb".to_owned());
        let encoded = encode_family(&families[0]).expect("encode");
        assert_eq!(encoded, input);
    }

    #[test]
    fn untyped_families_default_and_emit_a_type_line() {
        let families = parse_text("some_metric 1\n").expect("parse");
        assert_eq!(families[0].metric_type, MetricType::Untyped);
        assert_eq!(
            encode_family(&families[0]).expect("encode"),
            "# TYPE some_metric untyped\nsome_metric 1\n"
        );
    }

    #[test]
    fn malformed_inputs_report_go_errors() {
        let err = parse_text("# TYPE x nope\nx 1\n").expect_err("unknown type");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: unknown metric type \"nope\""
        );

        let err = parse_text("x not-a-float\n").expect_err("bad float");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: expected float as value, got \"not-a-float\""
        );

        let err = parse_text("x 1").expect_err("missing newline");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: unexpected end of input stream"
        );

        let err = parse_text("# TYPE x counter\n\n# TYPE x gauge\nx 1\n").expect_err("second type");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 3: second TYPE line for metric name \"x\", or TYPE reported after samples"
        );

        let err = parse_text("x{a=\"1\n").expect_err("unterminated label value");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: label value \"1\" contains unescaped new-line"
        );

        let err = parse_text("x{a=\"1\"\n").expect_err("missing brace");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: unexpected end of label value \"1\""
        );

        let err = parse_text("x{a=1}\n").expect_err("missing quote");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: expected '\"' at start of label value, found '1'"
        );

        let err = parse_text("x{a=\"1\",a=\"2\"} 1\n").expect_err("duplicate label");
        assert_eq!(
            err.to_string(),
            "text format parsing error in line 1: duplicate label names for metric \"x\""
        );
    }

    #[test]
    fn empty_input_and_comment_only_input_produce_no_families() {
        assert!(parse_text("").expect("empty").is_empty());
        assert!(parse_text("\n\n").expect("blank").is_empty());
        assert!(parse_text("# just a comment\n")
            .expect("comment")
            .is_empty());
        assert!(parse_text("# TYPE x gauge\n")
            .expect("no samples")
            .is_empty());
    }

    #[test]
    fn invalid_legacy_names_are_quoted_inside_the_braces() {
        // `writeNameAndLabelPairs` puts a non-legacy name inside `{}`.
        let mut out = String::new();
        write_sample(&mut out, "a.b", "", &[], None, 1.0, None);
        assert_eq!(out, "{\"a.b\"} 1\n");

        let mut out = String::new();
        write_sample(
            &mut out,
            "a.b",
            "",
            &[LabelPair {
                name: "c.d".to_owned(),
                value: "v".to_owned(),
            }],
            None,
            2.0,
            None,
        );
        assert_eq!(out, "{\"a.b\",\"c.d\"=\"v\"} 2\n");
    }
}
