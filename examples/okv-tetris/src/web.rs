use crate::api::API_VERSION;
use crate::game::{Action, GameState, HEIGHT, WIDTH};
use crate::store::{BranchStats, KernelStats, PrototypeKernel};
use okv_model::Version;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

const INDEX_HTML: &str = include_str!("../web/index.html");

pub fn serve(mut kernel: PrototypeKernel, port: u16) -> Result<(), String> {
    let address = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&address).map_err(|error| error.to_string())?;
    println!("objectKV Tetris web app: http://{address}/");
    println!("backend: okv-model + okv-log, single process, volatile memory");
    for connection in listener.incoming() {
        match connection {
            Ok(mut stream) => {
                if let Err(error) = handle(&mut stream, &mut kernel) {
                    let _ = respond(&mut stream, 500, "text/plain", error.as_bytes());
                }
            }
            Err(error) => eprintln!("connection failed: {error}"),
        }
    }
    Ok(())
}

fn handle(stream: &mut TcpStream, kernel: &mut PrototypeKernel) -> Result<(), String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    let request = read_request(stream)?;
    let route = request.target.split('?').next().unwrap_or(&request.target);
    match (request.method.as_str(), route) {
        ("GET", "/") => respond(stream, 200, "text/html; charset=utf-8", INDEX_HTML.as_bytes()),
        ("GET", "/healthz") => respond(stream, 200, "text/plain", b"ok\n"),
        ("GET", "/api/spec") => respond(
            stream,
            200,
            "application/json",
            format!(
                "{{\"api_version\":\"{API_VERSION}\",\"backend\":[\"okv-model\",\"okv-log\"],\"topology\":\"single-process\",\"durability\":\"volatile\",\"operations\":[\"point-read\",\"range-read\",\"transact\",\"recover\",\"fork-at-version\",\"switch-branch\"]}}"
            )
            .as_bytes(),
        ),
        ("GET", "/api/state") => {
            let requested = query_value(&request.target, "version")
                .and_then(|value| value.parse::<u64>().ok())
                .map(Version::new);
            let body = snapshot_json(kernel, requested)?;
            respond(stream, 200, "application/json", body.as_bytes())
        }
        ("POST", "/api/action") => {
            let action = parse_action(String::from_utf8_lossy(&request.body).trim())?;
            kernel.apply_action(action)?;
            let body = snapshot_json(kernel, None)?;
            respond(stream, 200, "application/json", body.as_bytes())
        }
        ("POST", "/api/recover") => {
            kernel.recover_from_txlog()?;
            let body = snapshot_json(kernel, None)?;
            respond(stream, 200, "application/json", body.as_bytes())
        }
        ("POST", "/api/branch") => {
            let body = String::from_utf8_lossy(&request.body);
            let version = if body.trim().is_empty() {
                kernel.latest_version()
            } else {
                body.trim()
                    .parse::<u64>()
                    .map(Version::new)
                    .map_err(|_| "fork body must be a version number".to_owned())?
            };
            kernel.fork_from(version)?;
            let body = snapshot_json(kernel, None)?;
            respond(stream, 200, "application/json", body.as_bytes())
        }
        ("POST", "/api/switch") => {
            let branch = String::from_utf8_lossy(&request.body);
            kernel.switch_branch(branch.trim())?;
            let body = snapshot_json(kernel, None)?;
            respond(stream, 200, "application/json", body.as_bytes())
        }
        _ => respond(stream, 404, "text/plain", b"not found\n"),
    }
}

fn snapshot_json(
    kernel: &mut PrototypeKernel,
    requested: Option<Version>,
) -> Result<String, String> {
    let latest = kernel.latest_version();
    let version = requested.unwrap_or(latest);
    if version.sequence() == 0 || version > latest {
        return Err(format!("snapshot {version} is outside 0:1..={latest}"));
    }
    let game = kernel.read_game(Some(version))?;
    let events = kernel.read_event_rows(version)?;
    let stats = kernel.stats()?;
    Ok(format!(
        "{{\"api_version\":\"{API_VERSION}\",\"maturity\":{{\"status\":\"CODE-COMPLETE\",\"topology\":\"single_process\",\"durability\":\"volatile\",\"object_publication\":false}},\"game\":{},\"storage\":{},\"events\":{}}}",
        game_json(&game),
        stats_json(&stats, version),
        events_json(&events)
    ))
}

fn game_json(game: &GameState) -> String {
    let board = game.visible_board();
    let rows = board
        .iter()
        .take(HEIGHT)
        .map(|row| {
            let cells = row
                .iter()
                .take(WIDTH)
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{cells}]")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"board\":[{rows}],\"score\":{},\"lines\":{},\"ticks\":{},\"piece\":{},\"game_over\":{}}}",
        game.score,
        game.lines,
        game.ticks,
        json_string(&game.active.kind.glyph().to_string()),
        game.game_over
    )
}

fn stats_json(stats: &KernelStats, viewed_version: Version) -> String {
    let receipt = stats.last_receipt.as_ref().map_or_else(
        || "null".to_owned(),
        |receipt| {
            format!(
                "{{\"version\":{},\"request_id\":{},\"mutations\":{},\"log_index\":{},\"replayed\":{},\"api_version\":{}}}",
                receipt.commit_version.sequence(),
                receipt.request_id,
                receipt.mutation_count,
                receipt.txlog_index,
                receipt.replayed,
                json_string(receipt.api_version)
            )
        },
    );
    let branches = stats
        .branches
        .iter()
        .map(branch_json)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"branch\":{},\"branches\":[{branches}],\"viewed_version\":{},\"latest_version\":{},\"txlog_entries\":{},\"txlog_bytes\":{},\"application_record_bytes\":{},\"point_reads\":{},\"range_reads\":{},\"recoveries\":{},\"visible_rows\":{},\"event_rows\":{},\"last_action\":{},\"receipt\":{receipt}}}",
        json_string(&stats.branch),
        viewed_version.sequence(),
        stats.latest_version.sequence(),
        stats.txlog_entries,
        stats.txlog_bytes,
        stats.application_record_bytes,
        stats.point_reads,
        stats.range_reads,
        stats.recoveries,
        stats.visible_rows,
        stats.event_rows,
        json_string(&stats.last_action)
    )
}

fn branch_json(branch: &BranchStats) -> String {
    let parent = branch
        .parent
        .as_ref()
        .map_or_else(|| "null".to_owned(), |parent| json_string(parent));
    format!(
        "{{\"name\":{},\"parent\":{parent},\"fork_version\":{},\"latest_version\":{},\"txlog_entries\":{},\"txlog_bytes\":{},\"active\":{}}}",
        json_string(&branch.name),
        branch.fork_version,
        branch.latest_version,
        branch.txlog_entries,
        branch.txlog_bytes,
        branch.active
    )
}

fn events_json(events: &[(Vec<u8>, Vec<u8>)]) -> String {
    let entries = events
        .iter()
        .rev()
        .take(10)
        .map(|(key, value)| {
            let key = String::from_utf8_lossy(key);
            let version = key.rsplit('/').next().unwrap_or("0");
            format!(
                "{{\"version\":{},\"action\":{}}}",
                version.parse::<u64>().unwrap_or_default(),
                json_string(&String::from_utf8_lossy(value))
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{entries}]")
}

fn parse_action(value: &str) -> Result<Action, String> {
    match value {
        "left" => Ok(Action::Left),
        "right" => Ok(Action::Right),
        "rotate" => Ok(Action::Rotate),
        "tick" => Ok(Action::Tick),
        "drop" => Ok(Action::Drop),
        "reset" => Ok(Action::Reset),
        _ => Err(format!("unknown action {value:?}")),
    }
}

fn query_value<'a>(target: &'a str, key: &str) -> Option<&'a str> {
    target.split_once('?')?.1.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        (candidate == key).then_some(value)
    })
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other if other.is_control() => escaped.push('?'),
            other => escaped.push(other),
        }
    }
    escaped.push('"');
    escaped
}

struct HttpRequest {
    method: String,
    target: String,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let count = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("connection closed before request headers".to_owned());
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > 64 * 1024 {
            return Err("request exceeds prototype limit".to_owned());
        }
        if let Some(position) = find_bytes(&bytes, b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec())
        .map_err(|_| "request headers are not UTF-8".to_owned())?;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or_default();
    while bytes.len() < header_end + content_length {
        let count = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("connection closed before request body".to_owned());
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let request_line = headers
        .lines()
        .next()
        .ok_or_else(|| "missing request line".to_owned())?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| "missing request method".to_owned())?
        .to_owned();
    let target = parts
        .next()
        .ok_or_else(|| "missing request target".to_owned())?
        .to_owned();
    Ok(HttpRequest {
        method,
        target,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn respond(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), String> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\nX-Content-Type-Options: nosniff\r\n\r\n",
        body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .and_then(|()| stream.write_all(body))
        .and_then(|()| stream.flush())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::INDEX_HTML;

    #[test]
    fn high_frequency_actions_do_not_dim_the_entire_app() {
        assert!(!INDEX_HTML.contains(".loading { opacity"));
        assert!(!INDEX_HTML.contains("classList.toggle(\"loading\""));
    }

    #[test]
    fn lineage_has_exact_version_scrubbers() {
        assert!(INDEX_HTML.contains("id=\"lineage\""));
        assert!(INDEX_HTML.contains("navigateHistory"));
        assert!(INDEX_HTML.contains("scheduleHistory"));
        assert!(INDEX_HTML.contains("flushScheduledHistory"));
        assert!(INDEX_HTML.contains("range.type = \"range\""));
        assert!(INDEX_HTML.contains("range.dataset.lineageScrubber"));
        assert!(INDEX_HTML.contains("branch.fork_version"));
        assert!(!INDEX_HTML.contains("window.setTimeout(open, 45)"));
    }

    #[test]
    fn workspace_is_full_width_and_lineage_is_a_preserved_side_tab() {
        assert!(INDEX_HTML.contains(".shell { width: 100%;"));
        assert!(INDEX_HTML.contains("class=\"panel workbench\""));
        assert!(INDEX_HTML.contains("role=\"tablist\""));
        assert!(INDEX_HTML.contains("data-panel-tab=\"history\""));
        assert!(INDEX_HTML.contains("id=\"history-panel\""));
        assert!(INDEX_HTML.contains("selectPanel"));
        assert!(INDEX_HTML.contains("function render({ lineage = true } = {})"));
        assert!(INDEX_HTML.contains("render({ lineage: !preserveLineage })"));
    }
}
