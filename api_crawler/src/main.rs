use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
pub struct LithiumResponse {
    pub status: String,
    pub data: Option<LithiumData>,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LithiumData {
    #[serde(default)]
    pub items: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default, rename = "type")]
    pub msg_type: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

const BASE_URL: &str = "https://community.intel.com/api/2.0/search";
const ITEMS_PER_PAGE: usize = 100;
const MAX_CONCURRENT_WORKERS: usize = 10;
const ITEMS_PER_CHUNK_FILE: usize = 10_000;
const REQ_PER_SEC: u64 = 20;
const MAX_RETRIES: u32 = 10;

// Centralized app state for the TUI Dashboard
struct AppState {
    total_messages: usize,
    total_requests: usize,
    rate_limited_count: usize,
    error_count: usize,
    worker_status: Vec<String>,
    logs: VecDeque<String>,
    start_time: Instant,
    done: bool,
}

impl AppState {
    fn new(num_workers: usize) -> Self {
        Self {
            total_messages: 0,
            total_requests: 0,
            rate_limited_count: 0,
            error_count: 0,
            worker_status: vec!["Idle".to_string(); num_workers],
            logs: VecDeque::with_capacity(100),
            start_time: Instant::now(),
            done: false,
        }
    }

    fn log(&mut self, msg: String) {
        if self.logs.len() >= 100 {
            self.logs.pop_front();
        }
        self.logs.push_back(msg);
    }
}

enum WriterMessage {
    Messages(Vec<Message>),
    Done,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app_state = Arc::new(Mutex::new(AppState::new(MAX_CONCURRENT_WORKERS)));

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Spawn the crawler in the background
    let state_for_crawler = app_state.clone();
    tokio::spawn(async move {
        run_crawler(state_for_crawler).await;
    });

    // TUI Loop
    loop {
        terminal.draw(|f| ui(f, &app_state))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if let KeyCode::Char('q') = key.code {
                    // Graceful shutdown requested by user
                    app_state.lock().unwrap().log("Shutdown requested. Exiting...".into());
                    break;
                }
            }
        }

        if app_state.lock().unwrap().done {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    println!("Dashboard closed. Crawling complete or aborted.");
    Ok(())
}

fn ui(f: &mut Frame, app_state: &Arc<Mutex<AppState>>) {
    let state = app_state.lock().unwrap();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Length(4),   // Header/Stats
                Constraint::Length(MAX_CONCURRENT_WORKERS as u16 + 2),  // Workers
                Constraint::Min(10),     // Logs
            ]
            .as_ref(),
        )
        .split(f.area());

    let elapsed = state.start_time.elapsed().as_secs();
    let rps = if elapsed > 0 { state.total_requests as f64 / elapsed as f64 } else { 0.0 };
    let mps = if elapsed > 0 { state.total_messages as f64 / elapsed as f64 } else { 0.0 };

    let stats_text = vec![
        Line::from(vec![
            Span::styled(" Requests: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{} | ", state.total_requests)),
            Span::styled("Messages: ", Style::default().fg(Color::Green)),
            Span::raw(format!("{} | ", state.total_messages)),
            Span::styled("Rate Limited: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{} | ", state.rate_limited_count)),
            Span::styled("Errors: ", Style::default().fg(Color::Red)),
            Span::raw(format!("{} | ", state.error_count)),
            Span::styled("Press 'q' to quit", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled(" Elapsed: ", Style::default().fg(Color::LightBlue)),
            Span::raw(format!("{}s | ", elapsed)),
            Span::styled("Req/s: ", Style::default().fg(Color::LightMagenta)),
            Span::raw(format!("{:.2} | ", rps)),
            Span::styled("Msg/s: ", Style::default().fg(Color::LightMagenta)),
            Span::raw(format!("{:.2}", mps)),
        ]),
    ];

    let stats_block = Paragraph::new(stats_text)
        .block(Block::default().borders(Borders::ALL).title(" Enterprise Khoros API Crawler Dashboard "));
    f.render_widget(stats_block, chunks[0]);

    let workers_list: Vec<ListItem> = state.worker_status.iter().enumerate().map(|(i, status)| {
        ListItem::new(format!(" Worker {:02}: {}", i, status))
    }).collect();
    let workers_block = List::new(workers_list)
        .block(Block::default().borders(Borders::ALL).title(" Workers "));
    f.render_widget(workers_block, chunks[1]);

    // Reverse logs to show newest at the top or bottom. We'll show newest at the bottom of the list natively.
    let logs_list: Vec<ListItem> = state.logs.iter().map(|l| ListItem::new(l.clone())).collect();
    let logs_block = List::new(logs_list)
        .block(Block::default().borders(Borders::ALL).title(" Event Logs "));
    f.render_widget(logs_block, chunks[2]);
}

async fn run_crawler(app_state: Arc<Mutex<AppState>>) {
    app_state.lock().unwrap().log("Initializing HTTP client...".into());
    let client = match Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .timeout(Duration::from_secs(30))
        .build() {
            Ok(c) => c,
            Err(e) => {
                app_state.lock().unwrap().log(format!("Failed to build client: {}", e));
                return;
            }
        };

    let (ticket_tx, ticket_rx) = async_channel::bounded(1);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1000 / REQ_PER_SEC));
        loop {
            interval.tick().await;
            if ticket_tx.send(()).await.is_err() {
                break;
            }
        }
    });

    let (writer_tx, writer_rx) = async_channel::bounded(100);
    let start_offset = std::env::var("START_OFFSET")
        .unwrap_or_else(|_| "0".to_string())
        .parse::<usize>()
        .unwrap_or(0);

    let start_chunk = std::env::var("START_CHUNK")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .unwrap_or(1);

    let state_for_writer = app_state.clone();
    let writer_handle = tokio::spawn(async move {
        writer_task(writer_rx, state_for_writer, start_chunk).await
    });

    let (offset_tx, offset_rx) = async_channel::bounded(100);
    tokio::spawn(async move {
        let mut current_offset = start_offset;
        loop {
            if offset_tx.send(current_offset).await.is_err() {
                break;
            }
            current_offset += ITEMS_PER_PAGE;
        }
    });

    let empty_pages = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut worker_handles = Vec::new();

    for i in 0..MAX_CONCURRENT_WORKERS {
        let client = client.clone();
        let ticket_rx = ticket_rx.clone();
        let writer_tx = writer_tx.clone();
        let offset_rx = offset_rx.clone();
        let empty_pages = empty_pages.clone();
        let state = app_state.clone();
        
        let handle = tokio::spawn(async move {
            worker_task(i, client, ticket_rx, writer_tx, offset_rx, empty_pages, state).await;
        });
        worker_handles.push(handle);
    }

    for handle in worker_handles {
        let _ = handle.await;
    }

    let _ = writer_tx.send(WriterMessage::Done).await;
    let _ = writer_handle.await;

    let mut final_state = app_state.lock().unwrap();
    final_state.log("All workers finished. Writer closed.".into());
    final_state.done = true;
}

async fn worker_task(
    worker_id: usize,
    client: Client,
    ticket_rx: async_channel::Receiver<()>,
    writer_tx: async_channel::Sender<WriterMessage>,
    offset_rx: async_channel::Receiver<usize>,
    empty_pages: Arc<std::sync::atomic::AtomicUsize>,
    app_state: Arc<Mutex<AppState>>,
) {
    let set_status = |msg: &str| {
        app_state.lock().unwrap().worker_status[worker_id] = msg.to_string();
    };
    let log = |msg: String| {
        app_state.lock().unwrap().log(msg);
    };

    while let Ok(offset) = offset_rx.recv().await {
        if empty_pages.load(std::sync::atomic::Ordering::SeqCst) > MAX_CONCURRENT_WORKERS * 2 {
            set_status("Finished (Pagination complete)");
            break;
        }

        let query = format!("SELECT * FROM messages LIMIT {} OFFSET {}", ITEMS_PER_PAGE, offset);
        let url = match url::Url::parse_with_params(BASE_URL, &[("q", query)]) {
            Ok(u) => u,
            Err(e) => {
                log(format!("[Worker {}] URL parse error: {}", worker_id, e));
                continue;
            }
        };

        let mut retries = 0;
        loop {
            set_status(&format!("Waiting for ticket (Offset: {})", offset));
            if ticket_rx.recv().await.is_err() {
                return;
            }

            set_status(&format!("Fetching Offset {}", offset));
            app_state.lock().unwrap().total_requests += 1;

            match client.get(url.clone()).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    
                    if status == StatusCode::TOO_MANY_REQUESTS || status.as_u16() == 1015 || status == StatusCode::FORBIDDEN {
                        app_state.lock().unwrap().rate_limited_count += 1;
                        if retries >= MAX_RETRIES {
                            log(format!("[Worker {}] Max retries reached for 429 at offset {}. Dropping.", worker_id, offset));
                            break;
                        }
                        let backoff = 2_u64.pow(retries).min(60);
                        set_status(&format!("Rate Limited ({}). Backoff {}s", status, backoff));
                        tokio::time::sleep(Duration::from_secs(backoff)).await;
                        retries += 1;
                        continue;
                    } else if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT {
                        app_state.lock().unwrap().error_count += 1;
                        if retries >= MAX_RETRIES {
                            log(format!("[Worker {}] Max retries reached for 5xx at offset {}. Dropping.", worker_id, offset));
                            break;
                        }
                        let backoff = 2_u64.pow(retries).min(60);
                        set_status(&format!("Server Error ({}). Backoff {}s", status, backoff));
                        tokio::time::sleep(Duration::from_secs(backoff)).await;
                        retries += 1;
                        continue;
                    } else if !status.is_success() {
                        app_state.lock().unwrap().error_count += 1;
                        log(format!("[Worker {}] Non-retryable error {} for offset {}", worker_id, status, offset));
                        break;
                    }

                    // Success parsing branch
                    match resp.json::<LithiumResponse>().await {
                        Ok(lithium_resp) => {
                            if let Some(data) = lithium_resp.data {
                                let item_count = data.items.len();
                                if item_count == 0 {
                                    empty_pages.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                    log(format!("[Worker {}] Empty page at offset {}", worker_id, offset));
                                } else {
                                    empty_pages.store(0, std::sync::atomic::Ordering::SeqCst);
                                    let _ = writer_tx.send(WriterMessage::Messages(data.items)).await;
                                }
                            } else {
                                log(format!("[Worker {}] No 'data' field at offset {}", worker_id, offset));
                            }
                        }
                        Err(e) => {
                            app_state.lock().unwrap().error_count += 1;
                            if retries >= MAX_RETRIES {
                                log(format!("[Worker {}] Max retries for JSON parse error at offset {}.", worker_id, offset));
                                break;
                            }
                            let backoff = 2_u64.pow(retries).min(60);
                            set_status(&format!("Parse Error. Backoff {}s", backoff));
                            log(format!("[Worker {}] JSON parse error: {}", worker_id, e));
                            tokio::time::sleep(Duration::from_secs(backoff)).await;
                            retries += 1;
                            continue;
                        }
                    }
                    break;
                }
                Err(e) => {
                    app_state.lock().unwrap().error_count += 1;
                    if retries >= MAX_RETRIES {
                        log(format!("[Worker {}] Max retries for connection error at offset {}.", worker_id, offset));
                        break;
                    }
                    let backoff = 2_u64.pow(retries).min(60);
                    set_status(&format!("Conn Error. Backoff {}s", backoff));
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    retries += 1;
                }
            }
        }
    }
}

async fn writer_task(rx: async_channel::Receiver<WriterMessage>, app_state: Arc<Mutex<AppState>>, start_chunk: usize) {
    let mut chunk_index = start_chunk;
    let mut current_items = 0;
    let mut current_file: Option<BufWriter<File>> = None;

    let log = |msg: String| {
        app_state.lock().unwrap().log(msg);
    };

    while let Ok(msg) = rx.recv().await {
        match msg {
            WriterMessage::Messages(messages) => {
                if current_file.is_none() {
                    let filename = format!("messages_chunk_{:03}.jsonl", chunk_index);
                    let f = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&filename)
                        .unwrap_or_else(|_| panic!("Failed to open {}", filename));
                    current_file = Some(BufWriter::new(f));
                    log(format!("Started new chunk file: {}", filename));
                }

                if let Some(ref mut writer) = current_file {
                    for msg in messages {
                        if let Ok(json_str) = serde_json::to_string(&msg) {
                            if writeln!(writer, "{}", json_str).is_ok() {
                                current_items += 1;
                                app_state.lock().unwrap().total_messages += 1;
                            }
                        }
                    }
                }

                if current_items >= ITEMS_PER_CHUNK_FILE {
                    if let Some(mut writer) = current_file.take() {
                        let _ = writer.flush();
                    }
                    chunk_index += 1;
                    current_items = 0;
                }
            }
            WriterMessage::Done => {
                if let Some(mut writer) = current_file.take() {
                    let _ = writer.flush();
                }
                log("Writer task shutting down...".into());
                break;
            }
        }
    }
}
