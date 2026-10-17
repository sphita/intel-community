use chrono::{DateTime, FixedOffset};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::time::Duration;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const BASE_URL: &str = "https://community.intel.com/posts.json";
const MAX_RETRIES: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()?;
    let file_path = "delta_updates.jsonl";
    let state_file = "last_sync_time.txt";
    
    // 1. Read cutoff time if it exists
    let cutoff_time: Option<DateTime<FixedOffset>> = match fs::read_to_string(state_file) {
        Ok(t_str) => {
            let t = t_str.trim();
            if t.is_empty() {
                None
            } else {
                match DateTime::parse_from_rfc3339(t) {
                    Ok(dt) => {
                        println!("Found state file! Resuming from last sync time: {}", dt.to_rfc3339());
                        Some(dt)
                    }
                    Err(e) => {
                        println!("Failed to parse state file timestamp '{}': {}. Will fetch recent posts to initialize.", t, e);
                        None
                    }
                }
            }
        }
        Err(_) => {
            println!("No state file found. Will fetch recent posts to initialize.");
            None
        }
    };

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;

    let mut before_id: Option<i64> = None;
    let mut fetched_count = 0;
    let mut highest_seen_time: Option<DateTime<FixedOffset>> = None;
    let mut should_stop = false;

    println!("Starting Smart Delta Sync (Discourse API)... pulling latest messages from Intel.");

    while !should_stop {
        let url = match before_id {
            Some(id) => format!("{}?before={}", BASE_URL, id),
            None => BASE_URL.to_string(),
        };

        let mut retries = 0;
        let mut json_data: Option<Value> = None;

        loop {
            match client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        if retries >= MAX_RETRIES {
                            eprintln!("Rate limit exceeded (429). Max retries reached, aborting.");
                            should_stop = true;
                            break;
                        }
                        let backoff = 2_u64.pow(retries + 1).min(60);
                        println!("Rate limited (429). Backing off for {}s...", backoff);
                        tokio::time::sleep(Duration::from_secs(backoff)).await;
                        retries += 1;
                        continue;
                    } else if status.is_server_error() {
                        if retries >= MAX_RETRIES {
                            eprintln!("Server error ({}). Max retries reached, aborting.", status);
                            should_stop = true;
                            break;
                        }
                        let backoff = 2_u64.pow(retries + 1).min(60);
                        println!("Server error ({}). Retrying in {}s...", status, backoff);
                        tokio::time::sleep(Duration::from_secs(backoff)).await;
                        retries += 1;
                        continue;
                    } else if !status.is_success() {
                        eprintln!("HTTP request failed with status: {}. Aborting fetch.", status);
                        should_stop = true;
                        break;
                    }

                    match resp.json::<Value>().await {
                        Ok(data) => {
                            json_data = Some(data);
                            break;
                        }
                        Err(e) => {
                            eprintln!("Failed to parse JSON response: {}. Aborting fetch.", e);
                            should_stop = true;
                            break;
                        }
                    }
                }
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        eprintln!("Network request failed: {}. Max retries reached, aborting.", e);
                        should_stop = true;
                        break;
                    }
                    let backoff = 2_u64.pow(retries + 1).min(60);
                    println!("Network request failed: {}. Retrying in {}s...", e, backoff);
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    retries += 1;
                }
            }
        }

        if should_stop || json_data.is_none() {
            break;
        }

        let data = json_data.unwrap();
        let items = match data.get("latest_posts").and_then(|i| i.as_array()) {
            Some(arr) if !arr.is_empty() => arr,
            _ => {
                println!("No more posts returned from API.");
                break;
            }
        };

        let mut chunk_saved = 0;
        let mut min_id: Option<i64> = None;

        for item in items {
            let id = item.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
            if min_id.is_none() || id < min_id.unwrap() {
                min_id = Some(id);
            }

            // Extract post created_at timestamp
            if let Some(created_at_str) = item.get("created_at").and_then(|s| s.as_str()) {
                if let Ok(msg_time) = DateTime::parse_from_rfc3339(created_at_str) {
                    // Track highest seen time (only if newer than cutoff)
                    if highest_seen_time.is_none() || msg_time > highest_seen_time.unwrap() {
                        if let Some(cutoff) = cutoff_time {
                            if msg_time > cutoff {
                                highest_seen_time = Some(msg_time);
                            }
                        } else {
                            highest_seen_time = Some(msg_time);
                        }
                    }

                    // Stop if reached cutoff
                    if let Some(cutoff) = cutoff_time {
                        if msg_time <= cutoff {
                            println!("Reached already-synced messages ({} <= {}). Stopping.", msg_time.to_rfc3339(), cutoff.to_rfc3339());
                            should_stop = true;
                            break;
                        }
                    }
                }
            }

            // Map Discourse fields to standard raw schema
            let post_number = item.get("post_number").and_then(|v| v.as_i64()).unwrap_or(1);
            let depth = if post_number == 1 { 0 } else { 1 };
            let topic_id = item.get("topic_id").map(|v| v.to_string()).unwrap_or_default();
            let category_id = item.get("category_id").map(|v| v.to_string()).unwrap_or_default();
            let user_id = item.get("user_id").map(|v| v.to_string()).unwrap_or_default();
            let username = item.get("username").and_then(|v| v.as_str()).unwrap_or("unknown");
            let topic_title = item.get("topic_title").and_then(|v| v.as_str()).unwrap_or("");
            let subject = if depth == 0 { topic_title.to_string() } else { format!("Re: {}", topic_title) };
            let post_time = item.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
            let body = item.get("cooked").and_then(|v| v.as_str()).unwrap_or("");
            let is_solution = item.get("accepted_answer").and_then(|v| v.as_bool()).unwrap_or(false)
                || item.get("topic_accepted_answer").and_then(|v| v.as_bool()).unwrap_or(false);

            let reactions_sum: i64 = item.get("reactions")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|r| r.get("count").and_then(|c| c.as_i64())).sum())
                .unwrap_or(0);
            let likes_sum: i64 = item.get("actions_summary")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter()
                    .filter(|a| a.get("id").and_then(|id| id.as_i64()) == Some(2))
                    .filter_map(|a| a.get("count").and_then(|c| c.as_i64()))
                    .sum()
                )
                .unwrap_or(0);
            let total_kudos = reactions_sum + likes_sum;

            let mapped_record = json!({
                "id": id.to_string(),
                "topic": { "id": topic_id },
                "depth": depth,
                "author": { "id": user_id, "login": username },
                "post_time": post_time,
                "is_solution": is_solution,
                "body": body,
                "kudos": { "sum": total_kudos },
                "board": { "id": category_id },
                "subject": subject,
            });

            let line = serde_json::to_string(&mapped_record)?;
            writeln!(file, "{}", line)?;
            chunk_saved += 1;
            fetched_count += 1;
        }

        println!("Saved {} new messages (batch before_id: {:?})", chunk_saved, before_id);

        if cutoff_time.is_none() && fetched_count >= 100 {
            println!("Fetched initial 100 messages. Stopping.");
            break;
        }

        if let Some(m_id) = min_id {
            before_id = Some(m_id);
        } else {
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Save new state if we saw newer messages
    if let Some(newest_time) = highest_seen_time {
        fs::write(state_file, newest_time.to_rfc3339())?;
        println!("Smart Delta Sync complete! Saved {} total new messages.", fetched_count);
        println!("Updated last sync time to: {}", newest_time.to_rfc3339());
    } else {
        println!("Dataset is already up to date. Saved {} new messages. State file left unchanged.", fetched_count);
    }

    Ok(())
}

