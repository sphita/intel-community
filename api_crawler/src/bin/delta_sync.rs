use chrono::{DateTime, FixedOffset};
use reqwest::Client;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let file_path = "delta_updates.jsonl";
    let state_file = "last_sync_time.txt";
    
    // 1. Read the cutoff time if it exists
    let cutoff_time = match fs::read_to_string(state_file) {
        Ok(t_str) => {
            let t = t_str.trim();
            println!("Found state file! Resuming from last sync time: {}", t);
            Some(DateTime::parse_from_rfc3339(t)?)
        }
        Err(_) => {
            println!("No state file found. Will fetch the top 1000 messages to initialize.");
            None
        }
    };

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;

    let mut offset = 0;
    let mut fetched_count = 0;
    let mut highest_seen_time: Option<DateTime<FixedOffset>> = None;
    let mut should_stop = false;

    println!("Starting Smart Delta Sync... pulling latest messages from Intel.");

    while !should_stop {
        let query = format!("SELECT * FROM messages ORDER BY post_time DESC LIMIT 100 OFFSET {}", offset);
        let url = format!("https://community.intel.com/api/2.0/search?q={}", urlencoding::encode(&query));

        match client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(json_data) = resp.json::<Value>().await {
                    if let Some(items) = json_data.get("data").and_then(|d| d.get("items")).and_then(|i| i.as_array()) {
                        if items.is_empty() {
                            break;
                        }
                        
                        let mut chunk_saved = 0;

                        for item in items {
                            // Extract post_time
                            if let Some(post_time_str) = item.get("post_time").and_then(|s| s.as_str()) {
                                if let Ok(msg_time) = DateTime::parse_from_rfc3339(post_time_str) {
                                    
                                    // Track the highest time seen (since we fetch descending, the very first item is the newest)
                                    if highest_seen_time.is_none() || msg_time > highest_seen_time.unwrap() {
                                        highest_seen_time = Some(msg_time);
                                    }

                                    // If we hit a message that is older than our last sync, STOP immediately.
                                    if let Some(cutoff) = cutoff_time {
                                        if msg_time <= cutoff {
                                            println!("Reached already-synced messages ({} <= {}). Stopping.", msg_time, cutoff);
                                            should_stop = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            
                            // Write to JSONL
                            let line = serde_json::to_string(item)?;
                            writeln!(file, "{}", line)?;
                            chunk_saved += 1;
                            fetched_count += 1;
                        }
                        
                        println!("Saved {} new messages from offset {}", chunk_saved, offset);
                        
                        // Safety fallback if no state file exists
                        if cutoff_time.is_none() && fetched_count >= 1000 {
                            println!("Fetched initial 1000 messages. Stopping.");
                            break;
                        }

                        offset += 100;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        println!("Failed to parse items array, breaking.");
                        break;
                    }
                }
            }
            Err(e) => {
                println!("Request failed: {}. Retrying in 5s...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
    
    // Save the new state
    if let Some(newest_time) = highest_seen_time {
        fs::write(state_file, newest_time.to_rfc3339())?;
        println!("Smart Delta Sync complete! Saved {} total new messages.", fetched_count);
        println!("Updated last sync time to: {}", newest_time.to_rfc3339());
    } else {
        println!("No messages were parsed. State file left unchanged.");
    }

    Ok(())
}
