// Copyright 2025 The Casbin Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Redis Watcher for Casbin-RS
//!
//! This library provides a Redis-based watcher implementation for Casbin-RS,
//! allowing policy synchronization across multiple instances through Redis pub/sub.
//!
//! # Examples
//!
//! ## Standalone Redis
//!
//! ```rust,no_run
//! use redis_watcher::{RedisWatcher, WatcherOptions};
//! use casbin::prelude::*;
//!
//! fn main() -> redis_watcher::Result<()> {
//!     let options = WatcherOptions::default()
//!         .with_channel("/casbin-policy-updates".to_string())
//!         .with_ignore_self(true);
//!     
//!     let mut watcher = RedisWatcher::new("redis://127.0.0.1:6379", options)?;
//!     
//!     // Set callback to reload policies when notified
//!     watcher.set_update_callback(Box::new(|msg: String| {
//!         println!("Received policy update: {}", msg);
//!         // Reload your enforcer here
//!     }));
//!     
//!     // Use watcher with enforcer
//!     // let mut enforcer = Enforcer::new("model.conf", "policy.csv").await.unwrap();
//!     // enforcer.set_watcher(Box::new(watcher));
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Redis Cluster
//!
//! ```rust,no_run
//! use redis_watcher::{RedisWatcher, WatcherOptions};
//! use casbin::prelude::*;
//!
//! fn main() -> redis_watcher::Result<()> {
//!     let options = WatcherOptions::default()
//!         .with_channel("/casbin-policy-updates".to_string())
//!         .with_ignore_self(true);
//!     
//!     // Connect to Redis Cluster with multiple nodes
//!     let cluster_urls = "redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002";
//!     let mut watcher = RedisWatcher::new_cluster(cluster_urls, options)?;
//!     
//!     // Set callback to reload policies when notified
//!     watcher.set_update_callback(Box::new(|msg: String| {
//!         println!("Received policy update from cluster: {}", msg);
//!         // Reload your enforcer here
//!     }));
//!     
//!     Ok(())
//! }
//! ```

mod options;
mod watcher;

#[cfg(test)]
mod watcher_test;

pub use options::WatcherOptions;
pub use watcher::RedisWatcher;

/// Re-export for convenience
pub use watcher::{Message, Result, UpdateType, WatcherError};
