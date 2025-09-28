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
//! This library provides Redis-based policy change notifications for Casbin-RS.
//! It's designed to be compatible with the Go version of casbin-redis-watcher.
//!
//! # Examples
//!
//! ```rust,no_run
//! use casbin_redis_watcher::{RedisWatcher, WatcherOptions, Watcher};
//! use casbin::prelude::*;
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//!
//! #[tokio::main]
//! async fn main() -> casbin_redis_watcher::Result<()> {
//!     let options = WatcherOptions::default()
//!         .with_channel("/casbin-policy-updates".to_string())
//!         .with_ignore_self(true);
//!     
//!     let mut watcher = RedisWatcher::new("redis://127.0.0.1:6379", options).await?;
//!     
//!     let enforcer = Enforcer::new("examples/rbac_model.conf", "examples/rbac_policy.csv").await.unwrap();
//!     let enforcer = Arc::new(Mutex::new(enforcer));
//!     
//!     let callback = casbin_redis_watcher::default_update_callback(enforcer.clone());
//!     watcher.set_update_callback(move |msg| callback(msg)).await?;
//!     
//!     // Update policy
//!     watcher.update().await?;
//!     
//!     // Close watcher
//!     watcher.close().await?;
//!     Ok(())
//! }
//! ```

mod options;
mod watcher;

#[cfg(test)]
mod watcher_test;

pub use options::WatcherOptions;
pub use watcher::{default_update_callback, RedisWatcher, Watcher};

/// Re-export for convenience
pub use watcher::{Message, Result, UpdateType, WatcherError};
