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

use uuid::Uuid;

/// Configuration options for the Redis watcher
/// This mirrors the Go version's WatcherOptions structure
#[derive(Debug, Clone)]
pub struct WatcherOptions {
    /// Redis channel for pub/sub
    pub channel: String,

    /// Whether to ignore messages from self
    pub ignore_self: bool,

    /// Local instance ID
    pub local_id: String,
}

impl Default for WatcherOptions {
    fn default() -> Self {
        Self {
            channel: "/casbin".to_string(),
            ignore_self: false,
            local_id: Uuid::new_v4().to_string(),
        }
    }
}

impl WatcherOptions {
    /// Create new WatcherOptions with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Redis pub/sub channel
    pub fn with_channel(mut self, channel: String) -> Self {
        self.channel = channel;
        self
    }

    /// Set whether to ignore self messages
    pub fn with_ignore_self(mut self, ignore_self: bool) -> Self {
        self.ignore_self = ignore_self;
        self
    }

    /// Set local instance ID
    pub fn with_local_id(mut self, local_id: String) -> Self {
        self.local_id = local_id;
        self
    }
}
