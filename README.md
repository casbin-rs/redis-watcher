Redis Watcher
---

[![Crates.io](https://img.shields.io/crates/v/redis-watcher.svg)](https://crates.io/crates/redis-watcher)
[![Docs](https://docs.rs/redis-watcher/badge.svg)](https://docs.rs/redis-watcher)
[![Build Status](https://github.com/casbin-rs/redis-watcher/actions/workflows/ci.yml/badge.svg)](https://github.com/casbin-rs/redis-watcher/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/casbin-rs/redis-watcher/branch/master/graph/badge.svg)](https://codecov.io/gh/casbin-rs/redis-watcher)


Redis Watcher is a [Redis](http://redis.io) watcher for [Casbin-RS](https://github.com/casbin/casbin-rs).

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
redis-watcher = "0.1.0"
casbin = { version = "2.13.0", features = ["watcher"] }
tokio = { version = "1.0", features = ["rt-multi-thread", "macros", "time"] }
redis = { version = "0.32.6", features = ["tokio-comp", "cluster-async", "aio"] }
```

**Note**: The `watcher` feature is required for Casbin to enable watcher functionality. For Redis cluster support, include the `cluster-async` feature.

## Simple Example

```rust
use redis_watcher::{RedisWatcher, WatcherOptions};
use casbin::{prelude::*, Watcher, EventData};

fn main() -> redis_watcher::Result<()> {
    // Configure watcher options
    let options = WatcherOptions::default()
        .with_channel("/casbin".to_string())
        .with_ignore_self(false);  // Set to true in production to ignore self-updates
    
    // Create watcher for standalone Redis
    let mut watcher = RedisWatcher::new("redis://127.0.0.1:6379", options)?;

    // Set callback to handle policy updates
    watcher.set_update_callback(Box::new(|msg: String| {
        println!("Policy updated: {}", msg);
        // Reload your enforcer policies here
    }));

    // The watcher automatically starts subscription when callback is set
    // Now you can use it with Casbin enforcer
    // Your enforcer will be notified when policies change

    Ok(())
}
```

## Cluster Example

```rust
use redis_watcher::{RedisWatcher, WatcherOptions};
use casbin::{prelude::*, Watcher};

fn main() -> redis_watcher::Result<()> {
    let options = WatcherOptions::default()
        .with_channel("/casbin".to_string())
        .with_ignore_self(true);

    // Initialize watcher with Redis cluster
    // Provide comma-separated list of cluster nodes
    let mut watcher = RedisWatcher::new_cluster(
        "redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002",
        options
    )?;

    // Set up callback to handle policy updates
    watcher.set_update_callback(Box::new(|msg: String| {
        println!("Received policy update from cluster: {}", msg);
        // Parse message and reload enforcer policies
    }));

    // Watcher is now ready to receive cluster-wide policy updates
    
    Ok(())
}
```

## Configuration

### WatcherOptions

The `WatcherOptions` struct provides configuration for the Redis watcher:

```rust
use redis_watcher::WatcherOptions;

let options = WatcherOptions::default()
    .with_channel("/casbin-policy-updates".to_string())  // Redis channel name
    .with_ignore_self(true)                              // Ignore self-generated updates
    .with_local_id("unique-instance-id".to_string());    // Unique identifier for this instance
```

**Options Explained:**

- **`channel`**: Redis pub/sub channel name for policy updates (default: `"/casbin"`)
- **`ignore_self`**: When `true`, the watcher ignores messages it published itself, preventing circular updates (default: `false`)
- **`local_id`**: Unique identifier for this watcher instance, automatically generated using UUID v4 if not specified

**Best Practices:**
- Set `ignore_self` to `true` in production to avoid processing your own updates
- Use a descriptive `local_id` for easier debugging in multi-instance deployments
- Choose a channel name that doesn't conflict with other Redis applications

### Update Types

The watcher supports various policy update types through the `UpdateType` enum, which corresponds to different Casbin operations:

```rust
pub enum UpdateType {
    Update,                           // Generic update notification
    UpdateForAddPolicy,               // Single policy addition
    UpdateForRemovePolicy,            // Single policy removal
    UpdateForRemoveFilteredPolicy,    // Filtered policy removal
    UpdateForSavePolicy,              // Complete policy save
    UpdateForAddPolicies,             // Batch policy addition
    UpdateForRemovePolicies,          // Batch policy removal
    UpdateForUpdatePolicy,            // Single policy update
    UpdateForUpdatePolicies,          // Batch policy update
}
```

**Message Structure:**

Each update is published as a JSON message with the following structure:

```rust
pub struct Message {
    pub method: UpdateType,       // Type of update
    pub id: String,               // Sender's local_id
    pub sec: String,              // Policy section (e.g., "p", "g")
    pub ptype: String,            // Policy type
    pub old_rule: Vec<String>,    // Old policy rule
    pub old_rules: Vec<Vec<String>>,  // Old policy rules (batch)
    pub new_rule: Vec<String>,    // New policy rule
    pub new_rules: Vec<Vec<String>>,  // New policy rules (batch)
    pub field_index: i32,         // Field index for filtered operations
    pub field_values: Vec<String>, // Field values for filtered operations
}
```

**Integration with Casbin:**

The watcher automatically converts Casbin's `EventData` to these message types when you call `watcher.update(event_data)`. This ensures consistent synchronization across all instances.

## Getting Help

### Documentation

- **API Documentation**: [docs.rs/redis-watcher](https://docs.rs/redis-watcher)
- **Casbin-RS Documentation**: [Casbin-RS GitHub](https://github.com/casbin/casbin-rs)
- **Redis Client**: [redis-rs Documentation](https://github.com/redis-rs/redis-rs)
- **Async Runtime**: [Tokio Documentation](https://tokio.rs)

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.