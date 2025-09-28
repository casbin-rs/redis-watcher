Redis Watcher
---

[![Build Status](https://github.com/casbin-rs/redis-watcher/actions/workflows/ci.yml/badge.svg)](https://github.com/casbin-rs/redis-watcher/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/casbin-rs/redis-watcher/branch/master/graph/badge.svg)](https://codecov.io/gh/casbin-rs/redis-watcher)


Redis Watcher is a [Redis](http://redis.io) watcher for [Casbin-RS](https://github.com/casbin/casbin-rs).

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
casbin-redis-watcher = "0.1"
casbin = "2.2"
tokio = { version = "1.0", features = ["full"] }
redis = { version = "0.32", features = ["tokio-comp", "aio"] }
```

## Simple Example

```rust
use casbin_redis_watcher::{RedisWatcher, WatcherOptions, Watcher};
use casbin::{prelude::*, Result as CasbinResult};
use std::sync::Arc;
use tokio::sync::Mutex;

fn update_callback(msg: &str) {
    println!("Policy updated: {}", msg);
}

#[tokio::main]
async fn main() -> casbin_redis_watcher::Result<()> {
    // Initialize the watcher.
    // Use the Redis URL as parameter.
    let options = WatcherOptions::default()
        .with_channel("/casbin".to_string())
        // Only exists in test, generally be true
        .with_ignore_self(false);
    
    let mut watcher = RedisWatcher::new("redis://localhost:6379", options).await?;

    // Or initialize the watcher in redis cluster.
    // let mut watcher = RedisWatcher::new_with_cluster(
    //     "redis://localhost:6379,redis://localhost:6380,redis://localhost:6381",
    //     options
    // ).await?;

    // Initialize the enforcer.
    let enforcer = Enforcer::new("examples/rbac_model.conf", "examples/rbac_policy.csv").await.unwrap();
    
    // Set the watcher for the enforcer.
    let enforcer = Arc::new(Mutex::new(enforcer));
    
    // Set callback to local example
    watcher.set_update_callback(update_callback).await?;
    
    // Or use the default callback
    // let callback = casbin_redis_watcher::default_update_callback(enforcer.clone());
    // watcher.set_update_callback(move |msg| callback(msg)).await?;

    // Start subscription
    watcher.start_subscription().await?;

    // Update the policy to test the effect.
    // You should see "Policy updated: [casbin rules updated]" in the log.
    watcher.update().await?;
    
    // Keep the program running to receive updates
    // In real applications, this would be your main application logic
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // Close the watcher
    watcher.close().await?;
    
    Ok(())
}
```

## Cluster Example

```rust
use casbin_redis_watcher::{RedisWatcher, WatcherOptions, Watcher};
use casbin::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> casbin_redis_watcher::Result<()> {
    let options = WatcherOptions::default()
        .with_channel("/casbin".to_string())
        .with_ignore_self(true);

    // Initialize watcher with Redis cluster
    let mut watcher = RedisWatcher::new_with_cluster(
        "redis://localhost:6379,redis://localhost:6380,redis://localhost:6381",
        options
    ).await?;

    let enforcer = Enforcer::new("examples/rbac_model.conf", "examples/rbac_policy.csv").await.unwrap();
    let enforcer = Arc::new(Mutex::new(enforcer));

    // Use default callback that automatically reloads policies
    let callback = casbin_redis_watcher::default_update_callback(enforcer.clone());
    watcher.set_update_callback(move |msg| callback(msg)).await?;

    // Start listening for policy changes
    watcher.start_subscription().await?;

    // Your application logic here...
    
    watcher.close().await?;
    Ok(())
}
```

## Configuration

### WatcherOptions

```rust
use casbin_redis_watcher::WatcherOptions;

let options = WatcherOptions::default()
    .with_channel("/casbin-policy-updates".to_string())  // Redis channel name
    .with_ignore_self(true)                              // Ignore self-generated updates
    .with_local_id("unique-instance-id".to_string());    // Unique identifier for this instance
```

### Update Types

The watcher supports various policy update types:

- `Update`: Generic update notification
- `UpdateForAddPolicy`: Single policy addition
- `UpdateForRemovePolicy`: Single policy removal
- `UpdateForRemoveFilteredPolicy`: Filtered policy removal
- `UpdateForSavePolicy`: Complete policy save
- `UpdateForAddPolicies`: Batch policy addition
- `UpdateForRemovePolicies`: Batch policy removal
- `UpdateForUpdatePolicy`: Single policy update
- `UpdateForUpdatePolicies`: Batch policy update

## Getting Help

- [Casbin-RS](https://github.com/casbin/casbin-rs)
- [Redis](https://github.com/redis-rs/redis-rs)
- [Tokio](https://tokio.rs)

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.
