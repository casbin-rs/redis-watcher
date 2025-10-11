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

//! Redis Watcher Tests
//!
//! Tests verify the Redis PubSub notification mechanism for policy updates.
//! Note: These tests only verify notifications, not complete policy synchronization.
//! For full synchronization, use a shared database adapter with callbacks.

#[cfg(test)]
mod tests {
    use crate::{RedisWatcher, WatcherOptions};
    use casbin::prelude::*;
    use std::sync::{Arc, Mutex};
    use tokio::time::{sleep, Duration};
    use uuid::Uuid;

    // Test configuration constants
    const REDIS_URL: &str = "redis://127.0.0.1:6379";
    const MODEL_PATH: &str = "examples/rbac_model.conf";
    const POLICY_PATH: &str = "examples/rbac_policy.csv";
    // Reduced sync delay after adding explicit wait_for_ready() calls
    // const SYNC_DELAY_MS: u64 = 2000; // No longer needed - using wait_for_ready()

    // ========== Helper Functions ==========

    /// Check if Redis is available for testing
    async fn is_redis_available() -> bool {
        if let Ok(client) = redis::Client::open(REDIS_URL) {
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                redis::cmd("PING")
                    .query_async::<String>(&mut conn)
                    .await
                    .is_ok()
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Check if Redis Cluster is available for testing
    async fn is_redis_cluster_available() -> bool {
        // Check environment variable first
        if std::env::var("REDIS_CLUSTER_AVAILABLE").unwrap_or_default() != "true" {
            println!("REDIS_CLUSTER_AVAILABLE not set to 'true'");
            return false;
        }

        let cluster_urls = std::env::var("REDIS_CLUSTER_URLS").unwrap_or_else(|_| {
            "redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002".to_string()
        });

        let urls: Vec<&str> = cluster_urls.split(',').map(|s| s.trim()).collect();
        println!("Checking Redis Cluster availability with URLs: {:?}", urls);

        if let Ok(client) = redis::cluster::ClusterClient::builder(urls).build() {
            match client.get_async_connection().await {
                Ok(mut conn) => match redis::cmd("PING").query_async::<String>(&mut conn).await {
                    Ok(response) => {
                        println!("Redis Cluster PING response: {}", response);
                        true
                    }
                    Err(e) => {
                        println!("Redis Cluster PING failed: {}", e);
                        false
                    }
                },
                Err(e) => {
                    println!("Failed to connect to Redis Cluster: {}", e);
                    false
                }
            }
        } else {
            println!("Failed to create Redis Cluster client");
            false
        }
    }

    #[tokio::test]
    async fn test_watcher_creation() {
        let options = WatcherOptions::default();
        let result = RedisWatcher::new(REDIS_URL, options);

        assert!(result.is_ok(), "Watcher creation should succeed");
    }

    // Distributed synchronization tests - verify notification mechanism between enforcers

    #[tokio::test]
    async fn test_watcher_notification_on_add_policy() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_add_policy_{}", Uuid::new_v4());

        // Create two enforcers with watchers to simulate distributed instances
        let wo1 = WatcherOptions::default()
            .with_channel(unique_channel.clone())
            .with_ignore_self(true)
            .with_local_id("enforcer1".to_string());

        let wo2 = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_ignore_self(true)
            .with_local_id("enforcer2".to_string());

        let mut e1 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();
        let _e2 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();

        // Track update message received by e2
        let update_message = Arc::new(Mutex::new(None::<String>));
        let update_clone = update_message.clone();

        let mut w1 = RedisWatcher::new(REDIS_URL, wo1).unwrap();
        let mut w2 = RedisWatcher::new(REDIS_URL, wo2).unwrap();

        // Wait for subscriptions to be ready before setting callbacks
        println!("Waiting for w1 subscription...");
        w1.wait_for_ready().await;
        println!("Waiting for w2 subscription...");
        w2.wait_for_ready().await;
        println!("Both watchers are ready");

        w1.set_update_callback(Box::new(|msg: String| {
            println!("[E1] Received update: {}", msg);
        }));

        w2.set_update_callback(Box::new(move |msg: String| {
            println!("[E2] Received update: {}", msg);
            *update_clone.lock().unwrap() = Some(msg);
        }));

        e1.set_watcher(Box::new(w1));

        sleep(Duration::from_millis(500)).await;

        // e1 adds a new policy - this should trigger watcher notification to e2
        let _ = e1
            .add_policy(vec![
                "alice".to_string(),
                "book1".to_string(),
                "write".to_string(),
            ])
            .await;

        sleep(Duration::from_millis(500)).await;

        // Verify e2's watcher received the update notification
        let received_msg = update_message.lock().unwrap();
        assert!(
            received_msg.is_some(),
            "E2 watcher should receive update notification from E1"
        );

        let msg = received_msg.as_ref().unwrap();
        assert!(
            msg.contains("UpdateForAddPolicy"),
            "Message should contain UpdateForAddPolicy"
        );
        assert!(
            msg.contains("alice"),
            "Message should contain the policy subject"
        );
        assert!(
            msg.contains("book1"),
            "Message should contain the policy object"
        );
        assert!(
            msg.contains("write"),
            "Message should contain the policy action"
        );

        println!("test_watcher_notification_on_add_policy passed");
    }

    #[tokio::test]
    async fn test_watcher_notification_on_remove_policy() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_remove_policy_{}", Uuid::new_v4());

        let wo1 = WatcherOptions::default()
            .with_channel(unique_channel.clone())
            .with_ignore_self(true)
            .with_local_id("enforcer1".to_string());

        let wo2 = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_ignore_self(true)
            .with_local_id("enforcer2".to_string());

        let mut e1 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();
        let _e2 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();

        let update_message = Arc::new(Mutex::new(None::<String>));
        let update_clone = update_message.clone();

        let mut w1 = RedisWatcher::new(REDIS_URL, wo1).unwrap();
        let mut w2 = RedisWatcher::new(REDIS_URL, wo2).unwrap();

        w1.wait_for_ready().await;
        w2.wait_for_ready().await;

        w1.set_update_callback(Box::new(|_| {}));
        w2.set_update_callback(Box::new(move |msg: String| {
            println!("[E2] Received update: {}", msg);
            *update_clone.lock().unwrap() = Some(msg);
        }));

        e1.set_watcher(Box::new(w1));

        sleep(Duration::from_millis(500)).await;

        // e1 removes a policy
        let _ = e1
            .remove_policy(vec![
                "alice".to_string(),
                "data1".to_string(),
                "read".to_string(),
            ])
            .await;

        sleep(Duration::from_millis(500)).await;

        let received_msg = update_message.lock().unwrap();
        assert!(
            received_msg.is_some(),
            "E2 watcher should receive update notification"
        );

        let msg = received_msg.as_ref().unwrap();
        assert!(
            msg.contains("UpdateForRemovePolicy"),
            "Message should contain UpdateForRemovePolicy"
        );
        assert!(
            msg.contains("alice"),
            "Message should contain the removed policy subject"
        );

        println!("test_watcher_notification_on_remove_policy passed");
    }

    #[tokio::test]
    async fn test_watcher_notification_on_add_policies() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_add_policies_{}", Uuid::new_v4());

        let wo1 = WatcherOptions::default()
            .with_channel(unique_channel.clone())
            .with_ignore_self(true)
            .with_local_id("enforcer1".to_string());

        let wo2 = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_ignore_self(true)
            .with_local_id("enforcer2".to_string());

        let mut e1 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();

        let update_message = Arc::new(Mutex::new(None::<String>));
        let update_clone = update_message.clone();

        let mut w1 = RedisWatcher::new(REDIS_URL, wo1).unwrap();
        let mut w2 = RedisWatcher::new(REDIS_URL, wo2).unwrap();

        w1.wait_for_ready().await;
        w2.wait_for_ready().await;

        w1.set_update_callback(Box::new(|_| {}));
        w2.set_update_callback(Box::new(move |msg: String| {
            println!("[E2] Received update: {}", msg);
            *update_clone.lock().unwrap() = Some(msg);
        }));

        e1.set_watcher(Box::new(w1));

        sleep(Duration::from_millis(500)).await;

        // Add multiple policies
        let rules = vec![
            vec!["jack".to_string(), "data4".to_string(), "read".to_string()],
            vec!["katy".to_string(), "data4".to_string(), "write".to_string()],
        ];
        let _ = e1.add_policies(rules).await;

        sleep(Duration::from_millis(500)).await;

        let received_msg = update_message.lock().unwrap();
        assert!(
            received_msg.is_some(),
            "E2 watcher should receive batch update notification"
        );

        let msg = received_msg.as_ref().unwrap();
        assert!(
            msg.contains("UpdateForAddPolicies"),
            "Message should contain UpdateForAddPolicies"
        );
        assert!(msg.contains("jack"), "Message should contain first policy");
        assert!(msg.contains("katy"), "Message should contain second policy");

        println!("test_watcher_notification_on_add_policies passed");
    }
    #[tokio::test]
    async fn test_three_enforcers_distributed_sync() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_three_enforcers_{}", Uuid::new_v4());

        // Simulate 3 distributed instances
        let mut enforcers = Vec::new();
        let mut callbacks = Vec::new();

        for i in 1..=3 {
            let wo = WatcherOptions::default()
                .with_channel(unique_channel.clone())
                .with_ignore_self(true)
                .with_local_id(format!("enforcer{}", i));

            let mut e = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();
            let mut w = RedisWatcher::new(REDIS_URL, wo).unwrap();

            w.wait_for_ready().await;

            let callback_received = Arc::new(Mutex::new(0));
            let callback_clone = callback_received.clone();

            let enforcer_id = i;
            w.set_update_callback(Box::new(move |msg: String| {
                println!("[E{}] Received update: {}", enforcer_id, msg);
                *callback_clone.lock().unwrap() += 1;
            }));

            e.set_watcher(Box::new(w));
            enforcers.push(e);
            callbacks.push(callback_received);
        }

        sleep(Duration::from_millis(500)).await;

        // Enforcer 1 adds a policy
        let _ = enforcers[0]
            .add_policy(vec![
                "user1".to_string(),
                "res1".to_string(),
                "read".to_string(),
            ])
            .await;

        sleep(Duration::from_millis(500)).await;

        // Enforcer 1 should not receive its own update (ignore_self=true)
        // Enforcers 2 and 3 should receive the update
        assert_eq!(
            *callbacks[0].lock().unwrap(),
            0,
            "E1 should not receive its own update"
        );
        assert_eq!(
            *callbacks[1].lock().unwrap(),
            1,
            "E2 should receive update from E1"
        );
        assert_eq!(
            *callbacks[2].lock().unwrap(),
            1,
            "E3 should receive update from E1"
        );

        println!("test_three_enforcers_distributed_sync passed");
    }

    // Tests for ignore_self behavior

    #[tokio::test]
    async fn test_ignore_self_true() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_ignore_true_{}", Uuid::new_v4());
        let callback_called = Arc::new(Mutex::new(false));
        let callback_called_clone = callback_called.clone();

        let wo = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_ignore_self(true)
            .with_local_id("test_watcher".to_string());

        let mut watcher = RedisWatcher::new(REDIS_URL, wo).unwrap();

        watcher.set_update_callback(Box::new(move |_msg: String| {
            *callback_called_clone.lock().unwrap() = true;
        }));

        sleep(Duration::from_millis(200)).await;

        // Watcher sends update to itself
        watcher.update(EventData::AddPolicy(
            "p".to_string(),
            "p".to_string(),
            vec!["test".to_string()],
        ));

        sleep(Duration::from_millis(300)).await;

        assert!(
            !*callback_called.lock().unwrap(),
            "Callback should NOT be called when ignore_self=true"
        );
        println!("test_ignore_self_true passed");
    }

    #[tokio::test]
    async fn test_ignore_self_false() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_ignore_false_{}", Uuid::new_v4());
        let callback_called = Arc::new(Mutex::new(false));
        let callback_called_clone = callback_called.clone();

        let wo = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_ignore_self(false);

        let mut watcher = RedisWatcher::new(REDIS_URL, wo).unwrap();

        watcher.set_update_callback(Box::new(move |_msg: String| {
            *callback_called_clone.lock().unwrap() = true;
        }));

        sleep(Duration::from_millis(200)).await;

        watcher.update(EventData::AddPolicy(
            "p".to_string(),
            "p".to_string(),
            vec!["test".to_string()],
        ));

        sleep(Duration::from_millis(300)).await;

        assert!(
            *callback_called.lock().unwrap(),
            "Callback SHOULD be called when ignore_self=false"
        );
        println!("test_ignore_self_false passed");
    }

    // Watcher trait implementation tests

    #[tokio::test]
    async fn test_watcher_trait_integration() {
        if !is_redis_available().await {
            println!("Skipping test - Redis not available");
            return;
        }

        let unique_channel = format!("test_trait_{}", Uuid::new_v4());
        let options = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_ignore_self(false);

        let watcher = RedisWatcher::new(REDIS_URL, options).expect("Failed to create watcher");

        // Test that watcher can be boxed as a Watcher trait object
        let mut boxed_watcher: Box<dyn Watcher> = Box::new(watcher);

        // Verify watcher responds to update calls
        boxed_watcher.update(EventData::AddPolicy(
            "p".to_string(),
            "p".to_string(),
            vec!["alice".to_string(), "data1".to_string(), "read".to_string()],
        ));

        println!("Watcher trait implementation test passed");
    }

    // Redis Cluster tests

    #[tokio::test]
    #[ignore] // Requires Redis Cluster to be running
    async fn test_redis_cluster_pubsub_notification() {
        if !is_redis_cluster_available().await {
            println!("Skipping test - Redis Cluster not available");
            return;
        }

        // CRITICAL: Redis Cluster PubSub messages DO NOT propagate between nodes
        // ALL watcher instances MUST connect to the SAME node for pub/sub to work
        // Use a single node URL instead of multiple nodes
        let pubsub_node = std::env::var("REDIS_CLUSTER_PUBSUB_NODE")
            .unwrap_or_else(|_| "redis://127.0.0.1:7000".to_string());

        println!("Redis Cluster PubSub Test Configuration");
        println!("IMPORTANT: All watchers MUST use the SAME node!");
        println!("PubSub node: {}", pubsub_node);

        let unique_channel = format!("test_cluster_sync_{}", Uuid::new_v4());
        println!("Using unique channel: {}", unique_channel);
        let channel_for_error = unique_channel.clone();

        // Create two enforcers with cluster watchers
        let wo1 = WatcherOptions::default()
            .with_channel(unique_channel.clone())
            .with_local_id("cluster_enforcer1".to_string())
            .with_ignore_self(true);

        let wo2 = WatcherOptions::default()
            .with_channel(unique_channel)
            .with_local_id("cluster_enforcer2".to_string())
            .with_ignore_self(true);

        let mut e1 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();
        let mut e2 = Enforcer::new(MODEL_PATH, POLICY_PATH).await.unwrap();

        let callback_received = Arc::new(Mutex::new(false));
        let callback_clone = callback_received.clone();
        let message_content = Arc::new(Mutex::new(String::new()));
        let message_clone = message_content.clone();

        println!(
            "Creating Redis Cluster watchers (both using node: {})...",
            pubsub_node
        );

        // [OK] CORRECT: Both watchers use the SAME single node URL
        let mut w1 = RedisWatcher::new_cluster(&pubsub_node, wo1)
            .expect("Failed to create cluster watcher1");
        let mut w2 = RedisWatcher::new_cluster(&pubsub_node, wo2)
            .expect("Failed to create cluster watcher2");

        println!("Waiting for watchers to be ready...");
        w1.wait_for_ready().await;
        w2.wait_for_ready().await;
        println!("Both cluster watchers are ready");

        println!("Setting up callbacks...");
        w1.set_update_callback(Box::new(|msg| {
            println!("[Cluster E1] Published update: {}", msg);
        }));
        w2.set_update_callback(Box::new(move |msg: String| {
            println!("[Cluster E2] Received update notification: {}", msg);
            *message_clone.lock().unwrap() = msg;
            *callback_clone.lock().unwrap() = true;
        }));

        e1.set_watcher(Box::new(w1));
        e2.set_watcher(Box::new(w2));

        println!("Waiting for final initialization...");
        sleep(Duration::from_millis(500)).await;

        println!("Adding policy via e1...");
        let unique_subject = format!("cluster-user-{}", Uuid::new_v4());
        // e1 adds a policy that is guaranteed to be new so the watcher emits an update
        let add_result = e1
            .add_policy(vec![
                unique_subject.clone(),
                "data2".to_string(),
                "write".to_string(),
            ])
            .await;
        println!("Add policy result: {:?}", add_result);

        println!("Waiting for message propagation...");
        sleep(Duration::from_millis(500)).await;

        // Check if callback was received with timeout
        let mut received = false;
        for i in 0..10 {
            received = *callback_received.lock().unwrap();
            if received {
                let msg = message_content.lock().unwrap();
                println!("Callback received after attempt {}: {}", i + 1, msg);
                break;
            }
            println!("Waiting for callback... attempt {}/10", i + 1);
            sleep(Duration::from_millis(500)).await;
        }

        if !received {
            let msg = message_content.lock().unwrap();
            eprintln!("Failed to receive callback after 10 attempts");
            eprintln!("Last message content: {}", msg);
            eprintln!("Callback received flag: {}", received);
        }

        assert!(
            received,
            "Cluster E2 should receive update notification. Check:\n\
             1. Both watchers connect to the same Redis node: {}\n\
             2. Channel name matches: {}\n\
             3. Redis Cluster is properly configured\n\
             4. Check logs above for publish/subscribe details\n\
             \n\
             Remember: Redis Cluster PubSub messages DO NOT propagate between nodes!\n\
             All instances MUST use the SAME node URL for PubSub.",
            pubsub_node, channel_for_error
        );

        // Note: This test verifies Redis PubSub notifications only.
        // For complete policy synchronization, use a shared database adapter with callbacks.

        // Verify that E1 has the new policy
        let p1 = e1.get_policy();
        assert!(
            p1.iter().any(|p| p.contains(&unique_subject)),
            "E1 should contain the newly added policy"
        );

        // Verify that E2's callback was properly invoked with the correct message content
        let msg = message_content.lock().unwrap();
        assert!(
            msg.contains(&unique_subject),
            "E2's received message should contain the new policy subject"
        );

        println!("Redis cluster PubSub notification test passed");
        println!("  - E1 successfully published policy change");
        println!("  - E2 successfully received notification via Redis Cluster PubSub");
        println!("  - Message content verified to contain correct policy data");
    }
}
