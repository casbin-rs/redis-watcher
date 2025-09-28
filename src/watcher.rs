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

use async_trait::async_trait;
use casbin::{CoreApi, Enforcer, MgmtApi};
use redis::{
    cluster::{ClusterClient, ClusterClientBuilder},
    cluster_async::ClusterConnection,
    AsyncCommands, Client, ProtocolVersion, PushInfo,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

// Type aliases to reduce complexity
type UpdateCallback = Box<dyn Fn(&str) + Send + Sync>;
type CallbackArc = Arc<RwLock<Option<UpdateCallback>>>;

// ========== Error Types ==========

#[derive(Error, Debug)]
pub enum WatcherError {
    #[error("Redis connection error: {0}")]
    RedisConnection(#[from] redis::RedisError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Callback not set")]
    CallbackNotSet,

    #[error("Watcher already closed")]
    AlreadyClosed,

    #[error("Configuration error: {0}")]
    Configuration(String),
}

pub type Result<T> = std::result::Result<T, WatcherError>;

// ========== Message Types ==========

/// Message types for communication between watcher instances
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub enum UpdateType {
    Update,
    UpdateForAddPolicy,
    UpdateForRemovePolicy,
    UpdateForRemoveFilteredPolicy,
    UpdateForSavePolicy,
    UpdateForAddPolicies,
    UpdateForRemovePolicies,
    UpdateForUpdatePolicy,
    UpdateForUpdatePolicies,
}

impl std::fmt::Display for UpdateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateType::Update => write!(f, "Update"),
            UpdateType::UpdateForAddPolicy => write!(f, "UpdateForAddPolicy"),
            UpdateType::UpdateForRemovePolicy => write!(f, "UpdateForRemovePolicy"),
            UpdateType::UpdateForRemoveFilteredPolicy => write!(f, "UpdateForRemoveFilteredPolicy"),
            UpdateType::UpdateForSavePolicy => write!(f, "UpdateForSavePolicy"),
            UpdateType::UpdateForAddPolicies => write!(f, "UpdateForAddPolicies"),
            UpdateType::UpdateForRemovePolicies => write!(f, "UpdateForRemovePolicies"),
            UpdateType::UpdateForUpdatePolicy => write!(f, "UpdateForUpdatePolicy"),
            UpdateType::UpdateForUpdatePolicies => write!(f, "UpdateForUpdatePolicies"),
        }
    }
}

/// Message structure for Redis pub/sub communication
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Message {
    pub method: UpdateType,
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sec: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ptype: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub old_rule: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub old_rules: Vec<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub new_rule: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub new_rules: Vec<Vec<String>>,
    #[serde(default)]
    pub field_index: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub field_values: Vec<String>,
}

impl Message {
    pub fn new(method: UpdateType, id: String) -> Self {
        Self {
            method,
            id,
            sec: String::new(),
            ptype: String::new(),
            old_rule: Vec::new(),
            old_rules: Vec::new(),
            new_rule: Vec::new(),
            new_rules: Vec::new(),
            field_index: 0,
            field_values: Vec::new(),
        }
    }

    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn unmarshal_binary(data: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(data)?)
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }
}

// ========== Default Update Callback ==========

/// Default update callback that works with Casbin enforcer wrapped in Arc<Mutex<>>
pub fn default_update_callback(
    enforcer: Arc<Mutex<Enforcer>>,
) -> impl Fn(&str) + Send + Sync + 'static {
    move |msg: &str| {
        let message = match Message::from_json(msg) {
            Ok(msg) => msg,
            Err(e) => {
                log::error!("Failed to parse message: {}", e);
                return;
            }
        };

        let enforcer = enforcer.clone();
        tokio::spawn(async move {
            let mut guard = enforcer.lock().await;
            let result = match message.method {
                UpdateType::Update | UpdateType::UpdateForSavePolicy => {
                    guard.load_policy().await.map(|_| true)
                }
                UpdateType::UpdateForAddPolicy => {
                    guard
                        .add_named_policy(&message.ptype, message.new_rule)
                        .await
                }
                UpdateType::UpdateForAddPolicies => {
                    guard
                        .add_named_policies(&message.ptype, message.new_rules)
                        .await
                }
                UpdateType::UpdateForRemovePolicy => {
                    guard
                        .remove_named_policy(&message.ptype, message.new_rule)
                        .await
                }
                UpdateType::UpdateForRemovePolicies => {
                    guard
                        .remove_named_policies(&message.ptype, message.new_rules)
                        .await
                }
                UpdateType::UpdateForRemoveFilteredPolicy => {
                    guard
                        .remove_filtered_named_policy(
                            &message.ptype,
                            message.field_index as usize,
                            message.field_values,
                        )
                        .await
                }
                UpdateType::UpdateForUpdatePolicy => {
                    // Update policy is not available in current Casbin-rs, simulate with remove + add
                    let remove_result = guard
                        .remove_named_policy(&message.ptype, message.old_rule)
                        .await;
                    if remove_result.unwrap_or(false) {
                        guard
                            .add_named_policy(&message.ptype, message.new_rule)
                            .await
                    } else {
                        Ok(false)
                    }
                }
                UpdateType::UpdateForUpdatePolicies => {
                    // Update policies is not available in current Casbin-rs, simulate with remove + add
                    let remove_result = guard
                        .remove_named_policies(&message.ptype, message.old_rules)
                        .await;
                    if remove_result.unwrap_or(false) {
                        guard
                            .add_named_policies(&message.ptype, message.new_rules)
                            .await
                    } else {
                        Ok(false)
                    }
                }
            };

            match result {
                Ok(success) => {
                    if !success {
                        log::warn!("Callback update policy failed");
                    }
                }
                Err(e) => {
                    log::error!("Update policy error: {}", e);
                }
            }
        });
    }
}

/// Trait for watcher implementations
#[async_trait]
pub trait Watcher: Send + Sync {
    /// Set update callback function
    async fn set_update_callback<F>(&mut self, callback: F) -> Result<()>
    where
        F: Fn(&str) + Send + Sync + 'static;

    /// Update - publishes a generic update message
    async fn update(&self) -> Result<()>;

    /// Update for add policy
    async fn update_for_add_policy(
        &self,
        sec: &str,
        ptype: &str,
        params: Vec<String>,
    ) -> Result<()>;

    /// Update for remove policy  
    async fn update_for_remove_policy(
        &self,
        sec: &str,
        ptype: &str,
        params: Vec<String>,
    ) -> Result<()>;

    /// Update for remove filtered policy
    async fn update_for_remove_filtered_policy(
        &self,
        sec: &str,
        ptype: &str,
        field_index: i32,
        field_values: Vec<String>,
    ) -> Result<()>;

    /// Update for save policy
    async fn update_for_save_policy(&self) -> Result<()>;

    /// Update for add policies (batch)
    async fn update_for_add_policies(
        &self,
        sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> Result<()>;

    /// Update for remove policies (batch)
    async fn update_for_remove_policies(
        &self,
        sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> Result<()>;

    /// Update for update policy
    async fn update_for_update_policy(
        &self,
        sec: &str,
        ptype: &str,
        old_rule: Vec<String>,
        new_rule: Vec<String>,
    ) -> Result<()>;

    /// Update for update policies (batch)
    async fn update_for_update_policies(
        &self,
        sec: &str,
        ptype: &str,
        old_rules: Vec<Vec<String>>,
        new_rules: Vec<Vec<String>>,
    ) -> Result<()>;

    /// Close the watcher
    async fn close(&mut self) -> Result<()>;
}

// ========== Redis Watcher Implementation ==========

pub struct RedisWatcher {
    sub_client: Option<Arc<Client>>,
    pub_client: Option<Arc<Client>>,
    cluster_client: Option<Arc<ClusterClient>>,
    cluster_connection: Arc<Mutex<Option<ClusterConnection>>>,
    push_receiver: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<PushInfo>>>>,
    options: crate::WatcherOptions,
    callback: CallbackArc,
    subscription_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    is_closed: Arc<RwLock<bool>>,
}

impl RedisWatcher {
    /// Create a new Redis watcher for standalone Redis
    pub async fn new(redis_url: &str, options: crate::WatcherOptions) -> Result<Self> {
        let client = Arc::new(Client::open(redis_url)?);

        // Test connection
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;

        let watcher = Self {
            sub_client: Some(client.clone()),
            pub_client: Some(client),
            cluster_client: None,
            cluster_connection: Arc::new(Mutex::new(None)),
            push_receiver: Arc::new(Mutex::new(None)),
            options,
            callback: Arc::new(RwLock::new(None)),
            subscription_handle: Arc::new(Mutex::new(None)),
            is_closed: Arc::new(RwLock::new(false)),
        };

        Ok(watcher)
    }

    /// Create a new Redis watcher for Redis cluster
    pub async fn new_with_cluster(
        cluster_urls: &str,
        options: crate::WatcherOptions,
    ) -> Result<Self> {
        let urls: Vec<&str> = cluster_urls.split(',').collect();

        // Create push channel for receiving pubsub messages
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Build cluster client with RESP3 protocol and push sender for pubsub support
        let cluster_client = Arc::new(
            ClusterClientBuilder::new(urls)
                .use_protocol(ProtocolVersion::RESP3)
                .push_sender(tx)
                .build()?,
        );

        // Test connection
        let mut conn = cluster_client.get_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;

        let watcher = Self {
            sub_client: None,
            pub_client: None,
            cluster_client: Some(cluster_client),
            cluster_connection: Arc::new(Mutex::new(Some(conn))),
            push_receiver: Arc::new(Mutex::new(Some(rx))),
            options,
            callback: Arc::new(RwLock::new(None)),
            subscription_handle: Arc::new(Mutex::new(None)),
            is_closed: Arc::new(RwLock::new(false)),
        };

        Ok(watcher)
    }

    async fn publish_message(&self, message: &Message) -> Result<()> {
        if *self.is_closed.read().await {
            return Err(WatcherError::AlreadyClosed);
        }

        let payload = message.to_json()?;

        if let Some(client) = &self.pub_client {
            let mut conn = client.get_multiplexed_async_connection().await?;
            let _: i32 = conn.publish(&self.options.channel, payload).await?;
        } else if let Some(cluster_client) = &self.cluster_client {
            let mut conn = cluster_client.get_async_connection().await?;
            let _: i32 = conn.publish(&self.options.channel, payload).await?;
        } else {
            return Err(WatcherError::Configuration(
                "No publish client available".to_string(),
            ));
        }

        Ok(())
    }

    /// Start subscription to Redis channel
    pub async fn start_subscription(&self) -> Result<()> {
        if *self.is_closed.read().await {
            return Err(WatcherError::AlreadyClosed);
        }

        let callback = self.callback.clone();
        let channel = self.options.channel.clone();
        let local_id = self.options.local_id.clone();
        let ignore_self = self.options.ignore_self;
        let is_closed = self.is_closed.clone();

        let handle = if let Some(client) = &self.sub_client {
            // Standalone Redis pubsub
            let client = client.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::standalone_subscription_loop(
                    client,
                    channel,
                    callback,
                    local_id,
                    ignore_self,
                    is_closed,
                )
                .await
                {
                    log::error!("Standalone subscription loop error: {}", e);
                }
            })
        } else if self.cluster_client.is_some() {
            // Cluster pubsub using push receiver
            let push_receiver = self.push_receiver.clone();
            let cluster_connection = self.cluster_connection.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::cluster_subscription_loop(
                    cluster_connection,
                    push_receiver,
                    channel,
                    callback,
                    local_id,
                    ignore_self,
                    is_closed,
                )
                .await
                {
                    log::error!("Cluster subscription loop error: {}", e);
                }
            })
        } else {
            return Err(WatcherError::Configuration(
                "No subscription client available".to_string(),
            ));
        };

        *self.subscription_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn standalone_subscription_loop(
        client: Arc<Client>,
        channel: String,
        callback: CallbackArc,
        local_id: String,
        ignore_self: bool,
        is_closed: Arc<RwLock<bool>>,
    ) -> Result<()> {
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.subscribe(&channel).await?;

        let mut stream = pubsub.on_message();

        while let Some(msg) = stream.next().await {
            if *is_closed.read().await {
                break;
            }

            let payload: String = match msg.get_payload() {
                Ok(p) => p,
                Err(e) => {
                    log::error!("Failed to get message payload: {}", e);
                    continue;
                }
            };

            // Handle close message
            if payload == "Close" {
                break;
            }

            match Message::from_json(&payload) {
                Ok(message) => {
                    let is_self = message.id == local_id;
                    if !(ignore_self && is_self) {
                        if let Some(cb) = callback.read().await.as_ref() {
                            cb(&payload);
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to parse message: {} with error: {}", payload, e);
                }
            }
        }

        Ok(())
    }

    async fn cluster_subscription_loop(
        cluster_connection: Arc<Mutex<Option<ClusterConnection>>>,
        push_receiver: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<PushInfo>>>>,
        channel: String,
        callback: CallbackArc,
        local_id: String,
        ignore_self: bool,
        is_closed: Arc<RwLock<bool>>,
    ) -> Result<()> {
        // Subscribe to the channel using cluster connection
        {
            let mut conn_guard = cluster_connection.lock().await;
            if let Some(ref mut conn) = *conn_guard {
                let _: () = conn.subscribe(&channel).await?;
            } else {
                return Err(WatcherError::Configuration(
                    "Cluster connection not available".to_string(),
                ));
            }
        }

        // Listen for push messages
        let mut receiver_guard = push_receiver.lock().await;
        if let Some(ref mut rx) = *receiver_guard {
            while let Some(push_info) = rx.recv().await {
                if *is_closed.read().await {
                    break;
                }

                // Process pubsub messages
                if push_info.kind == redis::PushKind::Message {
                    if let Some(redis::Value::BulkString(data)) = push_info.data.get(2) {
                        let payload = String::from_utf8_lossy(data);

                        // Handle close message
                        if payload == "Close" {
                            break;
                        }

                        match Message::from_json(&payload) {
                            Ok(message) => {
                                let is_self = message.id == local_id;
                                if !(ignore_self && is_self) {
                                    if let Some(cb) = callback.read().await.as_ref() {
                                        cb(&payload);
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to parse message: {} with error: {}",
                                    payload,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        } else {
            return Err(WatcherError::Configuration(
                "Push receiver not available".to_string(),
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Watcher for RedisWatcher {
    async fn set_update_callback<F>(&mut self, callback: F) -> Result<()>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        *self.callback.write().await = Some(Box::new(callback));
        self.start_subscription().await?;
        Ok(())
    }

    async fn update(&self) -> Result<()> {
        let message = Message::new(UpdateType::Update, self.options.local_id.clone());
        self.publish_message(&message).await
    }

    async fn update_for_add_policy(
        &self,
        sec: &str,
        ptype: &str,
        params: Vec<String>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForAddPolicy,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.new_rule = params;
        self.publish_message(&message).await
    }

    async fn update_for_remove_policy(
        &self,
        sec: &str,
        ptype: &str,
        params: Vec<String>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForRemovePolicy,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.new_rule = params;
        self.publish_message(&message).await
    }

    async fn update_for_remove_filtered_policy(
        &self,
        sec: &str,
        ptype: &str,
        field_index: i32,
        field_values: Vec<String>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForRemoveFilteredPolicy,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.field_index = field_index;
        message.field_values = field_values;
        self.publish_message(&message).await
    }

    async fn update_for_save_policy(&self) -> Result<()> {
        let message = Message::new(
            UpdateType::UpdateForSavePolicy,
            self.options.local_id.clone(),
        );
        self.publish_message(&message).await
    }

    async fn update_for_add_policies(
        &self,
        sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForAddPolicies,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.new_rules = rules;
        self.publish_message(&message).await
    }

    async fn update_for_remove_policies(
        &self,
        sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForRemovePolicies,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.new_rules = rules;
        self.publish_message(&message).await
    }

    async fn update_for_update_policy(
        &self,
        sec: &str,
        ptype: &str,
        old_rule: Vec<String>,
        new_rule: Vec<String>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForUpdatePolicy,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.old_rule = old_rule;
        message.new_rule = new_rule;
        self.publish_message(&message).await
    }

    async fn update_for_update_policies(
        &self,
        sec: &str,
        ptype: &str,
        old_rules: Vec<Vec<String>>,
        new_rules: Vec<Vec<String>>,
    ) -> Result<()> {
        let mut message = Message::new(
            UpdateType::UpdateForUpdatePolicies,
            self.options.local_id.clone(),
        );
        message.sec = sec.to_string();
        message.ptype = ptype.to_string();
        message.old_rules = old_rules;
        message.new_rules = new_rules;
        self.publish_message(&message).await
    }

    async fn close(&mut self) -> Result<()> {
        *self.is_closed.write().await = true;

        // Send close message
        if let Some(client) = &self.pub_client {
            let mut conn = client.get_multiplexed_async_connection().await?;
            let _: i32 = conn.publish(&self.options.channel, "Close").await?;
        } else if let Some(cluster_client) = &self.cluster_client {
            let mut conn = cluster_client.get_async_connection().await?;
            let _: i32 = conn.publish(&self.options.channel, "Close").await?;
        }

        // Wait for subscription task to finish
        if let Some(handle) = self.subscription_handle.lock().await.take() {
            handle.abort();
        }

        Ok(())
    }
}
