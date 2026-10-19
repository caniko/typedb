/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use serde_json::Value as JSONValue;

use crate::{
    DatabaseHash, DatabaseHashOpt, DatabaseId, MonitoringSection, hash_string_consistently,
    metrics::{
        ALL_CLIENT_ENDPOINTS, ActionMetrics, ClientEndpoint, DatabaseHistograms, DatabaseHistogramsSnapshot,
        ErrorMetrics, LoadMetrics, ServerMetrics, ServerProperties, client_endpoints_map,
    },
    reports::{json_monitoring::to_monitoring_json, prometheus_monitoring::to_monitoring_prometheus},
};

#[derive(Debug)]
pub struct CoreMetrics {
    pub(crate) server_properties: ServerProperties,
    pub(crate) server_metrics: ServerMetrics,
    load_metrics: RwLock<HashMap<DatabaseHash, LoadMetrics>>,
    action_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ActionMetrics>>>,
    error_metrics: HashMap<ClientEndpoint, RwLock<HashMap<DatabaseHashOpt, ErrorMetrics>>>,
    histogram_metrics: RwLock<HashMap<DatabaseHash, DatabaseHistograms>>,
}

impl CoreMetrics {
    pub(crate) fn new(
        deployment_id: String,
        server_id: String,
        distribution: String,
        version: String,
        data_directory: PathBuf,
        is_reporting_enabled: bool,
    ) -> Self {
        Self {
            server_properties: ServerProperties::new(deployment_id, server_id, distribution, is_reporting_enabled),
            server_metrics: ServerMetrics::new(version, data_directory),
            load_metrics: RwLock::new(HashMap::new()),
            action_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            error_metrics: client_endpoints_map!(RwLock::new(HashMap::new())),
            histogram_metrics: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn lock_load_metrics_read_for_database(
        &self,
        database_name: &str,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        let database_hash = hash_string_consistently(database_name);
        if let Some(lock) = self.try_lock_load_metrics_read_for_database(database_hash) {
            return lock;
        }
        self.add_database_to_load_metrics(DatabaseId::new(database_name));
        self.try_lock_load_metrics_read_for_database(database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_load_metrics_read_for_database(
        &self,
        database_hash: DatabaseHash,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>>> {
        let read_lock = self.lock_load_metrics_read();
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_load_metrics(&self, id: Arc<DatabaseId>) {
        let mut write_lock = self.lock_load_metrics_write();
        let database_hash = id.hash_value();
        write_lock.entry(database_hash).or_insert_with(|| LoadMetrics::new(id));
    }

    pub(crate) fn lock_load_metrics_read(&self) -> RwLockReadGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.read().expect("Expected read lock acquisition")
    }

    pub(crate) fn lock_load_metrics_write(&self) -> RwLockWriteGuard<'_, HashMap<DatabaseHash, LoadMetrics>> {
        self.load_metrics.write().expect("Expected write lock acquisition")
    }

    pub(crate) fn lock_histogram_metrics_read_for_database(
        &self,
        database_name: &str,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>> {
        let database_hash = hash_string_consistently(database_name);
        if let Some(lock) = self.try_lock_histogram_metrics_read_for_database(database_hash) {
            return lock;
        }
        self.add_database_to_histogram_metrics(DatabaseId::new(database_name));
        self.try_lock_histogram_metrics_read_for_database(database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_histogram_metrics_read_for_database(
        &self,
        database_hash: DatabaseHash,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>>> {
        let read_lock = self.lock_histogram_metrics_read();
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_histogram_metrics(&self, id: Arc<DatabaseId>) {
        let mut write_lock = self.lock_histogram_metrics_write();
        let database_hash = id.hash_value();
        write_lock.entry(database_hash).or_insert_with(|| DatabaseHistograms::new(id));
    }

    pub(crate) fn lock_histogram_metrics_read(&self) -> RwLockReadGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>> {
        self.histogram_metrics.read().expect("Expected read lock acquisition")
    }

    fn lock_histogram_metrics_write(&self) -> RwLockWriteGuard<'_, HashMap<DatabaseHash, DatabaseHistograms>> {
        self.histogram_metrics.write().expect("Expected write lock acquisition")
    }

    pub(crate) fn lock_action_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_name: Option<&str>,
        database_hash: DatabaseHashOpt,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        if let Some(lock) = self.try_lock_action_metrics_read_for_database(client, database_hash) {
            return lock;
        }
        self.add_database_to_action_metrics(client, database_name);
        self.try_lock_action_metrics_read_for_database(client, database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_action_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_hash: DatabaseHashOpt,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>>> {
        let read_lock = self.lock_action_metrics_read(client);
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_action_metrics(&self, client: ClientEndpoint, database_name: Option<&str>) {
        let id = database_name.map(DatabaseId::new);
        let database_hash = id.as_ref().map(|id| id.hash_value());
        let mut write_lock = self.lock_action_metrics_write(client);
        write_lock.entry(database_hash).or_insert_with(|| ActionMetrics::new(id));
    }

    pub(crate) fn lock_action_metrics_read(
        &self,
        client: ClientEndpoint,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        self.action_metrics
            .get(&client)
            .expect("Expected client {client}")
            .read()
            .expect("Expected read lock acquisition")
    }

    pub(crate) fn lock_action_metrics_write(
        &self,
        client: ClientEndpoint,
    ) -> RwLockWriteGuard<'_, HashMap<DatabaseHashOpt, ActionMetrics>> {
        self.action_metrics
            .get(&client)
            .expect("Expected client {client}")
            .write()
            .expect("Expected write lock acquisition")
    }

    pub(crate) fn lock_error_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_name: Option<&str>,
        database_hash: DatabaseHashOpt,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        if let Some(lock) = self.try_lock_error_metrics_read_for_database(client, database_hash) {
            return lock;
        }
        self.add_database_to_error_metrics(client, database_name);
        self.try_lock_error_metrics_read_for_database(client, database_hash)
            .expect("Expected metrics lock acquisition for database after adding")
    }

    fn try_lock_error_metrics_read_for_database(
        &self,
        client: ClientEndpoint,
        database_hash: DatabaseHashOpt,
    ) -> Option<RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>>> {
        let read_lock = self.lock_error_metrics_read(client);
        match read_lock.contains_key(&database_hash) {
            true => Some(read_lock),
            false => None,
        }
    }

    fn add_database_to_error_metrics(&self, client: ClientEndpoint, database_name: Option<&str>) {
        let id = database_name.map(DatabaseId::new);
        let database_hash = id.as_ref().map(|id| id.hash_value());
        let mut write_lock = self.lock_error_metrics_write(client);
        write_lock.entry(database_hash).or_insert_with(|| ErrorMetrics::new(id));
    }

    pub(crate) fn lock_error_metrics_read(
        &self,
        client: ClientEndpoint,
    ) -> RwLockReadGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        self.error_metrics
            .get(&client)
            .expect("Expected client {client}")
            .read()
            .expect("Expected read lock acquisition")
    }

    pub(crate) fn lock_error_metrics_write(
        &self,
        client: ClientEndpoint,
    ) -> RwLockWriteGuard<'_, HashMap<DatabaseHashOpt, ErrorMetrics>> {
        self.error_metrics
            .get(&client)
            .expect("Expected client {client}")
            .write()
            .expect("Expected write lock acquisition")
    }

    pub(crate) fn histogram_snapshots(&self) -> Vec<(Arc<DatabaseId>, DatabaseHistogramsSnapshot)> {
        self.lock_histogram_metrics_read().values().map(|db| (db.database_id().clone(), db.snapshot())).collect()
    }
}

impl MonitoringSection for CoreMetrics {
    fn name(&self) -> &str {
        "core"
    }

    fn write_prometheus(&self, out: &mut String) {
        out.push_str(&to_monitoring_prometheus(self));
    }

    fn write_json(&self) -> serde_json::Map<String, JSONValue> {
        to_monitoring_json(self)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use serde_json::json;

    use crate::{
        DatabaseId, Diagnostics,
        metrics::{ALL_CLIENT_ENDPOINTS, ActionKind, LoadKind, QueryType},
    };

    #[test]
    fn repeated_database_registration_preserves_metrics_and_reports() {
        let diagnostics = Diagnostics::new(
            "deployment_id".into(),
            "server_id".into(),
            "test".into(),
            "0.0.0".into(),
            PathBuf::from("."),
            true,
            true,
        );
        let metrics = diagnostics.core_metrics();
        let database_name = "diagnostics-lint";
        let id = DatabaseId::new(database_name);
        for _ in 0..2 {
            // Exercise an occupied entry, as when another caller wins the write lock.
            metrics.add_database_to_load_metrics(id.clone());
            metrics.add_database_to_histogram_metrics(id.clone());
            for client in ALL_CLIENT_ENDPOINTS {
                diagnostics.increment_load_count(client, database_name, LoadKind::ReadTransactions);
                diagnostics.observe_query_duration(database_name, QueryType::Read, Duration::from_millis(1));
                diagnostics.wal_metrics(database_name).record_bytes_written(1024);
                for database in [Some(database_name), None] {
                    metrics.add_database_to_action_metrics(client, database);
                    metrics.add_database_to_error_metrics(client, database);
                    diagnostics.submit_action_success(client, database, ActionKind::TransactionQuery);
                    diagnostics.submit_action_fail(client, database, ActionKind::TransactionQuery);
                    diagnostics.submit_error(client, database, "E1".into());
                }
            }
        }

        let monitoring = diagnostics.to_monitoring_json();
        assert_eq!(monitoring["serverID"], "server_id");
        assert_eq!(monitoring["load"].as_array().unwrap().len(), 1);
        let active = monitoring["load"][0]["activeTransactions"].as_array().unwrap();
        assert_eq!(active.iter().filter(|entry| entry["kind"] == "read" && entry["count"] == 2).count(), 2);
        let actions = monitoring["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions.iter().filter(|entry| entry["database"] == database_name).count(), 1);
        for action in actions {
            assert_eq!(action["attempted"], 8);
            assert_eq!(action["successful"], 4);
        }
        let errors = monitoring["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors.iter().filter(|entry| entry["database"] == database_name).count(), 1);
        assert!(errors.iter().all(|entry| entry["code"] == "E1" && entry["count"] == 4));
        assert_eq!(monitoring["queryDuration"][0]["histogram"]["count"], 4);
        assert_eq!(monitoring["walBytesWritten"][0]["value"], 4096);

        let posthog = diagnostics.to_posthog_reporting_json_against_snapshot("test_api_key");
        let batch = posthog["batch"].as_array().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.iter().filter(|report| report["event"] == "server_usage").count(), 1);
        let database = batch.iter().find(|report| report["event"] == "database_usage").unwrap();
        assert_eq!(database["properties"]["total_queries"], json!({"successful": 4, "failed": 4}));
        assert_eq!(database["properties"]["connection"]["read_transactions_peak_count"], json!({"grpc": 2, "http": 2}));
        for report in batch {
            for client in ["grpc", "http"] {
                assert_eq!(report["properties"]["transaction_queries"][client], json!({"successful": 2, "failed": 2}));
                assert_eq!(report["properties"]["user_errors"]["E1"][client]["count"], 2);
            }
        }
    }
}
