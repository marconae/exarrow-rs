//! Session management for Exasol database connections.
//!
//! This module handles session lifecycle, state tracking, and connection pooling support.

use crate::connection::auth::{AuthResponseData, AuthenticationHandler};
use crate::error::ConnectionError;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Session configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Session idle timeout
    pub idle_timeout: Duration,

    /// Enable automatic session keepalive
    pub enable_keepalive: bool,

    /// Keepalive interval
    pub keepalive_interval: Duration,

    /// Maximum number of retries for failed operations
    pub max_retries: u32,

    /// Enable transaction auto-commit mode
    pub auto_commit: bool,

    /// Default fetch size for queries
    pub default_fetch_size: usize,

    /// Query timeout
    pub query_timeout: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(600),
            enable_keepalive: true,
            keepalive_interval: Duration::from_secs(60),
            max_retries: 3,
            auto_commit: true,
            default_fetch_size: 1000,
            query_timeout: Duration::from_secs(300),
        }
    }
}

/// Session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Session is being initialized
    Initializing,

    /// Session is connected and ready
    Ready,

    /// Session is executing a query
    Executing,

    /// Session is in a transaction
    InTransaction,

    /// Session is idle
    Idle,

    /// Session is being closed
    Closing,

    /// Session is closed
    Closed,

    /// Session encountered an error
    Error,
}

impl SessionState {
    /// Check if the session is active.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            SessionState::Ready
                | SessionState::Executing
                | SessionState::InTransaction
                | SessionState::Idle
        )
    }

    /// Check if the session can execute queries.
    pub fn can_execute(&self) -> bool {
        matches!(
            self,
            SessionState::Ready | SessionState::InTransaction | SessionState::Idle
        )
    }
}

/// Database session information and state tracking.
pub struct Session {
    /// Session ID from the server
    session_id: String,

    /// Server information from authentication
    server_info: AuthResponseData,

    /// Session configuration
    config: SessionConfig,

    /// Current session state
    state: Arc<RwLock<SessionState>>,

    /// Last activity timestamp
    last_activity: Arc<RwLock<Instant>>,

    /// Query execution counter
    query_count: AtomicU64,

    /// Transaction active flag
    in_transaction: AtomicBool,

    /// Current schema
    current_schema: Arc<RwLock<Option<String>>>,

    /// Session attributes
    attributes: Arc<RwLock<std::collections::HashMap<String, String>>>,
}

impl Session {
    /// Create a new session.
    pub fn new(session_id: String, server_info: AuthResponseData, config: SessionConfig) -> Self {
        Self {
            session_id,
            server_info,
            config,
            state: Arc::new(RwLock::new(SessionState::Ready)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
            query_count: AtomicU64::new(0),
            in_transaction: AtomicBool::new(false),
            current_schema: Arc::new(RwLock::new(None)),
            attributes: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get the session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Get server information.
    pub fn server_info(&self) -> &AuthResponseData {
        &self.server_info
    }

    /// Get session configuration.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Get current session state.
    pub async fn state(&self) -> SessionState {
        *self.state.read().await
    }

    /// Set session state.
    pub async fn set_state(&self, new_state: SessionState) {
        let mut state = self.state.write().await;
        *state = new_state;
    }

    /// Update last activity timestamp.
    pub async fn update_activity(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
    }

    /// Get time since last activity.
    pub async fn idle_duration(&self) -> Duration {
        let last_activity = self.last_activity.read().await;
        last_activity.elapsed()
    }

    /// Check if session is idle beyond timeout.
    pub async fn is_idle_timeout(&self) -> bool {
        self.idle_duration().await > self.config.idle_timeout
    }

    /// Increment query counter.
    pub fn increment_query_count(&self) -> u64 {
        self.query_count.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Get total query count.
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::SeqCst)
    }

    /// Check if in transaction.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::SeqCst)
    }

    /// Begin a transaction.
    pub async fn begin_transaction(&self) -> Result<(), ConnectionError> {
        let state = self.state().await;
        if !state.can_execute() {
            return Err(ConnectionError::ConnectionClosed);
        }

        if self.in_transaction() {
            return Err(ConnectionError::InvalidParameter {
                parameter: "transaction".to_string(),
                message: "Transaction already active".to_string(),
            });
        }

        self.in_transaction.store(true, Ordering::SeqCst);
        self.set_state(SessionState::InTransaction).await;
        self.update_activity().await;

        Ok(())
    }

    /// Commit the current transaction.
    pub async fn commit_transaction(&self) -> Result<(), ConnectionError> {
        if !self.in_transaction() {
            return Err(ConnectionError::InvalidParameter {
                parameter: "transaction".to_string(),
                message: "No active transaction".to_string(),
            });
        }

        self.in_transaction.store(false, Ordering::SeqCst);
        self.set_state(SessionState::Ready).await;
        self.update_activity().await;

        Ok(())
    }

    /// Rollback the current transaction.
    pub async fn rollback_transaction(&self) -> Result<(), ConnectionError> {
        if !self.in_transaction() {
            return Err(ConnectionError::InvalidParameter {
                parameter: "transaction".to_string(),
                message: "No active transaction".to_string(),
            });
        }

        self.in_transaction.store(false, Ordering::SeqCst);
        self.set_state(SessionState::Ready).await;
        self.update_activity().await;

        Ok(())
    }

    /// Get current schema.
    pub async fn current_schema(&self) -> Option<String> {
        self.current_schema.read().await.clone()
    }

    /// Set current schema.
    pub async fn set_current_schema(&self, schema: Option<String>) {
        let mut current_schema = self.current_schema.write().await;
        *current_schema = schema;
        self.update_activity().await;
    }

    /// Get a session attribute.
    pub async fn get_attribute(&self, key: &str) -> Option<String> {
        let attributes = self.attributes.read().await;
        attributes.get(key).cloned()
    }

    /// Set a session attribute.
    pub async fn set_attribute(&self, key: String, value: String) {
        let mut attributes = self.attributes.write().await;
        attributes.insert(key, value);
    }

    /// Remove a session attribute.
    pub async fn remove_attribute(&self, key: &str) -> Option<String> {
        let mut attributes = self.attributes.write().await;
        attributes.remove(key)
    }

    /// Close the session.
    pub async fn close(&self) -> Result<(), ConnectionError> {
        self.set_state(SessionState::Closing).await;

        // Clean up resources
        if self.in_transaction() {
            // Force rollback if in transaction
            self.in_transaction.store(false, Ordering::SeqCst);
        }

        self.set_state(SessionState::Closed).await;

        Ok(())
    }

    /// Check if session is closed.
    pub async fn is_closed(&self) -> bool {
        matches!(self.state().await, SessionState::Closed)
    }

    /// Mark session as having an error.
    pub async fn mark_error(&self) {
        self.set_state(SessionState::Error).await;
    }

    /// Validate session is ready for operations.
    pub async fn validate_ready(&self) -> Result<(), ConnectionError> {
        let state = self.state().await;

        match state {
            SessionState::Closed => Err(ConnectionError::ConnectionClosed),
            SessionState::Error => Err(ConnectionError::InvalidParameter {
                parameter: "session".to_string(),
                message: "Session is in error state".to_string(),
            }),
            SessionState::Closing => Err(ConnectionError::ConnectionClosed),
            _ if !state.is_active() => Err(ConnectionError::InvalidParameter {
                parameter: "session".to_string(),
                message: format!("Session is not active: {:?}", state),
            }),
            _ => Ok(()),
        }
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("session_id", &self.session_id)
            .field("config", &self.config)
            .field("query_count", &self.query_count())
            .field("in_transaction", &self.in_transaction())
            .finish()
    }
}

/// Session manager for connection pooling and lifecycle management.
pub struct SessionManager {
    /// Active sessions
    sessions: Arc<RwLock<std::collections::HashMap<String, Arc<Session>>>>,

    /// Session factory (authentication handler)
    _auth_handler: Arc<AuthenticationHandler>,

    /// Session configuration
    config: SessionConfig,
}

impl SessionManager {
    /// Create a new session manager.
    pub fn new(auth_handler: Arc<AuthenticationHandler>, config: SessionConfig) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(std::collections::HashMap::new())),
            _auth_handler: auth_handler,
            config,
        }
    }

    /// Register a new session.
    pub async fn register_session(&self, session: Arc<Session>) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(session.session_id().to_string(), session);
    }

    /// Get a session by ID.
    pub async fn get_session(&self, session_id: &str) -> Option<Arc<Session>> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).cloned()
    }

    /// Remove a session.
    pub async fn remove_session(&self, session_id: &str) -> Option<Arc<Session>> {
        let mut sessions = self.sessions.write().await;
        sessions.remove(session_id)
    }

    /// Get all active sessions.
    pub async fn active_sessions(&self) -> Vec<Arc<Session>> {
        let sessions = self.sessions.read().await;
        sessions.values().cloned().collect()
    }

    /// Close all sessions.
    pub async fn close_all(&self) -> Result<(), ConnectionError> {
        let sessions = {
            let mut sessions = self.sessions.write().await;
            let active: Vec<_> = sessions.drain().map(|(_, s)| s).collect();
            active
        };

        for session in sessions {
            session.close().await?;
        }

        Ok(())
    }

    /// Clean up idle sessions beyond timeout.
    pub async fn cleanup_idle_sessions(&self) -> usize {
        let sessions = self.sessions.read().await;
        let mut to_remove = Vec::new();

        for (id, session) in sessions.iter() {
            if session.is_idle_timeout().await {
                to_remove.push(id.clone());
            }
        }

        drop(sessions);

        let count = to_remove.len();
        for id in to_remove {
            if let Some(session) = self.remove_session(&id).await {
                let _ = session.close().await;
            }
        }

        count
    }
}

impl std::fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager")
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::auth::{AuthResponseData, Credentials};

    fn mock_server_info() -> AuthResponseData {
        AuthResponseData {
            session_id: "test_session".to_string(),
            protocol_version: 3,
            release_version: "7.1.0".to_string(),
            database_name: "EXA".to_string(),
            product_name: "Exasol".to_string(),
            max_data_message_size: 4_194_304,
            max_identifier_length: 128,
            max_varchar_length: 2_000_000,
            identifier_quote_string: "\"".to_string(),
            time_zone: "UTC".to_string(),
            time_zone_behavior: "INVALID TIMESTAMP TO DOUBLE".to_string(),
        }
    }

    #[tokio::test]
    async fn test_session_creation() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert_eq!(session.session_id(), "sess123");
        assert_eq!(session.state().await, SessionState::Ready);
        assert_eq!(session.query_count(), 0);
        assert!(!session.in_transaction());
    }

    #[tokio::test]
    async fn test_session_state_transitions() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert_eq!(session.state().await, SessionState::Ready);

        session.set_state(SessionState::Executing).await;
        assert_eq!(session.state().await, SessionState::Executing);

        session.set_state(SessionState::Idle).await;
        assert_eq!(session.state().await, SessionState::Idle);

        session.set_state(SessionState::Closed).await;
        assert_eq!(session.state().await, SessionState::Closed);
    }

    #[tokio::test]
    async fn test_session_query_counter() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert_eq!(session.increment_query_count(), 1);
        assert_eq!(session.increment_query_count(), 2);
        assert_eq!(session.query_count(), 2);
    }

    #[tokio::test]
    async fn test_session_transaction() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert!(!session.in_transaction());

        // Begin transaction
        session.begin_transaction().await.unwrap();
        assert!(session.in_transaction());
        assert_eq!(session.state().await, SessionState::InTransaction);

        // Cannot begin another transaction
        let result = session.begin_transaction().await;
        assert!(result.is_err());

        // Commit transaction
        session.commit_transaction().await.unwrap();
        assert!(!session.in_transaction());
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn test_session_rollback() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        session.begin_transaction().await.unwrap();
        assert!(session.in_transaction());

        session.rollback_transaction().await.unwrap();
        assert!(!session.in_transaction());
        assert_eq!(session.state().await, SessionState::Ready);
    }

    #[tokio::test]
    async fn test_session_schema() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert!(session.current_schema().await.is_none());

        session
            .set_current_schema(Some("MY_SCHEMA".to_string()))
            .await;
        assert_eq!(
            session.current_schema().await,
            Some("MY_SCHEMA".to_string())
        );

        session.set_current_schema(None).await;
        assert!(session.current_schema().await.is_none());
    }

    #[tokio::test]
    async fn test_session_attributes() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert!(session.get_attribute("key1").await.is_none());

        session
            .set_attribute("key1".to_string(), "value1".to_string())
            .await;
        assert_eq!(
            session.get_attribute("key1").await,
            Some("value1".to_string())
        );

        let removed = session.remove_attribute("key1").await;
        assert_eq!(removed, Some("value1".to_string()));
        assert!(session.get_attribute("key1").await.is_none());
    }

    #[tokio::test]
    async fn test_session_activity() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        session.update_activity().await;

        let idle = session.idle_duration().await;
        assert!(idle < Duration::from_millis(100));

        tokio::time::sleep(Duration::from_millis(10)).await;
        let idle = session.idle_duration().await;
        assert!(idle >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_session_close() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        assert!(!session.is_closed().await);

        session.close().await.unwrap();
        assert!(session.is_closed().await);
        assert_eq!(session.state().await, SessionState::Closed);
    }

    #[tokio::test]
    async fn test_session_validate_ready() {
        let session = Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        );

        // Ready state should validate
        assert!(session.validate_ready().await.is_ok());

        // Closed state should fail
        session.set_state(SessionState::Closed).await;
        assert!(session.validate_ready().await.is_err());

        // Error state should fail
        session.set_state(SessionState::Error).await;
        assert!(session.validate_ready().await.is_err());
    }

    #[tokio::test]
    async fn test_session_manager() {
        let creds = Credentials::new("admin".to_string(), "secret".to_string());
        let auth_handler = Arc::new(AuthenticationHandler::new(
            creds,
            "test".to_string(),
            "1.0".to_string(),
        ));
        let manager = SessionManager::new(auth_handler, SessionConfig::default());

        let session = Arc::new(Session::new(
            "sess123".to_string(),
            mock_server_info(),
            SessionConfig::default(),
        ));

        // Register session
        manager.register_session(session.clone()).await;

        // Get session
        let retrieved = manager.get_session("sess123").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().session_id(), "sess123");

        // Active sessions
        let active = manager.active_sessions().await;
        assert_eq!(active.len(), 1);

        // Remove session
        let removed = manager.remove_session("sess123").await;
        assert!(removed.is_some());

        // Session no longer found
        assert!(manager.get_session("sess123").await.is_none());
    }

    #[tokio::test]
    async fn test_session_manager_close_all() {
        let creds = Credentials::new("admin".to_string(), "secret".to_string());
        let auth_handler = Arc::new(AuthenticationHandler::new(
            creds,
            "test".to_string(),
            "1.0".to_string(),
        ));
        let manager = SessionManager::new(auth_handler, SessionConfig::default());

        // Register multiple sessions
        for i in 0..3 {
            let session = Arc::new(Session::new(
                format!("sess{}", i),
                mock_server_info(),
                SessionConfig::default(),
            ));
            manager.register_session(session).await;
        }

        assert_eq!(manager.active_sessions().await.len(), 3);

        // Close all
        manager.close_all().await.unwrap();
        assert_eq!(manager.active_sessions().await.len(), 0);
    }

    #[test]
    fn test_session_state_checks() {
        assert!(SessionState::Ready.is_active());
        assert!(SessionState::Executing.is_active());
        assert!(!SessionState::Closed.is_active());
        assert!(!SessionState::Error.is_active());

        assert!(SessionState::Ready.can_execute());
        assert!(SessionState::InTransaction.can_execute());
        assert!(!SessionState::Executing.can_execute());
        assert!(!SessionState::Closed.can_execute());
    }
}
