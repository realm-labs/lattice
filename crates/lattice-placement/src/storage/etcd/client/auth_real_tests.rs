use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use etcd_client::{
    Certificate, Client, ConnectOptions, DeleteOptions, GetOptions, Permission,
    RoleRevokePermissionOptions, TlsOptions,
};
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::{InstanceCapacity, InstanceId, InstanceIncarnation};
use lattice_core::{actor_kind, service_kind};
use tonic::Code;

use super::{
    EtcdAuthenticationAttemptOutcome, EtcdAuthenticationState, EtcdKv, RealEtcdClient,
    cached_authentication_attempt, record_authentication_attempt,
};
use crate::error::PlacementError;
use crate::registry::{InstanceRecord, InstanceState};
use crate::storage::etcd::codec::{actor_key, epoch_floor_key, instance_key};
use crate::storage::etcd::{
    EtcdConnectionOptions, EtcdPasswordAuthentication, EtcdPlacementStore, EtcdPlacementStoreConfig,
};
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementEpochKey, PlacementPrefix,
    PlacementState, PlacementStore,
};

const TEST_ETCD_AUTH_ENDPOINT: &str = "LATTICE_TEST_ETCD_AUTH_ENDPOINT";
const TEST_ETCD_AUTH_CA_FILE: &str = "LATTICE_TEST_ETCD_AUTH_CA_FILE";
const ROOT_USER: &str = "root";
const ROOT_PASSWORD: &str = "lattice-root-password";
const RUNTIME_USER: &str = "lattice-runtime";
const RUNTIME_PASSWORD: &str = "lattice-runtime-password";
const AUTHORITY_USER: &str = "lattice-authority";
const AUTHORITY_PASSWORD: &str = "lattice-authority-password";
const LEGACY_USER: &str = "lattice-legacy-writer";
const LEGACY_PASSWORD: &str = "lattice-legacy-password";

#[tokio::test]
async fn cancelled_authentication_attempt_retains_a_short_failure_backoff() {
    let authentication = EtcdAuthenticationState {
        username: "runtime".to_string(),
        password: "secret".to_string(),
        token: Arc::new(RwLock::new(None)),
        refresh_interval: Duration::from_secs(30),
        refresh_lock: tokio::sync::Mutex::new(()),
        last_attempt: StdMutex::new(None),
        attempt_count: AtomicU64::new(1),
    };
    record_authentication_attempt(&authentication, EtcdAuthenticationAttemptOutcome::Pending);

    assert!(cached_authentication_attempt(&authentication, false).is_none());
    assert_eq!(
        cached_authentication_attempt(&authentication, true)
            .expect("a canceled refresher must leave a conservative cached result")
            .unwrap_err(),
        PlacementError::EtcdAuthenticationFailed
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(cached_authentication_attempt(&authentication, true).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fresh TLS etcd with a <=3s JWT in LATTICE_TEST_ETCD_AUTH_ENDPOINT and LATTICE_TEST_ETCD_AUTH_CA_FILE"]
async fn real_etcd_authenticated_roles_fence_runtime_mutations() {
    let endpoint = std::env::var(TEST_ETCD_AUTH_ENDPOINT)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_AUTH_ENDPOINT} to a fresh disposable etcd"));
    assert!(!endpoint.trim().is_empty());
    assert!(
        endpoint.starts_with("https://localhost:"),
        "the TLS test endpoint must use the certificate's localhost SAN"
    );
    let ca_file = std::env::var(TEST_ETCD_AUTH_CA_FILE)
        .unwrap_or_else(|_| panic!("set {TEST_ETCD_AUTH_CA_FILE} to the test CA PEM"));
    let ca_bytes = std::fs::read(&ca_file).expect("read TLS test CA");
    let tls_options = || TlsOptions::new().ca_certificate(Certificate::from_pem(ca_bytes.clone()));

    let namespace = unique_namespace();
    let prefix = PlacementPrefix::new(namespace.clone());
    let namespace_range = format!("{namespace}/");
    let runtime_instance = InstanceId::new("world-runtime");
    let runtime_instance_key = instance_key(&prefix, &service_kind!("World"), &runtime_instance);

    let mut bootstrap = Client::connect(
        [endpoint.clone()],
        Some(ConnectOptions::new().with_tls(tls_options())),
    )
    .await
    .expect("connect to fresh unauthenticated etcd");
    bootstrap
        .user_add(ROOT_USER, ROOT_PASSWORD, None)
        .await
        .expect("add root user");
    bootstrap.role_add(ROOT_USER).await.expect("add root role");
    bootstrap
        .user_grant_role(ROOT_USER, ROOT_USER)
        .await
        .expect("grant root role");

    add_role_user(
        &mut bootstrap,
        RUNTIME_USER,
        RUNTIME_PASSWORD,
        [
            Permission::read(namespace_range.clone()).with_prefix(),
            Permission::write(runtime_instance_key.clone()),
        ],
    )
    .await;
    add_role_user(
        &mut bootstrap,
        AUTHORITY_USER,
        AUTHORITY_PASSWORD,
        [Permission::read_write(namespace_range.clone()).with_prefix()],
    )
    .await;
    add_role_user(
        &mut bootstrap,
        LEGACY_USER,
        LEGACY_PASSWORD,
        [Permission::read_write(namespace_range.clone()).with_prefix()],
    )
    .await;
    bootstrap.auth_enable().await.expect("enable etcd auth");
    drop(bootstrap);

    let mut root = Client::connect(
        [endpoint.clone()],
        Some(
            ConnectOptions::new()
                .with_user(ROOT_USER, ROOT_PASSWORD)
                .with_tls(tls_options()),
        ),
    )
    .await
    .expect("connect authenticated root");
    let secrets = tempfile::tempdir().unwrap();
    let runtime = authenticated_store(
        &endpoint,
        &namespace,
        RUNTIME_USER,
        RUNTIME_PASSWORD,
        secrets.path(),
        Path::new(&ca_file),
    )
    .await;
    let authority = authenticated_store(
        &endpoint,
        &namespace,
        AUTHORITY_USER,
        AUTHORITY_PASSWORD,
        secrets.path(),
        Path::new(&ca_file),
    )
    .await;
    let legacy = authenticated_store(
        &endpoint,
        &namespace,
        LEGACY_USER,
        LEGACY_PASSWORD,
        secrets.path(),
        Path::new(&ca_file),
    )
    .await;
    let initial_jwt_expiration = authentication_jwt_expiration(&runtime.client);
    let initial_jwt_ttl = initial_jwt_expiration
        .duration_since(SystemTime::now())
        .expect("fresh JWT must not already be expired");
    assert!(
        initial_jwt_ttl <= Duration::from_secs(3),
        "the authentication proof requires a short-lived JWT, got {initial_jwt_ttl:?}"
    );

    let bad_password_file = secrets.path().join("bad-password");
    std::fs::write(&bad_password_file, b"definitely-wrong").unwrap();
    assert_eq!(
        EtcdPlacementStore::<RealEtcdClient>::connect_with_connection_options(
            store_config(&endpoint, &namespace),
            authenticated_connection(RUNTIME_USER, bad_password_file, Path::new(&ca_file)),
        )
        .await
        .unwrap_err(),
        PlacementError::AuthenticatedEtcdConnect
    );
    let runtime_password_file = secrets.path().join(format!("{RUNTIME_USER}-password"));
    assert_eq!(
        EtcdPlacementStore::<RealEtcdClient>::connect_with_connection_options(
            store_config(&endpoint, &namespace),
            EtcdConnectionOptions::password_file(EtcdPasswordAuthentication::new(
                RUNTIME_USER,
                runtime_password_file.clone(),
            )),
        )
        .await
        .unwrap_err(),
        PlacementError::AuthenticatedEtcdConnect,
        "the generated test CA must not be trusted without explicit configuration"
    );
    let wrong_host_endpoint = endpoint.replacen("https://localhost:", "https://127.0.0.1:", 1);
    assert_eq!(
        EtcdPlacementStore::<RealEtcdClient>::connect_with_connection_options(
            store_config(&wrong_host_endpoint, &namespace),
            authenticated_connection(RUNTIME_USER, runtime_password_file, Path::new(&ca_file)),
        )
        .await
        .unwrap_err(),
        PlacementError::AuthenticatedEtcdConnect,
        "TLS must reject a certificate whose SAN does not match the endpoint host"
    );

    let mut anonymous = Client::connect(
        [endpoint.clone()],
        Some(ConnectOptions::new().with_tls(tls_options())),
    )
    .await
    .expect("anonymous transport connection may be established");
    assert_backend_anonymous_denied(
        anonymous
            .get(
                namespace_range.clone(),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .unwrap_err(),
    );

    let authority_lease = authority.grant_instance_lease().await.unwrap();
    let protected_key = actor_key_for(7);
    let protected_record = actor_record(7, "world-authority", authority_lease);
    let protected_version = authority
        .compare_and_put_actor(protected_key.clone(), None, protected_record.clone())
        .await
        .expect("authority creates protected actor and floor");
    let protected_floor_key =
        epoch_floor_key(&prefix, &PlacementEpochKey::Actor(protected_key.clone()));
    let (floor_version, _) = runtime
        .client
        .get(&protected_floor_key)
        .await
        .expect("runtime reads protected floor")
        .expect("authority floor exists");
    assert_eq!(protected_version, floor_version);
    assert_eq!(
        runtime.get_actor(&protected_key).await.unwrap(),
        Some((protected_version, protected_record))
    );
    let attempts_before = runtime.client.authentication_attempt_count_for_test();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let (first, second, third, fourth) = tokio::join!(
        runtime.get_actor(&protected_key),
        runtime.get_actor(&protected_key),
        runtime.get_actor(&protected_key),
        runtime.get_actor(&protected_key),
    );
    for result in [first, second, third, fourth] {
        assert!(result.unwrap().is_some());
    }
    assert_eq!(
        runtime.client.authentication_attempt_count_for_test(),
        attempts_before + 1,
        "concurrent operations must single-flight one bounded token refresh"
    );
    assert!(runtime.get_actor(&protected_key).await.unwrap().is_some());
    assert_eq!(
        runtime.client.authentication_attempt_count_for_test(),
        attempts_before + 1,
        "operations inside the refresh interval must not re-authenticate"
    );
    runtime
        .keepalive_instance_lease(authority_lease)
        .await
        .expect(
            "etcd RBAC does not key-authorize LeaseKeepAlive; runtime liveness must move behind an identity-bound authority API",
        );

    let runtime_lease = runtime.grant_instance_lease().await.unwrap();
    runtime
        .upsert_instance(instance_record(runtime_instance.clone(), runtime_lease))
        .await
        .expect("runtime writes only its exact liveness key");
    runtime
        .keepalive_instance_lease(runtime_lease)
        .await
        .expect("authenticated runtime refreshes its own liveness lease");
    assert_placement_permission_denied(
        runtime
            .upsert_instance(instance_record(
                InstanceId::new("world-other"),
                runtime_lease,
            ))
            .await,
    );

    let mut view = runtime
        .open_ownership_view(
            &service_kind!("World"),
            &runtime_instance,
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .expect("runtime read permission supports ownership snapshot and watch");
    assert_eq!(
        view.snapshot
            .local_instance
            .as_ref()
            .map(|record| &record.instance_id),
        Some(&runtime_instance)
    );
    assert_eq!(view.snapshot.records.len(), 1);
    let watch_attempts_before = runtime.client.authentication_attempt_count_for_test();
    let watch_jwt_expiration = authentication_jwt_expiration(&runtime.client);
    let wait_past_expiration = watch_jwt_expiration
        .duration_since(SystemTime::now())
        .unwrap_or_default()
        .saturating_add(Duration::from_millis(250));
    assert!(
        wait_past_expiration <= Duration::from_secs(3),
        "watch JWT expiry wait must stay bounded, got {wait_past_expiration:?}"
    );
    tokio::time::sleep(wait_past_expiration).await;
    authority
        .compare_and_put_actor(
            actor_key_for(10),
            None,
            actor_record(10, "world-authority", authority_lease),
        )
        .await
        .expect("authority creates a watched actor after the refresh interval");
    let watched_batch = tokio::time::timeout(Duration::from_secs(5), view.watch.next())
        .await
        .expect("authenticated ownership watch must not stall")
        .expect("authenticated ownership watch proof must refresh its token");
    assert!(!watched_batch.events.is_empty());
    assert_eq!(
        runtime.client.authentication_attempt_count_for_test(),
        watch_attempts_before + 1,
        "background ownership floor proofs must use the shared bounded token refresh"
    );
    drop(view);

    let denied_key = actor_key_for(8);
    assert_placement_permission_denied(
        runtime
            .compare_and_put_actor(
                denied_key.clone(),
                None,
                actor_record(8, runtime_instance.as_str(), runtime_lease),
            )
            .await,
    );
    assert!(runtime.get_actor(&denied_key).await.unwrap().is_none());
    assert!(
        runtime
            .client
            .get(&epoch_floor_key(
                &prefix,
                &PlacementEpochKey::Actor(denied_key)
            ))
            .await
            .unwrap()
            .is_none(),
        "unauthorized actor/floor transaction must commit neither key"
    );
    assert_placement_permission_denied(runtime.client.delete(&protected_floor_key).await);
    assert!(
        runtime
            .client
            .get(&protected_floor_key)
            .await
            .unwrap()
            .is_some()
    );

    let legacy_lease = legacy.grant_instance_lease().await.unwrap();
    let legacy_key = actor_key_for(9);
    legacy
        .compare_and_put_actor(
            legacy_key.clone(),
            None,
            actor_record(9, "legacy-owner", legacy_lease),
        )
        .await
        .expect("legacy credential writes before revocation");
    root.refresh_token()
        .await
        .expect("refresh short-lived root JWT before revocation");
    root.role_revoke_permission(
        LEGACY_USER,
        namespace_range.clone(),
        Some(RoleRevokePermissionOptions::new().with_prefix()),
    )
    .await
    .expect("revoke legacy writer range");
    assert_placement_credential_revoked(
        legacy.client.delete(&actor_key(&prefix, &legacy_key)).await,
    );

    root.refresh_token()
        .await
        .expect("refresh short-lived root JWT before password rotation");
    root.user_change_password(RUNTIME_USER, "rotated-away-from-runtime")
        .await
        .expect("rotate runtime password away from the connected client");
    let failed_attempts_before = runtime.client.authentication_attempt_count_for_test();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let (first, second, third, fourth) = tokio::join!(
        runtime.get_actor(&protected_key),
        runtime.get_actor(&protected_key),
        runtime.get_actor(&protected_key),
        runtime.get_actor(&protected_key),
    );
    for result in [first, second, third, fourth] {
        assert_eq!(
            result.unwrap_err(),
            PlacementError::EtcdAuthenticationFailed
        );
    }
    assert_eq!(
        runtime.client.authentication_attempt_count_for_test(),
        failed_attempts_before + 1,
        "concurrent failed refreshes must share one rate-bounded authentication attempt"
    );
    assert_eq!(
        runtime.get_actor(&protected_key).await.unwrap_err(),
        PlacementError::EtcdAuthenticationFailed
    );
    assert_eq!(
        runtime.client.authentication_attempt_count_for_test(),
        failed_attempts_before + 1,
        "a cached failed refresh must suppress repeated bcrypt work during backoff"
    );
    root.refresh_token()
        .await
        .expect("refresh short-lived root JWT before restoring the password");
    root.user_change_password(RUNTIME_USER, RUNTIME_PASSWORD)
        .await
        .expect("restore the runtime password after the simulated rotation gap");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(runtime.get_actor(&protected_key).await.unwrap().is_some());
    assert_eq!(
        runtime.client.authentication_attempt_count_for_test(),
        failed_attempts_before + 2,
        "the short failure backoff must permit prompt recovery before a normal lease TTL"
    );

    root.refresh_token()
        .await
        .expect("refresh short-lived root JWT before disabling auth");
    root.auth_disable().await.expect("disable etcd auth");
    drop((runtime, authority, legacy, root, anonymous));
    let mut cleanup = Client::connect(
        [endpoint],
        Some(ConnectOptions::new().with_tls(tls_options())),
    )
    .await
    .expect("connect cleanup client");
    cleanup
        .delete(namespace_range, Some(DeleteOptions::new().with_prefix()))
        .await
        .expect("clean test namespace");
    for user in [RUNTIME_USER, AUTHORITY_USER, LEGACY_USER, ROOT_USER] {
        cleanup.user_delete(user).await.expect("delete test user");
    }
    for role in [RUNTIME_USER, AUTHORITY_USER, LEGACY_USER, ROOT_USER] {
        cleanup.role_delete(role).await.expect("delete test role");
    }
}

async fn add_role_user<const N: usize>(
    client: &mut Client,
    name: &str,
    password: &str,
    permissions: [Permission; N],
) {
    client.role_add(name).await.expect("add test role");
    for permission in permissions {
        client
            .role_grant_permission(name, permission)
            .await
            .expect("grant test role permission");
    }
    client
        .user_add(name, password, None)
        .await
        .expect("add test user");
    client
        .user_grant_role(name, name)
        .await
        .expect("grant role to test user");
}

async fn authenticated_store(
    endpoint: &str,
    namespace: &str,
    username: &str,
    password: &str,
    secret_directory: &Path,
    ca_file: &Path,
) -> EtcdPlacementStore<RealEtcdClient> {
    let password_file = secret_directory.join(format!("{username}-password"));
    std::fs::write(&password_file, format!("{password}\n")).unwrap();
    EtcdPlacementStore::connect_with_connection_options(
        store_config(endpoint, namespace),
        authenticated_connection(username, password_file, ca_file),
    )
    .await
    .unwrap_or_else(|error| panic!("connect authenticated {username} store: {error}"))
}

fn authenticated_connection(
    username: &str,
    password_file: impl Into<std::path::PathBuf>,
    ca_file: &Path,
) -> EtcdConnectionOptions {
    EtcdConnectionOptions::password_file(EtcdPasswordAuthentication::new(username, password_file))
        .with_token_refresh_interval(Duration::from_secs(1))
        .with_ca_file(ca_file)
}

fn store_config(endpoint: &str, namespace: &str) -> EtcdPlacementStoreConfig {
    EtcdPlacementStoreConfig {
        key_prefix: namespace.to_string(),
        endpoints: vec![endpoint.to_string()],
        instance_lease_ttl_secs: 30,
        activation_lock_ttl_secs: 30,
    }
}

fn assert_backend_anonymous_denied(error: etcd_client::Error) {
    match error {
        etcd_client::Error::GRpcStatus(status) => assert!(
            matches!(
                status.code(),
                Code::InvalidArgument | Code::Unauthenticated | Code::PermissionDenied
            ),
            "anonymous etcd access must be rejected, got {status}"
        ),
        other => panic!("expected anonymous etcd access rejection, got {other}"),
    }
}

fn assert_placement_permission_denied<T>(result: Result<T, PlacementError>) {
    match result {
        Err(PlacementError::Etcd { message }) => assert!(
            message.to_ascii_lowercase().contains("permission denied"),
            "expected permission denied, got {message}"
        ),
        Err(other) => panic!("expected permission denied, got {other}"),
        Ok(_) => panic!("unauthorized placement operation unexpectedly succeeded"),
    }
}

fn assert_placement_credential_revoked<T>(result: Result<T, PlacementError>) {
    match result {
        Err(PlacementError::Etcd { message }) => assert!(
            message.to_ascii_lowercase().contains("permission denied")
                || message
                    .to_ascii_lowercase()
                    .contains("revision of auth store is old"),
            "expected immediate authorization revocation, got {message}"
        ),
        Err(other) => panic!("expected immediate authorization revocation, got {other}"),
        Ok(_) => panic!("revoked placement operation unexpectedly succeeded"),
    }
}

fn actor_key_for(actor_id: u64) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
    }
}

fn actor_record(actor_id: u64, owner: &str, lease_id: LeaseId) -> ActorPlacementRecord {
    ActorPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(1),
        lease_id,
        state: PlacementState::Running,
    }
}

fn instance_record(instance_id: InstanceId, lease_id: LeaseId) -> InstanceRecord {
    let incarnation = InstanceIncarnation::new(format!("{}-boot", instance_id.as_str()));
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id,
        incarnation,
        lease_id,
        advertised_endpoint: "http://127.0.0.1:50051".parse().unwrap(),
        control_endpoint: "http://127.0.0.1:50052".parse().unwrap(),
        version: "auth-test".to_string(),
        state: InstanceState::Ready,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}

fn unique_namespace() -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must follow Unix epoch")
        .as_nanos();
    format!(
        "/lattice/real-etcd-auth-tests/{}-{nonce}",
        std::process::id()
    )
}

fn authentication_jwt_expiration(client: &RealEtcdClient) -> SystemTime {
    let authentication = client
        .authentication
        .as_ref()
        .expect("test client must be authenticated");
    let token = authentication
        .token
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let token = token
        .as_ref()
        .and_then(|token| token.to_str().ok())
        .expect("test authentication token must be ASCII");
    let mut segments = token.split('.');
    let _header = segments.next().expect("JWT header segment");
    let payload = segments.next().expect("JWT payload segment");
    let _signature = segments.next().expect("JWT signature segment");
    assert!(segments.next().is_none(), "JWT must contain three segments");
    let payload = URL_SAFE_NO_PAD.decode(payload).expect("decode JWT payload");
    let claims: serde_json::Value = serde_json::from_slice(&payload).expect("decode JWT claims");
    let expiration = claims
        .get("exp")
        .and_then(serde_json::Value::as_u64)
        .expect("JWT exp claim must be an unsigned timestamp");
    UNIX_EPOCH + Duration::from_secs(expiration)
}
