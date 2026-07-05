use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::http::HeaderMap;

use lattice_core::{
    ActorId, Epoch, InstanceCapacity, InstanceId, TraceContext, actor_kind, service_kind,
};
use lattice_eventbus::{
    EventBus, EventEnvelope, EventId, EventSubscription, LocalEventBus, Subject, SubjectFilter,
};
use lattice_placement::{
    ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, InstanceRecord, InstanceState,
    LeaseId, NoopLogicControl, PlacementCoordinator, PlacementPrefix, PlacementState,
    PlacementStore,
};
use serde_json::json;

use super::*;

#[tokio::test]
async fn service_scheduler_cancels_interval_on_shutdown() {
    let scheduler = ServiceScheduler::new();
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_clone = ticks.clone();
    scheduler
        .interval(Duration::from_millis(5), move || {
            let ticks = ticks_clone.clone();
            async move {
                ticks.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;

    tokio::time::sleep(Duration::from_millis(20)).await;
    scheduler.shutdown().await;
    let after_shutdown = ticks.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(after_shutdown > 0);
    assert_eq!(ticks.load(Ordering::SeqCst), after_shutdown);
}

#[tokio::test]
async fn graceful_shutdown_drains_before_releasing_lease_and_cancels_runtime_work() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    store
        .upsert_instance(instance_record("world-a"))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b"))
        .await
        .unwrap();
    let actor_key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    store
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            ActorPlacementRecord {
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
                owner: InstanceId::new("world-a"),
                epoch: Epoch(1),
                lease_id: LeaseId(1),
                state: PlacementState::Running,
            },
        )
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let scheduler = ServiceScheduler::new();
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_clone = ticks.clone();
    scheduler
        .interval(Duration::from_millis(5), move || {
            let ticks = ticks_clone.clone();
            async move {
                ticks.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await;
    let bus = LocalEventBus::new();
    let deliveries = Arc::new(AtomicUsize::new(0));
    let deliveries_clone = deliveries.clone();
    let subscription = bus
        .subscribe(
            EventSubscription::local(SubjectFilter::new("system.shutdown.*")),
            move |_event| {
                let deliveries = deliveries_clone.clone();
                async move {
                    deliveries.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
    let lease_controller = InMemoryShutdownLeaseController::default();
    let shutdown = GracefulShutdown::new(
        service_kind!("World"),
        InstanceId::new("world-a"),
        coordinator,
        lease_controller.clone(),
        scheduler,
    );
    shutdown.own_subscription(subscription).await;

    let report = shutdown
        .shutdown(ShutdownTrigger::KubernetesPreStop)
        .await
        .unwrap();
    let migrated = store.get_actor(&actor_key).await.unwrap().unwrap().1;
    let drained = store
        .get_instance(&InstanceId::new("world-a"))
        .await
        .unwrap()
        .unwrap();
    let ticks_after_shutdown = ticks.load(Ordering::SeqCst);
    bus.publish(test_event("system.shutdown.done"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(!shutdown.is_ready());
    assert_eq!(
        report.stages,
        vec![
            ShutdownStage::ReadinessFalse,
            ShutdownStage::LeaseKeptAlive,
            ShutdownStage::SubscriptionsCancelled,
            ShutdownStage::Drained,
            ShutdownStage::SchedulerStopped,
            ShutdownStage::LeaseReleased,
        ]
    );
    assert_eq!(report.drain.migrated_actors, 1);
    assert_eq!(migrated.owner, InstanceId::new("world-b"));
    assert_eq!(drained.state, InstanceState::Draining);
    assert_eq!(deliveries.load(Ordering::SeqCst), 0);
    assert_eq!(ticks.load(Ordering::SeqCst), ticks_after_shutdown);
    assert_eq!(
        lease_controller.events().await,
        vec![
            LeaseEvent::KeepAlive(InstanceId::new("world-a")),
            LeaseEvent::Release(InstanceId::new("world-a")),
        ]
    );
}

#[tokio::test]
async fn cluster_inspector_summarizes_instances_and_actor_owners() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let instance = instance_record("world-a");
    store.upsert_instance(instance.clone()).await.unwrap();
    let actors = vec![ActorPlacementRecord {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
        owner: InstanceId::new("world-a"),
        epoch: Epoch(1),
        lease_id: LeaseId(1),
        state: PlacementState::Running,
    }];
    let inspector = ClusterInspector::new(store);

    let cluster = inspector
        .summarize(&service_kind!("World"), &actors)
        .await
        .unwrap();
    let node = inspector.summarize_node(&instance, &actors);

    assert_eq!(
        cluster,
        ClusterSummary {
            instance_count: 1,
            actor_owner_count: 1
        }
    );
    assert_eq!(node.actor_kinds, vec![actor_kind!("World")]);
}

#[test]
fn admin_auth_requires_configured_token() {
    let auth = AdminAuth::bearer_token("secret");
    let mut headers = HeaderMap::new();

    assert_eq!(auth.authorize(&headers), Err(AdminApiError::Unauthorized));
    headers.insert("x-lattice-admin-token", "secret".parse().unwrap());

    assert_eq!(auth.authorize(&headers), Ok(()));
}

#[test]
fn admin_pagination_reports_partial_results() {
    let page = paginate(
        &[1, 2, 3, 4],
        PageRequest {
            offset: 1,
            limit: 2,
        },
    );

    assert_eq!(page.items, vec![2, 3]);
    assert_eq!(page.total, 4);
    assert!(page.partial);
}

#[test]
fn admin_http_adapter_builds_axum_router() {
    let mut snapshot = AdminSnapshot::new(
        ClusterSummary {
            instance_count: 1,
            actor_owner_count: 0,
        },
        vec![InstanceView::from(instance_record("world-a"))],
    );
    snapshot.nodes.push(NodeInspectView {
        instance_id: InstanceId::new("world-a"),
        reachable: false,
        summary: None,
        error: Some("timeout".to_string()),
    });
    snapshot.nodes.push(NodeInspectView {
        instance_id: InstanceId::new("world-b"),
        reachable: true,
        summary: None,
        error: None,
    });
    snapshot.placements.push(inspection("World/7", "Running"));
    snapshot.virtual_shards.push(inspection("World#0", "Ready"));
    snapshot
        .singletons
        .push(inspection("Season/default", "Running"));
    snapshot.mailboxes.push(inspection("World/7", "Depth=0"));
    snapshot.schedulers.push(inspection("service", "Running"));
    snapshot
        .event_subscriptions
        .push(inspection("system.shutdown.*", "Active"));

    assert!(
        paginate(
            &snapshot.nodes,
            PageRequest {
                offset: 0,
                limit: 1
            }
        )
        .partial
    );
    let _router = AdminHttpAdapter::new(AdminAuth::disabled(), snapshot).router();
}

fn instance_record(instance_id: &str) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: "test".to_string(),
        state: InstanceState::Ready,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}

fn test_event(subject: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new("event-1"),
        subject: Subject::new(subject),
        event_type: "ShutdownEvent".to_string(),
        source_service: service_kind!("World"),
        source_instance: InstanceId::new("world-a"),
        actor_kind: None,
        actor_id: None,
        request_id: None,
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    }
}

fn inspection(name: &str, state: &str) -> InspectionView {
    InspectionView {
        name: name.to_string(),
        owner: Some(InstanceId::new("world-a")),
        state: state.to_string(),
        details: HashMap::new(),
    }
}

#[tokio::test]
async fn operation_tracker_models_retry_compensation_and_manual_repair() {
    let tracker = OperationTracker::default();
    let operation_id = OperationId::new("trade-1");

    tracker.start(operation_id.clone()).await.unwrap();
    tracker.mark_retrying(&operation_id, 1).await.unwrap();
    assert_eq!(
        tracker.get(&operation_id).await.unwrap().status,
        OperationStatus::Retrying { attempts: 1 }
    );

    tracker
        .mark_compensation_required(&operation_id, "debit applied but credit unknown")
        .await
        .unwrap();
    assert!(matches!(
        tracker.get(&operation_id).await.unwrap().status,
        OperationStatus::CompensationRequired { .. }
    ));

    tracker
        .mark_manual_required(&operation_id, "operator review")
        .await
        .unwrap();
    assert!(matches!(
        tracker.get(&operation_id).await.unwrap().status,
        OperationStatus::ManualRequired { .. }
    ));
}

#[tokio::test]
async fn transactional_outbox_tracks_unpublished_events_idempotently() {
    let outbox = TransactionalOutbox::default();
    let event_id = OutboxEventId::new("event-1");
    let event = OutboxEvent {
        event_id: event_id.clone(),
        topic: "game.world.player_entered".to_string(),
        payload: json!({ "world_id": 1, "player_id": 1001 }),
        published: false,
    };

    outbox.enqueue(event.clone()).await.unwrap();
    let duplicate = outbox.enqueue(event).await;
    assert!(matches!(duplicate, Err(OpsError::DuplicateOutboxEvent)));
    assert_eq!(outbox.unpublished().await.len(), 1);

    outbox.mark_published(&event_id).await.unwrap();
    assert!(outbox.unpublished().await.is_empty());
}

#[tokio::test]
async fn telemetry_records_span_links_and_rejects_high_cardinality_metric_labels() {
    let telemetry = TelemetryRecorder::default();
    let trace = TraceContext {
        traceparent: Some("trace-a".to_string()),
        tracestate: None,
    };
    let linked = TraceContext {
        traceparent: Some("trace-b".to_string()),
        tracestate: None,
    };

    telemetry
        .record_span(TraceSpan {
            name: "event fanout".to_string(),
            kind: TraceSpanKind::EventBus,
            context: trace.clone(),
            links: vec![linked.clone()],
        })
        .await;
    telemetry
        .record_metric(MetricSample {
            name: "actor_mailbox_depth".to_string(),
            value: 4,
            labels: HashMap::from([("actor_kind".to_string(), "World".to_string())]),
        })
        .await
        .unwrap();
    let bad_metric = telemetry
        .record_metric(MetricSample {
            name: "rpc_latency".to_string(),
            value: 10,
            labels: HashMap::from([("request_id".to_string(), "req-1".to_string())]),
        })
        .await;

    assert_eq!(telemetry.spans().await[0].links, vec![linked]);
    assert_eq!(telemetry.metrics().await.len(), 1);
    assert!(matches!(
        bad_metric,
        Err(OpsError::HighCardinalityMetricLabel { .. })
    ));
}

#[tokio::test]
async fn opentelemetry_pipeline_exports_resource_spans_metrics_and_links() {
    let telemetry = TelemetryRecorder::default();
    let exporter = InMemoryTelemetryExporter::default();
    let pipeline = TelemetryConfig::new("test").build_in_memory_pipeline(
        service_kind!("World"),
        InstanceId::new("world-a"),
        exporter.clone(),
    );
    let producer = TraceContext {
        traceparent: Some("producer-trace".to_string()),
        tracestate: None,
    };
    telemetry
        .record_span(TraceSpan {
            name: "event consumer".to_string(),
            kind: TraceSpanKind::EventBus,
            context: TraceContext {
                traceparent: Some("consumer-trace".to_string()),
                tracestate: None,
            },
            links: vec![producer.clone()],
        })
        .await;
    telemetry
        .record_metric(MetricSample {
            name: "eventbus_deliveries".to_string(),
            value: 1,
            labels: HashMap::from([("event_type".to_string(), "PlayerEntered".to_string())]),
        })
        .await
        .unwrap();

    pipeline.export_from(&telemetry).await.unwrap();
    let batches = exporter.batches().await;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].resource.service_kind, service_kind!("World"));
    assert_eq!(batches[0].spans[0].links, vec![producer]);
    assert_eq!(batches[0].metrics[0].name, "eventbus_deliveries");
}
