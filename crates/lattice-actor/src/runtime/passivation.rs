fn spawn_passivation_monitor<A>(
    handle: &ActorHandle<A>,
    passivation: PassivationPolicy,
) -> Option<watch::Sender<u64>>
where
    A: Actor,
{
    let PassivationPolicy::IdleTimeout(timeout) = passivation else {
        return None;
    };

    let (activity_tx, mut activity_rx) = watch::channel(0_u64);
    let handle = handle.clone();
    tokio::spawn(async move {
        loop {
            let observed = *activity_rx.borrow();
            tokio::select! {
                _ = tokio::time::sleep(timeout) => {
                    if *activity_rx.borrow() == observed {
                        let _ = handle.try_stop_internal(StopReason::Passivated(
                            PassivationReason::IdleTimeout,
                        ));
                        break;
                    }
                }
                changed = activity_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    });
    Some(activity_tx)
}

fn record_activity(activity_tx: Option<&watch::Sender<u64>>) {
    if let Some(activity_tx) = activity_tx {
        let next = activity_tx.borrow().wrapping_add(1);
        activity_tx.send_replace(next);
    }
}
