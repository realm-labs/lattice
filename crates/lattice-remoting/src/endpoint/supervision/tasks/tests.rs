use std::{
    future::pending,
    sync::{Arc, mpsc},
    time::Duration,
};

use tokio::{runtime::Handle, sync::oneshot, time::timeout};

use super::IoTasks;

struct SlowResourceDrop {
    entered: Option<oneshot::Sender<()>>,
    release: mpsc::Receiver<()>,
}

impl Drop for SlowResourceDrop {
    fn drop(&mut self) {
        let _ = self.entered.take().unwrap().send(());
        self.release.recv_timeout(Duration::from_secs(2)).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_socket_resources_are_dropped_before_shutdown_reports_empty() {
    let tasks = Arc::new(IoTasks::default());
    let (entered, dropping) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let resource = SlowResourceDrop {
        entered: Some(entered),
        release: wait,
    };
    let task = tasks.spawn(&Handle::current(), async move {
        pending::<()>().await;
        drop(resource);
    });
    tasks.abort_all();
    timeout(Duration::from_secs(1), dropping)
        .await
        .unwrap()
        .unwrap();
    let premature = timeout(Duration::from_millis(30), tasks.wait_empty()).await;
    release.send(()).unwrap();
    assert!(
        premature.is_err(),
        "socket resources are still owned by the cancelled future"
    );
    timeout(Duration::from_secs(1), tasks.wait_empty())
        .await
        .unwrap();
    assert!(task.join().await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn dropping_a_join_waiter_cancels_a_socket_task_that_has_not_been_polled() {
    let tasks = Arc::new(IoTasks::default());
    let task = tasks.spawn(&Handle::current(), pending::<()>());
    drop(task);
    timeout(Duration::from_secs(1), tasks.wait_empty())
        .await
        .unwrap();
}
