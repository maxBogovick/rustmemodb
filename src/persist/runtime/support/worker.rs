/// Background worker for periodic snapshotting.
pub struct RuntimeSnapshotWorker {
    runtime: Arc<Mutex<PersistEntityRuntime>>,
    stop_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl RuntimeSnapshotWorker {
    /// Signals the worker to stop and waits for it to finish.
    pub async fn stop(mut self) -> Result<()> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }

        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .await
                .map_err(|err| DbError::ExecutionError(format!("snapshot worker join: {}", err)))?;
        }

        let mut runtime = self.runtime.lock().await;
        runtime.snapshot_worker_running = false;
        Ok(())
    }
}

impl Drop for RuntimeSnapshotWorker {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort();
        }
    }
}

/// Spawns a new snapshot worker thread if configured.
pub async fn spawn_runtime_snapshot_worker(
    runtime: Arc<Mutex<PersistEntityRuntime>>,
) -> Result<RuntimeSnapshotWorker> {
    let interval_ms = {
        let mut guard = runtime.lock().await;
        let interval = guard
            .policy
            .snapshot
            .background_worker_interval_ms
            .ok_or_else(|| {
                DbError::ExecutionError(
                    "snapshot.background_worker_interval_ms must be configured to start worker"
                        .to_string(),
                )
            })?;
        guard.snapshot_worker_running = true;
        interval.max(10)
    };

    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let runtime_for_worker = runtime.clone();

    let join_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = sleep(TokioDuration::from_millis(interval_ms)) => {
                    let mut guard = runtime_for_worker.lock().await;
                    if let Err(err) = guard.run_snapshot_tick().await {
                        guard.record_snapshot_worker_error(&err);
                    }
                }
            }
        }
    });

    Ok(RuntimeSnapshotWorker {
        runtime,
        stop_tx: Some(stop_tx),
        join_handle: Some(join_handle),
    })
}
