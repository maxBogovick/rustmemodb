impl PersistApp {
    /// Executes a closure inside a retryable transactional context.
    ///
    /// Only transient write-write conflicts are retried according to
    /// `PersistAppPolicy::conflict_retry`.
    pub async fn transaction<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut(PersistTx) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 1usize;
        loop {
            let result = self
                .session
                .with_transaction(|tx_session| operation(PersistTx::new(tx_session)))
                .await;

            match result {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !self.should_retry_transaction_conflict(attempt, &err) {
                        return Err(err);
                    }

                    let backoff_ms = self.retry_backoff_ms(attempt);
                    warn!(
                        "PersistApp.transaction retry on conflict (attempt {} of {}): {} (backoff={}ms)",
                        attempt,
                        self.policy.conflict_retry.max_attempts.max(1),
                        err,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    attempt += 1;
                }
            }
        }
    }
}
