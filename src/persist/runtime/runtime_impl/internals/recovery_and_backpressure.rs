impl PersistEntityRuntime {
    /// Reads and parses all journal records with a sequence number greater than `greater_than_seq`.
    ///
    /// Used during startup recovery (`load_from_disk`) to replay events that occurred
    /// after the last snapshot.
    ///
    /// Returns a sorted vector of records.
    async fn read_journal_records(
        &self,
        greater_than_seq: u64,
    ) -> Result<Vec<RuntimeJournalRecord>> {
        let path = self.journal_path();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let mut records = Vec::new();
        let mut lines = BufReader::new(file).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?
        {
            if line.trim().is_empty() {
                continue;
            }

            let record = serde_json::from_str::<RuntimeJournalRecord>(&line).map_err(|err| {
                DbError::ExecutionError(format!("parse runtime journal record: {}", err))
            })?;

            if record.seq > greater_than_seq {
                records.push(record);
            }
        }

        records.sort_by(|a, b| a.seq.cmp(&b.seq));
        Ok(records)
    }

    /// Returns the current size of the journal file in bytes.
    ///
    /// Used for metrics and potentially for compaction triggers.
    async fn journal_size_bytes(&self) -> Result<u64> {
        let path = self.journal_path();
        if !path.exists() {
            return Ok(0);
        }

        let metadata = fs::metadata(&path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        Ok(metadata.len())
    }

    /// Acquires a permit from the `inflight` semaphore to limit concurrent operations.
    ///
    /// Implements backpressure by waiting for a slot or timing out if the system is overloaded.
    async fn acquire_inflight_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let timeout_ms = self.policy.backpressure.acquire_timeout_ms;
        let fut = self.inflight.clone().acquire_owned();

        timeout(TokioDuration::from_millis(timeout_ms), fut)
            .await
            .map_err(|_| {
                DbError::ExecutionError(format!(
                    "Backpressure: could not acquire operation slot within {}ms",
                    timeout_ms
                ))
            })?
            .map_err(|_| DbError::ExecutionError("Backpressure semaphore closed".to_string()))
    }

    /// Calculates the exponential backoff duration for a retry attempt.
    fn retry_backoff_ms(&self, attempt: u32) -> u64 {
        let base = self.policy.retry.initial_backoff_ms.max(1);
        let max = self.policy.retry.max_backoff_ms.max(base);
        let factor = 2u64.saturating_pow(attempt.saturating_sub(1));
        base.saturating_mul(factor).min(max)
    }

    /// Updates the status of an outbox record in the idempotency index.
    ///
    /// When an outbox record is dispatched (e.g., status changes to `Delivered`),
    /// this ensures the idempotency receipt reflects the latest state, so retries
    /// know the side effect was already handled.
    fn update_idempotency_outbox_status(&mut self, updated: &RuntimeOutboxRecord) {
        for receipt in self.idempotency_index.values_mut() {
            for outbox in &mut receipt.outbox {
                if outbox.outbox_id == updated.outbox_id {
                    outbox.status = updated.status.clone();
                }
            }
        }
    }
}
