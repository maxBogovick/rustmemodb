impl PersistEntityRuntime {
    /// Applies a deterministic command to an entity by creating an envelope and processing it.
    ///
    /// This is a high-level entry point that constructs the `RuntimeCommandEnvelope` and deals
    /// with the result state.
    pub async fn apply_deterministic_command(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        command: &str,
        payload: serde_json::Value,
    ) -> Result<PersistState> {
        let envelope = RuntimeCommandEnvelope::new(entity_type, persist_id, command, payload);
        let result = self.apply_command_envelope(envelope).await?;
        Ok(result.state)
    }

    /// Resolves a command envelope, handling command migration rules.
    ///
    /// If a command migration is registered for the command in the envelope, the payload
    /// is transformed and the command name/version are updated in the returned envelope.
    fn resolve_command_envelope_for_execution(
        &self,
        envelope: RuntimeCommandEnvelope,
    ) -> Result<RuntimeCommandEnvelope> {
        if self
            .deterministic_registry
            .get(envelope.entity_type.as_str())
            .and_then(|commands| commands.get(envelope.command_name.as_str()))
            .is_some()
        {
            return Ok(envelope);
        }

        let Some(rules) = self
            .command_migration_registry
            .get(envelope.entity_type.as_str())
        else {
            return Ok(envelope);
        };

        let Some(rule) = rules.iter().find(|rule| {
            rule.descriptor.from_command == envelope.command_name
                && rule.descriptor.from_payload_version == envelope.payload_version
        }) else {
            return Ok(envelope);
        };

        let migrated_payload = (rule.transform)(&envelope.payload_json)?;

        let mut migrated = envelope;
        migrated.command_name = rule.descriptor.to_command.clone();
        migrated.payload_version = rule.descriptor.to_payload_version;
        migrated.payload_json = migrated_payload;
        Ok(migrated)
    }

    /// Processes a full command envelope, executing the deterministic handler.
    ///
    /// This core logic handles:
    /// - Envelope validation
    /// - Command migration resolution
    /// - Idempotency checks (replay handling)
    /// - Optimistic concurrency control (version checks)
    /// - Retries with backoff
    /// - Journaling and state updates
    /// - Outbox record creation
    /// - Projection maintenance
    pub async fn apply_command_envelope(
        &mut self,
        envelope: RuntimeCommandEnvelope,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let span = info_span!(
            "runtime.command.envelope",
            envelope_id = %envelope.envelope_id,
            entity_type = %envelope.entity_type,
            entity_id = %envelope.entity_id,
            command = %envelope.command_name
        );
        let _enter = span.enter();

        validate_command_envelope(&envelope)?;
        event!(
            Level::DEBUG,
            payload_version = envelope.payload_version,
            "runtime envelope accepted"
        );

        let envelope = self.resolve_command_envelope_for_execution(envelope)?;

        let command_handler = self
            .deterministic_registry
            .get(envelope.entity_type.as_str())
            .and_then(|commands| commands.get(envelope.command_name.as_str()))
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Deterministic command '{}' is not registered for entity type '{}'",
                    envelope.command_name, envelope.entity_type
                ))
            })?;
        if let Some(payload_schema) = &command_handler.payload_schema {
            payload_schema
                .validate(&envelope.payload_json)
                .map_err(|err| {
                    DbError::ExecutionError(format!(
                        "Payload validation for command '{}': {}",
                        envelope.command_name, err
                    ))
                })?;
        }

        let idempotency_scope_key = build_idempotency_scope_key(&envelope);
        if let Some(scope_key) = &idempotency_scope_key {
            if let Some(existing) = self.idempotency_index.get(scope_key) {
                event!(Level::INFO, "runtime envelope idempotent replay");
                return Ok(RuntimeEnvelopeApplyResult {
                    envelope_id: existing.envelope_id.clone(),
                    state: existing.state.clone(),
                    idempotent_replay: true,
                    outbox: existing.outbox.clone(),
                });
            }
        }

        if self.policy.determinism == RuntimeDeterminismPolicy::StrictContextOnly
            && !matches!(
                &command_handler.handler,
                RegisteredDeterministicCommandHandler::Context(_)
            )
        {
            return Err(DbError::ExecutionError(format!(
                "Determinism policy is StrictContextOnly; command '{}::{}' must be registered via register_deterministic_context_command[_with_schema]",
                envelope.entity_type, envelope.command_name
            )));
        }

        let key = RuntimeEntityKey::new(envelope.entity_type.clone(), envelope.entity_id.clone());
        let base = self.take_entity_for_mutation(&key)?;
        self.mailbox_start_command(&key);
        if let Some(expected_version) = envelope.expected_version {
            let actual_version = base.state.metadata.version.max(0) as u64;
            if expected_version != actual_version {
                self.hot_entities.insert(key.clone(), base);
                self.mailbox_complete_command(&key);
                event!(
                    Level::WARN,
                    expected_version,
                    actual_version,
                    "runtime envelope expected version mismatch"
                );
                return Err(DbError::ExecutionError(format!(
                    "Expected version mismatch for {}:{} (expected {}, actual {})",
                    envelope.entity_type, envelope.entity_id, expected_version, actual_version
                )));
            }
        }

        let max_attempts = self.policy.retry.max_attempts.max(1);
        let mut last_err: Option<DbError> = None;
        let deterministic_ctx = RuntimeDeterministicContext::from_envelope(&envelope);

        for attempt in 1..=max_attempts {
            let mut working = base.clone();
            let result = invoke_registered_handler(
                &command_handler.handler,
                &mut working.state,
                &envelope,
                &deterministic_ctx,
            );

            match result {
                Ok(side_effects) => {
                    working.state.metadata.persisted = true;
                    working.touch();
                    let outbox_records = side_effects
                        .into_iter()
                        .enumerate()
                        .map(|(index, effect)| RuntimeOutboxRecord {
                            outbox_id: format!("{}:{}", envelope.envelope_id, index),
                            envelope_id: envelope.envelope_id.clone(),
                            entity_type: envelope.entity_type.clone(),
                            entity_id: envelope.entity_id.clone(),
                            effect_type: effect.effect_type,
                            payload_json: effect.payload_json,
                            status: RuntimeOutboxStatus::Pending,
                            created_at: envelope.created_at,
                        })
                        .collect::<Vec<_>>();

                    let invocation = RuntimeCommandInvocation {
                        command: envelope.command_name.clone(),
                        payload: envelope.payload_json.clone(),
                    };

                    let projection_undo = match self.apply_projection_upsert(&working.state) {
                        Ok(undo) => undo,
                        Err(err) => {
                            event!(Level::ERROR, error = %err, "runtime projection upsert failed");
                            last_err = Some(err);
                            continue;
                        }
                    };

                    match self
                        .append_record(RuntimeJournalOp::Upsert {
                            entity: working.clone(),
                            reason: "command".to_string(),
                            command: Some(invocation),
                            envelope: Some(envelope.clone()),
                            outbox: outbox_records.clone(),
                            idempotency_scope_key: idempotency_scope_key.clone(),
                        })
                        .await
                    {
                        Ok(()) => {
                            self.tombstones.remove(&key);
                            self.hot_entities.insert(key.clone(), working.clone());
                            for record in &outbox_records {
                                self.outbox_records
                                    .insert(record.outbox_id.clone(), record.clone());
                            }
                            if let Some(scope_key) = idempotency_scope_key.as_ref() {
                                self.idempotency_index.insert(
                                    scope_key.clone(),
                                    RuntimeIdempotencyReceipt {
                                        envelope_id: envelope.envelope_id.clone(),
                                        entity_type: envelope.entity_type.clone(),
                                        entity_id: envelope.entity_id.clone(),
                                        command_name: envelope.command_name.clone(),
                                        state: working.state.clone(),
                                        outbox: outbox_records.clone(),
                                    },
                                );
                            }
                            let snapshot_result = self.maybe_snapshot_and_compact().await;
                            self.mailbox_complete_command(&key);
                            if let Err(err) = snapshot_result {
                                event!(
                                    Level::ERROR,
                                    error = %err,
                                    "runtime post-commit snapshot/compaction failed"
                                );
                                return Err(err);
                            }
                            event!(
                                Level::INFO,
                                attempt,
                                outbox_records = outbox_records.len(),
                                "runtime envelope applied"
                            );
                            return Ok(RuntimeEnvelopeApplyResult {
                                envelope_id: envelope.envelope_id,
                                state: working.state,
                                idempotent_replay: false,
                                outbox: outbox_records,
                            });
                        }
                        Err(err) => {
                            self.rollback_projection_undo(projection_undo);
                            event!(Level::ERROR, error = %err, "runtime journal append failed");
                            last_err = Some(err);
                        }
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, error = %err, "runtime deterministic handler failed");
                    last_err = Some(err);
                }
            }

            if attempt < max_attempts {
                sleep(TokioDuration::from_millis(self.retry_backoff_ms(attempt))).await;
            }
        }

        self.hot_entities.insert(key.clone(), base);
        self.mailbox_complete_command(&key);
        let err = last_err.unwrap_or_else(|| {
            DbError::ExecutionError("Failed to apply deterministic command".to_string())
        });
        event!(Level::ERROR, error = %err, "runtime envelope apply failed");
        Err(err)
    }
}
