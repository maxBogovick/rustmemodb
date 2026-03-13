impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
{
    /// Marks in-memory id index as stale.
    ///
    /// Any mutation path that may change entity ordering, ids, or persisted flags
    /// must call this before leaving the method.
    pub(super) fn mark_persisted_index_dirty(&self) {
        self.persisted_index_dirty
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Rebuilds `persist_id -> index` lookup map when needed.
    fn ensure_persisted_index(&self) {
        if !self
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        let mut guard = self
            .persisted_index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !self
            .persisted_index_dirty
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        guard.clear();
        for (index, item) in self.collection.items().iter().enumerate() {
            if item.metadata().persisted {
                guard.insert(item.persist_id().to_string(), index);
            }
        }

        self.persisted_index_dirty
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Returns current item index for persisted entity id.
    ///
    /// Includes a one-time self-heal pass when index entry is stale due to
    /// direct low-level collection mutation.
    pub(super) fn persisted_item_index(&self, persist_id: &str) -> Option<usize> {
        self.ensure_persisted_index();

        let candidate = self
            .persisted_index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(persist_id)
            .copied()?;

        if self.persisted_item_index_matches(candidate, persist_id) {
            return Some(candidate);
        }

        // Low-level callers can mutate collection directly through `collection_mut`.
        // Rebuild once to avoid returning stale references.
        self.mark_persisted_index_dirty();
        self.ensure_persisted_index();
        let refreshed = self
            .persisted_index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(persist_id)
            .copied()?;

        if self.persisted_item_index_matches(refreshed, persist_id) {
            Some(refreshed)
        } else {
            None
        }
    }

    fn persisted_item_index_matches(&self, index: usize, persist_id: &str) -> bool {
        self.collection.items().get(index).is_some_and(|item| {
            item.metadata().persisted && item.persist_id() == persist_id
        })
    }

    /// Ensures that entity `persist_id` is present in in-memory collection.
    ///
    /// Strategy:
    /// 1. Fast path: return existing in-memory index.
    /// 2. Miss path: hydrate entity from storage (`get_one_db`) and return index.
    #[allow(dead_code)]
    pub(super) async fn ensure_item_loaded_by_id(
        &mut self,
        persist_id: &str,
    ) -> Result<Option<usize>>
    where
        V::Item: PersistEntityFactory,
    {
        if let Some(index) = self.persisted_item_index(persist_id) {
            return Ok(Some(index));
        }

        // DB-first miss handling: hydrate from storage, then resolve index via cache map.
        if !self.refresh_cached_item_from_db(persist_id).await? {
            return Ok(None);
        }

        Ok(self.persisted_item_index(persist_id))
    }

    /// Session-aware variant of `ensure_item_loaded_by_id`.
    ///
    /// Uses the provided session as the storage source of truth so callers running
    /// in an explicit transaction (`with_session` / `with_tx`) do not accidentally
    /// hydrate from a different read context.
    pub(crate) async fn ensure_item_loaded_by_id_with_session(
        &mut self,
        session: &PersistSession,
        persist_id: &str,
    ) -> Result<Option<usize>>
    where
        V::Item: PersistEntityFactory,
    {
        if let Some(index) = self.persisted_item_index(persist_id) {
            return Ok(Some(index));
        }

        if !self
            .refresh_cached_item_from_db_with_session(session, persist_id)
            .await?
        {
            return Ok(None);
        }

        Ok(self.persisted_item_index(persist_id))
    }

    /// Loads one entity from storage by id and returns hydrated state.
    ///
    /// Returns `None` when row does not exist in storage.
    #[allow(dead_code)]
    async fn load_item_from_db_by_id(&self, persist_id: &str) -> Result<Option<V::Item>>
    where
        V::Item: PersistEntityFactory,
    {
        self.load_item_from_db_by_id_with_session(&self.session, persist_id)
            .await
    }

    /// Session-aware entity hydration by id from storage.
    async fn load_item_from_db_by_id_with_session(
        &self,
        session: &PersistSession,
        persist_id: &str,
    ) -> Result<Option<V::Item>>
    where
        V::Item: PersistEntityFactory,
    {
        let table_name = self.lookup_table_name_for_db_hydration();
        let sql = format!(
            "SELECT * FROM {} WHERE __persist_id = '{}' LIMIT 1",
            table_name,
            crate::persist::sql_escape_string(persist_id)
        );
        let query = session.query(&sql).await?;
        let Some(row) = query.rows().first() else {
            return Ok(None);
        };

        let state = self.state_from_storage_row(&table_name, query.columns(), row)?;
        let mut item = V::Item::from_state(&state)?;
        item.metadata_mut().persisted = true;
        Ok(Some(item))
    }

    /// Refreshes one cached item from storage.
    ///
    /// Returns `true` if row exists in storage and cache was refreshed.
    #[allow(dead_code)]
    async fn refresh_cached_item_from_db(&mut self, persist_id: &str) -> Result<bool>
    where
        V::Item: PersistEntityFactory,
    {
        let session = self.session.clone();
        self.refresh_cached_item_from_db_with_session(&session, persist_id)
            .await
    }

    /// Session-aware cache refresh for one entity id.
    async fn refresh_cached_item_from_db_with_session(
        &mut self,
        session: &PersistSession,
        persist_id: &str,
    ) -> Result<bool>
    where
        V::Item: PersistEntityFactory,
    {
        let Some(item) = self
            .load_item_from_db_by_id_with_session(session, persist_id)
            .await?
        else {
            self.evict_cached_item(persist_id);
            return Ok(false);
        };
        self.upsert_cached_item(item);
        Ok(true)
    }

    /// Upserts one hydrated entity into in-memory collection cache.
    ///
    /// This keeps in-memory reads fast while storage remains the source of truth.
    fn upsert_cached_item(&mut self, item: V::Item) {
        let persist_id = item.persist_id().to_string();
        if let Some(index) = self.persisted_item_index(&persist_id) {
            if let Some(slot) = self.collection.items_mut().get_mut(index) {
                *slot = item;
            } else {
                self.collection.add_one(item);
            }
        } else {
            self.collection.add_one(item);
        }
        self.mark_persisted_index_dirty();
    }

    /// Removes stale cached entity when storage says row does not exist.
    fn evict_cached_item(&mut self, persist_id: &str) {
        if let Some(index) = self.persisted_item_index(persist_id) {
            let _ = self.collection.remove_by_index(index);
            self.mark_persisted_index_dirty();
        }
    }

    fn lookup_table_name_for_db_hydration(&self) -> String
    where
        V::Item: PersistEntityFactory,
    {
        self.collection
            .items()
            .first()
            .map(|item| item.table_name().to_string())
            .unwrap_or_else(V::Item::default_table_name)
    }

    fn state_from_storage_row(
        &self,
        table_name: &str,
        columns: &[crate::core::Column],
        row: &crate::core::Row,
    ) -> Result<crate::persist::PersistState>
    where
        V::Item: PersistEntityFactory,
    {
        let mut persist_id = None::<String>;
        let mut version = None::<i64>;
        let mut schema_version = None::<u32>;
        let mut touch_count = None::<u64>;
        let mut created_at = None::<chrono::DateTime<chrono::Utc>>;
        let mut updated_at = None::<chrono::DateTime<chrono::Utc>>;
        let mut last_touch_at = None::<chrono::DateTime<chrono::Utc>>;
        let mut fields = serde_json::Map::<String, serde_json::Value>::new();

        for (column, value) in columns.iter().zip(row.iter()) {
            // Some storage/query paths can return qualified names (`table.__persist_id`).
            // Normalize to raw field name so hydration works across engines.
            let normalized_name = column
                .name
                .rsplit('.')
                .next()
                .unwrap_or(column.name.as_str());
            match normalized_name {
                "__persist_id" => {
                    persist_id = Some(self.persist_id_from_value(value)?);
                }
                "__version" => {
                    version = Some(self.i64_from_value(value, "__version")?);
                }
                "__schema_version" => {
                    let raw = self.i64_from_value(value, "__schema_version")?;
                    schema_version = Some(u32::try_from(raw).map_err(|_| {
                        DbError::ExecutionError(format!(
                            "invalid __schema_version '{}': negative or overflow",
                            raw
                        ))
                    })?);
                }
                "__touch_count" => {
                    let raw = self.i64_from_value(value, "__touch_count")?;
                    touch_count = Some(u64::try_from(raw).map_err(|_| {
                        DbError::ExecutionError(format!(
                            "invalid __touch_count '{}': negative or overflow",
                            raw
                        ))
                    })?);
                }
                "__created_at" => {
                    created_at = Some(self.timestamp_from_value(value, "__created_at")?);
                }
                "__updated_at" => {
                    updated_at = Some(self.timestamp_from_value(value, "__updated_at")?);
                }
                "__last_touch_at" => {
                    last_touch_at = Some(self.timestamp_from_value(value, "__last_touch_at")?);
                }
                "__schema_version_legacy" => {}
                other => {
                    fields.insert(other.to_string(), self.value_to_json(value));
                }
            }
        }

        let persist_id = persist_id.ok_or_else(|| {
            DbError::ExecutionError(format!(
                "storage row missing __persist_id for table '{}'",
                table_name
            ))
        })?;

        let metadata = crate::persist::PersistMetadata {
            version: version.unwrap_or(1),
            schema_version: schema_version.unwrap_or_else(crate::persist::default_schema_version),
            created_at: created_at.unwrap_or_else(chrono::Utc::now),
            updated_at: updated_at.unwrap_or_else(chrono::Utc::now),
            last_touch_at: last_touch_at.unwrap_or_else(chrono::Utc::now),
            touch_count: touch_count.unwrap_or(0),
            persisted: true,
        };

        Ok(crate::persist::PersistState {
            persist_id,
            type_name: V::Item::entity_type_name().to_string(),
            table_name: table_name.to_string(),
            metadata,
            fields: serde_json::Value::Object(fields),
        })
    }

    fn i64_from_value(&self, value: &crate::core::Value, column: &str) -> Result<i64> {
        value.as_i64().ok_or_else(|| {
            DbError::ExecutionError(format!(
                "expected integer value for '{}', got {}",
                column,
                value.type_name()
            ))
        })
    }

    fn persist_id_from_value(&self, value: &crate::core::Value) -> Result<String> {
        match value {
            crate::core::Value::Text(text) => Ok(text.clone()),
            crate::core::Value::Uuid(uuid) => Ok(uuid.to_string()),
            other => Err(DbError::ExecutionError(format!(
                "expected __persist_id as TEXT/UUID, got {}",
                other.type_name()
            ))),
        }
    }

    fn timestamp_from_value(
        &self,
        value: &crate::core::Value,
        column: &str,
    ) -> Result<chrono::DateTime<chrono::Utc>> {
        match value {
            crate::core::Value::Timestamp(ts) => Ok(*ts),
            crate::core::Value::Text(raw) => chrono::DateTime::parse_from_rfc3339(raw)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .map_err(|err| {
                    DbError::ExecutionError(format!(
                        "failed to parse '{}' timestamp '{}': {}",
                        column, raw, err
                    ))
                }),
            other => Err(DbError::ExecutionError(format!(
                "expected '{}' as TIMESTAMP/TEXT, got {}",
                column,
                other.type_name()
            ))),
        }
    }

    fn value_to_json(&self, value: &crate::core::Value) -> serde_json::Value {
        match value {
            crate::core::Value::Null => serde_json::Value::Null,
            crate::core::Value::Integer(v) => serde_json::json!(v),
            crate::core::Value::Float(v) => serde_json::json!(v),
            crate::core::Value::Text(v) => self.text_value_to_json(v),
            crate::core::Value::Boolean(v) => serde_json::json!(v),
            crate::core::Value::Timestamp(v) => serde_json::json!(v.to_rfc3339()),
            crate::core::Value::Date(v) => serde_json::json!(v.format("%Y-%m-%d").to_string()),
            crate::core::Value::Uuid(v) => serde_json::json!(v.to_string()),
            crate::core::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(|entry| self.value_to_json(entry)).collect())
            }
            crate::core::Value::Json(v) => v.clone(),
        }
    }

    fn text_value_to_json(&self, raw: &str) -> serde_json::Value {
        let trimmed = raw.trim();
        let looks_like_json_container = (trimmed.starts_with('{') && trimmed.ends_with('}'))
            || (trimmed.starts_with('[') && trimmed.ends_with(']'));
        if looks_like_json_container && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) {
            return parsed;
        }
        serde_json::json!(raw)
    }

    /// Validates unique constraints for all items in the collection.
    ///
    /// Checks that fields marked as unique in the schema do not have duplicate values.
    /// Uses serialized values to handle complex types uniformly.
    fn validate_unique_constraints(&self) -> Result<()> {
        // Validate against serialized entity state so the check works for any model generated by
        // persist_struct! without requiring repository-layer duplicate lookup code.
        let mut seen = std::collections::HashMap::<(&'static str, String), String>::new();

        for item in self.collection.items() {
            let unique_fields = item.unique_fields();
            if unique_fields.is_empty() {
                continue;
            }

            let state = item.state();
            let fields = state.fields.as_object().ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Persist state fields must be a JSON object for unique validation: {}:{}",
                    item.table_name(),
                    item.persist_id()
                ))
            })?;

            for field in unique_fields {
                let value = fields.get(field).ok_or_else(|| {
                    DbError::ExecutionError(format!(
                        "Unique field '{}' is missing in persist state for {}:{}",
                        field,
                        item.table_name(),
                        item.persist_id()
                    ))
                })?;

                let normalized_value = serde_json::to_string(value).map_err(|err| {
                    DbError::ExecutionError(format!(
                        "Failed to serialize unique field '{}' for {}:{}: {}",
                        field,
                        item.table_name(),
                        item.persist_id(),
                        err
                    ))
                })?;

                let key = (field, normalized_value.clone());
                if let Some(existing_persist_id) = seen.get(&key) {
                    if existing_persist_id != item.persist_id() {
                        return Err(DbError::ConstraintViolation(format!(
                            "unique constraint violation on '{}.{}': value {} already exists (persist_id='{}')",
                            self.name, field, normalized_value, existing_persist_id
                        )));
                    }
                } else {
                    seen.insert(key, item.persist_id().to_string());
                }
            }
        }

        Ok(())
    }

    /// Internal helper that validates constraints before delegates to the collection's save.
    async fn save_all_checked(&mut self, session: &PersistSession) -> Result<()> {
        self.validate_unique_constraints()?;
        self.collection.save_all(session).await
    }

    /// Returns a slice of all items in the collection.
    pub fn list(&self) -> &[V::Item] {
        self.collection.items()
    }

    /// Retrieves a single item by id from in-memory cache.
    ///
    /// This method does not perform storage I/O. Use `get_one_db` for DB-first reads.
    pub fn get_cached(&self, persist_id: &str) -> Option<&V::Item> {
        let index = self.persisted_item_index(persist_id)?;
        self.collection.items().get(index)
    }

    /// Legacy alias for cache-only lookup.
    ///
    /// Prefer `get_cached` to make cache semantics explicit.
    pub fn get(&self, persist_id: &str) -> Option<&V::Item> {
        self.get_cached(persist_id)
    }

    /// Retrieves one entity by id using storage as the source of truth.
    ///
    /// On hit, cache is refreshed (insert/update). On miss, stale cache entry is evicted.
    pub async fn get_one_db(&mut self, persist_id: &str) -> Result<Option<V::Item>>
    where
        V::Item: Clone + PersistEntityFactory,
    {
        let session = self.session.clone();
        self.get_one_db_with_session(&session, persist_id).await
    }

    /// Session-aware DB-first entity lookup.
    ///
    /// This method is the authoritative read path for callers operating inside
    /// explicit transaction scopes.
    pub(crate) async fn get_one_db_with_session(
        &mut self,
        session: &PersistSession,
        persist_id: &str,
    ) -> Result<Option<V::Item>>
    where
        V::Item: Clone + PersistEntityFactory,
    {
        let Some(item) = self
            .load_item_from_db_by_id_with_session(session, persist_id)
            .await?
        else {
            self.evict_cached_item(persist_id);
            return Ok(None);
        };
        self.upsert_cached_item(item.clone());
        Ok(Some(item))
    }

    /// Returns current persisted version for one entity using DB-first lookup.
    ///
    /// This avoids relying on stale in-memory metadata during optimistic prechecks.
    pub async fn get_version_db(&mut self, persist_id: &str) -> Result<Option<i64>>
    where
        V::Item: PersistEntityFactory,
    {
        let table_name = self.lookup_table_name_for_db_hydration();
        let sql = format!(
            "SELECT __version FROM {} WHERE __persist_id = '{}' LIMIT 1",
            table_name,
            crate::persist::sql_escape_string(persist_id)
        );
        let query = self.session.query(&sql).await?;
        let Some(row) = query.rows().first() else {
            self.evict_cached_item(persist_id);
            return Ok(None);
        };
        let Some(version_value) = row.first() else {
            return Err(DbError::ExecutionError(format!(
                "version lookup for '{}:{}' returned no __version column value",
                self.name, persist_id
            )));
        };

        let version = self.i64_from_value(version_value, "__version")?;

        // Keep in-memory metadata monotonic with storage reads when item is cached.
        if let Some(index) = self.persisted_item_index(persist_id)
            && let Some(item) = self.collection.items_mut().get_mut(index)
        {
            item.metadata_mut().version = version;
            item.metadata_mut().persisted = true;
        }

        Ok(Some(version))
    }

    /// Returns a paginated list of items.
    ///
    /// Skips `offset` items and returns at most `limit` items.
    /// Only returns items that are persisted (not deleted).
    pub fn list_page(&self, offset: usize, limit: usize) -> Vec<&V::Item> {
        if limit == 0 {
            return Vec::new();
        }

        self.collection
            .items()
            .iter()
            .filter(|item| item.metadata().persisted)
            .skip(offset)
            .take(limit)
            .collect()
    }

    /// Returns items filtering by an arbitrary predicate.
    ///
    /// Only items that pass both the predicate and are persisted are returned.
    pub fn list_filtered<F>(&self, predicate: F) -> Vec<&V::Item>
    where
        F: Fn(&V::Item) -> bool,
    {
        self.collection
            .items()
            .iter()
            .filter(|item| item.metadata().persisted)
            .filter(|item| predicate(item))
            .collect()
    }

    /// Returns a new vector of items sorted by the provided comparison function.
    ///
    /// Does not modify the underlying storage order.
    pub fn list_sorted_by<F>(&self, mut compare: F) -> Vec<&V::Item>
    where
        F: FnMut(&V::Item, &V::Item) -> Ordering,
    {
        let mut items = self
            .collection
            .items()
            .iter()
            .filter(|item| item.metadata().persisted)
            .collect::<Vec<_>>();
        items.sort_by(|left, right| compare(left, right));
        items
    }
}
