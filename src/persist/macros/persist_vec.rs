#[macro_export]
macro_rules! persist_vec {
    (hetero $vis:vis $name:ident) => {
        $vis struct $name {
            inner: $crate::persist::HeteroPersistVec,
        }

        impl $name {
            /// Creates an empty heterogeneous collection wrapper.
            pub fn new(name: impl Into<String>) -> Self {
                Self {
                    inner: $crate::persist::HeteroPersistVec::new(name),
                }
            }

            /// Returns the number of currently registered entities.
            pub fn len(&self) -> usize {
                self.inner.len()
            }

            /// Returns `true` when the collection has no entities.
            pub fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            /// Lists registered entity type names for this collection.
            pub fn registered_types(&self) -> Vec<String> {
                self.inner.registered_types()
            }

            /// Registers an entity type with its default migration metadata.
            pub fn register_type<T>(&mut self)
            where
                T: $crate::persist::PersistEntityFactory + 'static,
            {
                self.inner.register_type::<T>();
            }

            /// Registers an entity type with an explicit migration plan.
            pub fn register_type_with_migration_plan<T>(
                &mut self,
                migration_plan: $crate::persist::PersistMigrationPlan,
            )
            where
                T: $crate::persist::PersistEntityFactory + 'static,
            {
                self.inner
                    .register_type_with_migration_plan::<T>(migration_plan);
            }

            /// Adds one typed entity to the heterogeneous collection.
            pub fn add_one<T>(&mut self, item: T) -> $crate::core::Result<()>
            where
                T: $crate::persist::PersistEntity + 'static,
            {
                self.inner.add_one(item)
            }

            /// Adds many typed entities to the heterogeneous collection.
            pub fn add_many<T, I>(&mut self, items: I) -> $crate::core::Result<()>
            where
                T: $crate::persist::PersistEntity + 'static,
                I: IntoIterator<Item = T>,
            {
                self.inner.add_many(items)
            }

            /// Adds one pre-boxed entity.
            pub fn add_boxed(
                &mut self,
                item: Box<dyn $crate::persist::PersistEntity>,
            ) -> $crate::core::Result<()> {
                self.inner.add_boxed(item)
            }

            /// Adds many pre-boxed entities.
            pub fn add_many_boxed<I>(&mut self, items: I) -> $crate::core::Result<()>
            where
                I: IntoIterator<Item = Box<dyn $crate::persist::PersistEntity>>,
            {
                self.inner.add_many_boxed(items)
            }

            /// Returns entity states for snapshot/replay tooling.
            pub fn states(&self) -> Vec<$crate::persist::PersistState> {
                self.inner.states()
            }

            /// Returns descriptors for all entities in the collection.
            pub fn descriptors(&self) -> Vec<$crate::persist::ObjectDescriptor> {
                self.inner.descriptors()
            }

            /// Returns aggregated callable-function usage statistics.
            pub fn functions_catalog(&self) -> std::collections::HashMap<String, usize> {
                self.inner.functions_catalog()
            }

            /// Ensures backing tables exist for all currently stored entities.
            pub async fn ensure_all_tables(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.inner.ensure_all_tables(session).await
            }

            /// Persists all dirty entities through the provided session.
            pub async fn save_all(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.inner.save_all(session).await
            }

            /// Deletes entities older than `max_age` according to metadata timestamps.
            pub async fn prune_stale(
                &mut self,
                max_age: chrono::Duration,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<usize> {
                self.inner.prune_stale(max_age, session).await
            }

            /// Invokes a dynamic function on all entities that support it.
            pub async fn invoke_supported(
                &mut self,
                function: &str,
                args: Vec<$crate::core::Value>,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<Vec<$crate::persist::InvokeOutcome>> {
                self.inner.invoke_supported(function, args, session).await
            }

            /// Builds a heterogeneous snapshot in the selected mode.
            pub fn snapshot(
                &self,
                mode: $crate::persist::SnapshotMode,
            ) -> $crate::persist::HeteroPersistVecSnapshot {
                self.inner.snapshot(mode)
            }

            /// Restores collection state from a snapshot using default conflict policy.
            pub async fn restore(
                &mut self,
                snapshot: $crate::persist::HeteroPersistVecSnapshot,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.inner.restore(snapshot, session).await
            }

            /// Restores collection state from a snapshot with explicit conflict policy.
            pub async fn restore_with_policy(
                &mut self,
                snapshot: $crate::persist::HeteroPersistVecSnapshot,
                session: &$crate::persist::PersistSession,
                conflict_policy: $crate::persist::RestoreConflictPolicy,
            ) -> $crate::core::Result<()> {
                self.inner
                    .restore_with_policy(snapshot, session, conflict_policy)
                    .await
            }
        }

        impl $crate::persist::app::PersistCollection for $name {
            type Snapshot = $crate::persist::HeteroPersistVecSnapshot;

            fn new_collection(name: impl Into<String>) -> Self {
                Self::new(name)
            }

            fn len(&self) -> usize {
                self.inner.len()
            }

            fn snapshot(&self, mode: $crate::persist::SnapshotMode) -> Self::Snapshot {
                self.inner.snapshot(mode)
            }

            fn save_all<'a>(
                &'a mut self,
                session: &'a $crate::persist::PersistSession,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = $crate::core::Result<()>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(async move { self.inner.save_all(session).await })
            }

            fn restore_with_policy<'a>(
                &'a mut self,
                snapshot: Self::Snapshot,
                session: &'a $crate::persist::PersistSession,
                conflict_policy: $crate::persist::RestoreConflictPolicy,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = $crate::core::Result<()>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(async move {
                    self.inner
                        .restore_with_policy(snapshot, session, conflict_policy)
                        .await
                })
            }
        }
    };
    ($vis:vis $name:ident, $item_ty:ty) => {
        $vis struct $name {
            inner: $crate::persist::PersistVec<$item_ty>,
        }

        impl $name {
            /// Creates an empty typed persistent collection wrapper.
            pub fn new(name: impl Into<String>) -> Self {
                Self {
                    inner: $crate::persist::PersistVec::new(name),
                }
            }

            /// Returns the number of entities in the collection.
            pub fn len(&self) -> usize {
                self.inner.len()
            }

            /// Returns `true` if no entities are currently present.
            pub fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            /// Adds one entity to the collection.
            pub fn add_one(&mut self, item: $item_ty) {
                self.inner.add_one(item);
            }

            /// Adds many entities to the collection.
            pub fn add_many<I>(&mut self, items: I)
            where
                I: IntoIterator<Item = $item_ty>,
            {
                self.inner.add_many(items);
            }

            /// Removes an entity by its persist id.
            pub fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<$item_ty> {
                self.inner.remove_by_persist_id(persist_id)
            }

            /// Returns entity states for diagnostics and snapshot tooling.
            pub fn states(&self) -> Vec<$crate::persist::PersistState> {
                self.inner.states()
            }

            /// Returns descriptors for entities currently in memory.
            pub fn descriptors(&self) -> Vec<$crate::persist::ObjectDescriptor> {
                self.inner.descriptors()
            }

            /// Returns a histogram of supported dynamic function names.
            pub fn functions_catalog(&self) -> std::collections::HashMap<String, usize> {
                self.inner.functions_catalog()
            }

            /// Returns a read-only view of entities.
            pub fn items(&self) -> &[$item_ty] {
                self.inner.items()
            }

            /// Returns a mutable view of entities.
            pub fn items_mut(&mut self) -> &mut [$item_ty] {
                self.inner.items_mut()
            }

            /// Ensures backing table DDL exists for the item type.
            pub async fn ensure_all_tables(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.inner.ensure_all_tables(session).await
            }

            /// Persists all dirty entities through the provided session.
            pub async fn save_all(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.inner.save_all(session).await
            }

            /// Deletes stale entities older than `max_age`.
            pub async fn prune_stale(
                &mut self,
                max_age: chrono::Duration,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<usize> {
                self.inner.prune_stale(max_age, session).await
            }

            /// Invokes a dynamic function on entities that expose it.
            pub async fn invoke_supported(
                &mut self,
                function: &str,
                args: Vec<$crate::core::Value>,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<Vec<$crate::persist::InvokeOutcome>> {
                self.inner.invoke_supported(function, args, session).await
            }

            /// Builds a snapshot for the current collection state.
            pub fn snapshot(
                &self,
                mode: $crate::persist::SnapshotMode,
            ) -> $crate::persist::PersistVecSnapshot {
                self.inner.snapshot(mode)
            }

            /// Restores collection data from a snapshot using default migration behavior.
            pub async fn restore(
                &mut self,
                snapshot: $crate::persist::PersistVecSnapshot,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.inner.restore(snapshot, session).await
            }

            /// Restores collection data with explicit restore conflict policy.
            pub async fn restore_with_policy(
                &mut self,
                snapshot: $crate::persist::PersistVecSnapshot,
                session: &$crate::persist::PersistSession,
                conflict_policy: $crate::persist::RestoreConflictPolicy,
            ) -> $crate::core::Result<()> {
                self.inner
                    .restore_with_policy(snapshot, session, conflict_policy)
                    .await
            }

            /// Restores collection data with an explicit migration plan override.
            pub async fn restore_with_custom_migration_plan(
                &mut self,
                snapshot: $crate::persist::PersistVecSnapshot,
                session: &$crate::persist::PersistSession,
                conflict_policy: $crate::persist::RestoreConflictPolicy,
                migration_plan: $crate::persist::PersistMigrationPlan,
            ) -> $crate::core::Result<()> {
                self.inner
                    .restore_with_custom_migration_plan(
                        snapshot,
                        session,
                        conflict_policy,
                        migration_plan,
                    )
                    .await
            }
        }

        impl $crate::persist::app::PersistCollection for $name {
            type Snapshot = $crate::persist::PersistVecSnapshot;

            fn new_collection(name: impl Into<String>) -> Self {
                Self::new(name)
            }

            fn len(&self) -> usize {
                self.inner.len()
            }

            fn snapshot(&self, mode: $crate::persist::SnapshotMode) -> Self::Snapshot {
                self.inner.snapshot(mode)
            }

            fn save_all<'a>(
                &'a mut self,
                session: &'a $crate::persist::PersistSession,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = $crate::core::Result<()>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(async move { self.inner.save_all(session).await })
            }

            fn restore_with_policy<'a>(
                &'a mut self,
                snapshot: Self::Snapshot,
                session: &'a $crate::persist::PersistSession,
                conflict_policy: $crate::persist::RestoreConflictPolicy,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = $crate::core::Result<()>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(async move {
                    self.inner
                        .restore_with_policy(snapshot, session, conflict_policy)
                        .await
                })
            }
        }

        impl $crate::persist::app::PersistIndexedCollection for $name {
            type Item = $item_ty;

            fn items(&self) -> &[Self::Item] {
                self.inner.items()
            }

            fn items_mut(&mut self) -> &mut [Self::Item] {
                self.inner.items_mut()
            }

            fn add_one(&mut self, item: Self::Item) {
                self.inner.add_one(item);
            }

            fn add_many(&mut self, items: Vec<Self::Item>) {
                self.inner.add_many(items);
            }

            fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<Self::Item> {
                self.inner.remove_by_persist_id(persist_id)
            }
        }
    };
}
