#[macro_export]
macro_rules! persist_struct {
    ($vis:vis struct $name:ident from_struct = $source:ty $(,)?) => {
        $vis type $name = <$source as $crate::persist::PersistModelExt>::Persisted;
        $crate::paste::paste! {
            $vis type [<$name Draft>] = <<$source as $crate::persist::PersistModelExt>::Persisted as $crate::persist::PersistCommandModel>::Draft;
            $vis type [<$name Patch>] = <<$source as $crate::persist::PersistModelExt>::Persisted as $crate::persist::PersistCommandModel>::Patch;
            $vis type [<$name Command>] = <<$source as $crate::persist::PersistModelExt>::Persisted as $crate::persist::PersistCommandModel>::Command;
        }
    };
    ($vis:vis struct $name:ident from_ddl = $ddl:expr $(,)?) => {
        $crate::persist_struct!(
            @impl_dynamic
            $vis
            $name
            $crate::persist::default_table_name(stringify!($name), line!(), column!());
            ddl;
            $ddl
        );
    };
    ($vis:vis struct $name:ident table = $table_name:expr, from_ddl = $ddl:expr $(,)?) => {
        $crate::persist_struct!(@impl_dynamic $vis $name $table_name; ddl; $ddl);
    };
    ($vis:vis struct $name:ident from_json_schema = $json_schema:expr $(,)?) => {
        $crate::persist_struct!(
            @impl_dynamic
            $vis
            $name
            $crate::persist::default_table_name(stringify!($name), line!(), column!());
            json_schema;
            $json_schema
        );
    };
    ($vis:vis struct $name:ident table = $table_name:expr, from_json_schema = $json_schema:expr $(,)?) => {
        $crate::persist_struct!(@impl_dynamic $vis $name $table_name; json_schema; $json_schema);
    };
    ($vis:vis struct $name:ident table = $table_name:tt { $($(#[$($field_meta:tt)*])* $field:ident : $field_ty:ty),+ $(,)? }) => {
        $crate::persist_struct!(@impl $vis $name $table_name; $($(#[$($field_meta)*])* $field : $field_ty),+);
    };
    ($vis:vis struct $name:ident { $($(#[$($field_meta:tt)*])* $field:ident : $field_ty:ty),+ $(,)? }) => {
        $crate::persist_struct!(
            @impl
            $vis
            $name
            $crate::persist::default_table_name(stringify!($name), line!(), column!());
            $($(#[$($field_meta)*])* $field : $field_ty),+
        );
    };
    (@impl $vis:vis $name:ident $table_expr:expr; $($(#[$($field_meta:tt)*])* $field:ident : $field_ty:ty),+) => {
        $crate::paste::paste! {
            $vis struct [<$name Draft>] {
                $( pub $field: $field_ty, )+
            }

            impl [<$name Draft>] {
                /// Builds a draft payload with all required fields.
                pub fn new($($field: $field_ty),+) -> Self {
                    Self {
                        $( $field, )+
                    }
                }
            }

            $vis struct [<$name Patch>] {
                $( pub $field: Option<$field_ty>, )+
            }

            impl Default for [<$name Patch>] {
                fn default() -> Self {
                    Self {
                        $( $field: None, )+
                    }
                }
            }

            impl [<$name Patch>] {
                /// Returns `true` when patch carries no field updates.
                pub fn is_empty(&self) -> bool {
                    true $(
                        && self.$field.is_none()
                    )+
                }

                /// Validates that patch contains at least one changed field.
                pub fn validate(&self) -> $crate::core::Result<()> {
                    if self.is_empty() {
                        return Err($crate::core::DbError::ExecutionError(
                            "Patch payload must include at least one field".to_string(),
                        ));
                    }
                    Ok(())
                }
            }

            $vis enum [<$name Command>] {
                $(
                    [<Set $field:camel>]($field_ty),
                )+
                Touch,
            }

            impl [<$name Command>] {
                /// Returns stable command name for telemetry/audit use.
                pub fn name(&self) -> &'static str {
                    match self {
                        $(
                            Self::[<Set $field:camel>](_) => stringify!([<Set $field:camel>]),
                        )+
                        Self::Touch => "Touch",
                    }
                }
            }

            impl $crate::persist::PersistCommandName for [<$name Command>] {
                fn command_name(&self) -> &'static str {
                    self.name()
                }
            }
        }

        #[derive(Clone)]
        $vis struct $name {
            $( $field: $field_ty, )+
            __persist_id: String,
            __table_name: String,
            __metadata: $crate::persist::PersistMetadata,
            __dirty_fields: std::collections::HashSet<&'static str>,
            __table_ready: bool,
            __bound_session: Option<$crate::persist::PersistSession>,
            __auto_persist: bool,
            __functions: std::collections::HashMap<
                String,
                std::sync::Arc<
                    dyn Fn(
                            &mut Self,
                            Vec<$crate::core::Value>
                        ) -> $crate::core::Result<$crate::core::Value>
                        + Send
                        + Sync
                >,
            >,
        }

        impl $name {
            fn __type_checks()
            where
                $( $field_ty: $crate::persist::PersistValue, )+
            {}

            /// Returns default table name generated for this model.
            pub fn default_table_name() -> String {
                ($table_expr).to_string()
            }

            /// Returns CREATE TABLE SQL for this model on a provided table name.
            pub fn create_table_sql_for(table_name: &str) -> String {
                Self::__type_checks();
                let mut columns = vec![
                    "__persist_id TEXT PRIMARY KEY".to_string(),
                    "__version INTEGER NOT NULL".to_string(),
                    "__schema_version INTEGER NOT NULL".to_string(),
                    "__touch_count INTEGER NOT NULL".to_string(),
                    "__created_at TIMESTAMP NOT NULL".to_string(),
                    "__updated_at TIMESTAMP NOT NULL".to_string(),
                    "__last_touch_at TIMESTAMP NOT NULL".to_string(),
                ];
                $(
                    columns.push(format!(
                        "{} {}",
                        stringify!($field),
                        <$field_ty as $crate::persist::PersistValue>::sql_type()
                    ));
                )+

                format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns.join(", "))
            }

            /// Returns index DDL statements for fields marked `#[persist(index|unique)]`.
            pub fn create_index_sql_for(table_name: &str) -> Vec<String> {
                let mut statements = Vec::new();

                $(
                    let is_unique = $crate::__persist_field_is_unique!($(#[$($field_meta)*])*);
                    let is_indexed = $crate::__persist_field_is_indexed!($(#[$($field_meta)*])*) || is_unique;
                    if is_indexed {
                        let index_name = $crate::persist::default_index_name(
                            table_name,
                            stringify!($field),
                        );
                        if is_unique {
                            statements.push(format!(
                                "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})",
                                index_name,
                                table_name,
                                stringify!($field)
                            ));
                        } else {
                            statements.push(format!(
                                "CREATE INDEX IF NOT EXISTS {} ON {} ({})",
                                index_name,
                                table_name,
                                stringify!($field)
                            ));
                        }
                    }
                )+

                statements
            }

            /// Creates a new entity instance with generated persistence metadata.
            pub fn new($($field: $field_ty),+) -> Self {
                Self::__type_checks();
                let now = chrono::Utc::now();
                Self {
                    $( $field, )+
                    __persist_id: $crate::persist::new_persist_id(),
                    __table_name: Self::default_table_name(),
                    __metadata: $crate::persist::PersistMetadata::new(now),
                    __dirty_fields: std::collections::HashSet::new(),
                    __table_ready: false,
                    __bound_session: None,
                    __auto_persist: false,
                    __functions: std::collections::HashMap::new(),
                }
            }

            /// Creates a new entity instance using a custom table name.
            pub fn with_table_name(table_name: impl Into<String>, $($field: $field_ty),+) -> Self {
                let mut this = Self::new($($field),+);
                this.__table_name = table_name.into();
                this
            }

            /// Returns entity persistence identifier.
            pub fn persist_id(&self) -> &str {
                &self.__persist_id
            }

            /// Returns physical table name used for this entity.
            pub fn table_name(&self) -> &str {
                &self.__table_name
            }

            /// Returns persistence metadata (version/timestamps/state flags).
            pub fn metadata(&self) -> &$crate::persist::PersistMetadata {
                &self.__metadata
            }

            /// Binds a session so helper persisted methods can execute without explicit session argument.
            pub fn bind_session(&mut self, session: $crate::persist::PersistSession) {
                self.__bound_session = Some(session);
            }

            /// Unbinds current session and disables auto-persist mode.
            pub fn unbind_session(&mut self) {
                self.__bound_session = None;
                self.__auto_persist = false;
            }

            /// Returns `true` when a session is currently bound.
            pub fn has_bound_session(&self) -> bool {
                self.__bound_session.is_some()
            }

            /// Returns current auto-persist flag.
            pub fn auto_persist_enabled(&self) -> bool {
                self.__auto_persist
            }

            /// Enables or disables auto-persist for bound-session workflows.
            pub fn set_auto_persist(&mut self, enabled: bool) -> $crate::core::Result<()> {
                if enabled && self.__bound_session.is_none() {
                    return Err($crate::core::DbError::ExecutionError(
                        "Auto-persist requires a bound PersistSession".to_string()
                    ));
                }

                self.__auto_persist = enabled;
                Ok(())
            }

            /// Saves entity through bound session.
            pub async fn save_bound(&mut self) -> $crate::core::Result<()> {
                let session = self.__bound_session.clone().ok_or_else(|| {
                    $crate::core::DbError::ExecutionError(
                        "No bound PersistSession for save_bound".to_string()
                    )
                })?;
                <Self as $crate::persist::PersistEntity>::save(self, &session).await
            }

            /// Deletes entity through bound session.
            pub async fn delete_bound(&mut self) -> $crate::core::Result<()> {
                let session = self.__bound_session.clone().ok_or_else(|| {
                    $crate::core::DbError::ExecutionError(
                        "No bound PersistSession for delete_bound".to_string()
                    )
                })?;
                <Self as $crate::persist::PersistEntity>::delete(self, &session).await
            }

            async fn __auto_persist_if_enabled(&mut self) -> $crate::core::Result<()> {
                if !self.__auto_persist || self.__dirty_fields.is_empty() {
                    return Ok(());
                }

                let session = self.__bound_session.clone().ok_or_else(|| {
                    $crate::core::DbError::ExecutionError(
                        "Auto-persist is enabled but no PersistSession is bound".to_string()
                    )
                })?;
                <Self as $crate::persist::PersistEntity>::save(self, &session).await
            }

            /// Applies a local mutation and, when enabled, flushes one auto-persist save.
            pub async fn mutate_persisted<F>(&mut self, mutator: F) -> $crate::core::Result<()>
            where
                F: FnOnce(&mut Self),
            {
                mutator(self);
                self.__auto_persist_if_enabled().await
            }

            $crate::paste::paste! {
                /// Builds entity from command-first draft payload.
                pub fn from_draft(draft: [<$name Draft>]) -> Self {
                    Self::new($(draft.$field),+)
                }

                /// Applies patch payload and returns whether state changed.
                pub fn patch(&mut self, patch: [<$name Patch>]) -> $crate::core::Result<bool> {
                    patch.validate()?;
                    let mut changed = false;

                    $(
                        if let Some(value) = patch.$field {
                            if self.$field != value {
                                self.$field = value;
                                self.__mark_dirty(stringify!($field));
                                changed = true;
                            }
                        }
                    )+

                    Ok(changed)
                }

                /// Applies command payload and returns whether state changed.
                pub fn apply(&mut self, command: [<$name Command>]) -> $crate::core::Result<bool> {
                    match command {
                        $(
                            [<$name Command>]::[<Set $field:camel>](value) => {
                                let changed = self.$field != value;
                                if changed {
                                    self.$field = value;
                                    self.__mark_dirty(stringify!($field));
                                }
                                Ok(changed)
                            }
                        )+
                        [<$name Command>]::Touch => {
                            self.touch();
                            Ok(true)
                        }
                    }
                }

                /// Applies patch and flushes auto-persist if configured.
                pub async fn patch_persisted(
                    &mut self,
                    patch: [<$name Patch>],
                ) -> $crate::core::Result<bool> {
                    let changed = self.patch(patch)?;
                    self.__auto_persist_if_enabled().await?;
                    Ok(changed)
                }

                /// Applies command and flushes auto-persist if configured.
                pub async fn apply_persisted(
                    &mut self,
                    command: [<$name Command>],
                ) -> $crate::core::Result<bool> {
                    let changed = self.apply(command)?;
                    self.__auto_persist_if_enabled().await?;
                    Ok(changed)
                }
            }

            /// Increments touch metadata counters/timestamps.
            pub fn touch(&mut self) {
                self.__metadata.touch_count = self.__metadata.touch_count.saturating_add(1);
                self.__metadata.last_touch_at = chrono::Utc::now();
            }

            fn __mark_dirty(&mut self, field: &'static str) {
                self.__dirty_fields.insert(field);
                self.touch();
            }

            /// Registers a named dynamic function handler on this entity instance.
            pub fn register_function<F>(&mut self, name: impl Into<String>, handler: F)
            where
                F: Fn(
                        &mut Self,
                        Vec<$crate::core::Value>
                    ) -> $crate::core::Result<$crate::core::Value>
                    + Send
                    + Sync
                    + 'static,
            {
                self.__functions.insert(name.into(), std::sync::Arc::new(handler));
            }

            /// Returns entity business state as JSON object.
            pub fn state_json(&self) -> serde_json::Value {
                serde_json::json!({
                    $( stringify!($field): &self.$field, )+
                })
            }

            /// Returns descriptor compatible with `PersistEntity` reflection API.
            pub fn descriptor(&self) -> $crate::persist::ObjectDescriptor {
                <Self as $crate::persist::PersistEntity>::descriptor(self)
            }

            /// Returns available dynamic function descriptors.
            pub fn available_functions(&self) -> Vec<$crate::persist::FunctionDescriptor> {
                <Self as $crate::persist::PersistEntity>::available_functions(self)
            }

            fn __create_table_sql(&self) -> String {
                Self::create_table_sql_for(&self.__table_name)
            }

            fn __create_index_sqls(&self) -> Vec<String> {
                Self::create_index_sql_for(&self.__table_name)
            }

            fn __insert_sql(&self) -> String {
                let mut columns = vec![
                    "__persist_id".to_string(),
                    "__version".to_string(),
                    "__schema_version".to_string(),
                    "__touch_count".to_string(),
                    "__created_at".to_string(),
                    "__updated_at".to_string(),
                    "__last_touch_at".to_string(),
                ];
                let mut values = vec![
                    format!(
                        "'{}'",
                        $crate::persist::sql_escape_string(&self.__persist_id)
                    ),
                    self.__metadata.version.to_string(),
                    self.__metadata.schema_version.to_string(),
                    self.__metadata.touch_count.to_string(),
                    format!("'{}'", self.__metadata.created_at.to_rfc3339()),
                    format!("'{}'", self.__metadata.updated_at.to_rfc3339()),
                    format!("'{}'", self.__metadata.last_touch_at.to_rfc3339()),
                ];

                $(
                    columns.push(stringify!($field).to_string());
                    values.push(
                        <$field_ty as $crate::persist::PersistValue>::to_sql_literal(&self.$field)
                    );
                )+

                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.__table_name,
                    columns.join(", "),
                    values.join(", ")
                )
            }

            fn __update_sql(&self, expected_version: i64, new_version: i64) -> Option<String> {
                if self.__dirty_fields.is_empty() {
                    return None;
                }

                let mut set_clauses = Vec::new();

                $(
                    if self.__dirty_fields.contains(stringify!($field)) {
                        set_clauses.push(format!(
                            "{} = {}",
                            stringify!($field),
                            <$field_ty as $crate::persist::PersistValue>::to_sql_literal(&self.$field)
                        ));
                    }
                )+

                set_clauses.push(format!("__version = {}", new_version));
                set_clauses.push(format!(
                    "__schema_version = {}",
                    self.__metadata.schema_version
                ));
                set_clauses.push(format!(
                    "__updated_at = '{}'",
                    self.__metadata.updated_at.to_rfc3339()
                ));
                set_clauses.push(format!(
                    "__last_touch_at = '{}'",
                    self.__metadata.last_touch_at.to_rfc3339()
                ));
                set_clauses.push(format!("__touch_count = {}", self.__metadata.touch_count));

                Some(format!(
                    "UPDATE {} SET {} WHERE __persist_id = '{}' AND __version = {}",
                    self.__table_name,
                    set_clauses.join(", "),
                    $crate::persist::sql_escape_string(&self.__persist_id),
                    expected_version
                ))
            }

            fn __require_no_args(
                function: &str,
                args: &[$crate::core::Value],
            ) -> $crate::core::Result<()> {
                if args.is_empty() {
                    return Ok(());
                }

                Err($crate::core::DbError::ExecutionError(format!(
                    "Function '{}' expects 0 arguments, got {}",
                    function,
                    args.len()
                )))
            }

            $crate::paste::paste! {
                $(
                    /// Generated typed setter for this field.
                    pub fn [<set_ $field>](&mut self, value: $field_ty) {
                        if self.$field != value {
                            self.$field = value;
                            self.__mark_dirty(stringify!($field));
                        }
                    }

                    /// Generated typed setter with auto-persist behavior.
                    pub async fn [<set_ $field _persisted>](
                        &mut self,
                        value: $field_ty,
                    ) -> $crate::core::Result<bool> {
                        let changed = if self.$field != value {
                            self.$field = value;
                            self.__mark_dirty(stringify!($field));
                            true
                        } else {
                            false
                        };

                        self.__auto_persist_if_enabled().await?;
                        Ok(changed)
                    }

                    /// Generated typed getter for this field.
                    pub fn $field(&self) -> &$field_ty {
                        &self.$field
                    }
                )+
            }
        }

        #[async_trait::async_trait]
        impl $crate::persist::PersistEntity for $name {
            fn type_name(&self) -> &'static str {
                stringify!($name)
            }

            fn table_name(&self) -> &str {
                &self.__table_name
            }

            fn persist_id(&self) -> &str {
                &self.__persist_id
            }

            fn metadata(&self) -> &$crate::persist::PersistMetadata {
                &self.__metadata
            }

            fn metadata_mut(&mut self) -> &mut $crate::persist::PersistMetadata {
                &mut self.__metadata
            }

            fn unique_fields(&self) -> Vec<&'static str> {
                let mut fields = Vec::new();
                $(
                    if $crate::__persist_field_is_unique!($(#[$($field_meta)*])*) {
                        fields.push(stringify!($field));
                    }
                )+
                fields
            }

            fn indexed_fields(&self) -> Vec<&'static str> {
                let mut fields = Vec::new();
                $(
                    if $crate::__persist_field_is_indexed!($(#[$($field_meta)*])*)
                        || $crate::__persist_field_is_unique!($(#[$($field_meta)*])*)
                    {
                        fields.push(stringify!($field));
                    }
                )+
                fields
            }

            fn descriptor(&self) -> $crate::persist::ObjectDescriptor {
                $crate::persist::ObjectDescriptor {
                    type_name: stringify!($name).to_string(),
                    table_name: self.__table_name.clone(),
                    functions: self.available_functions(),
                }
            }

            fn state(&self) -> $crate::persist::PersistState {
                $crate::persist::PersistState {
                    persist_id: self.__persist_id.clone(),
                    type_name: stringify!($name).to_string(),
                    table_name: self.__table_name.clone(),
                    metadata: self.__metadata.clone(),
                    fields: self.state_json(),
                }
            }

            fn supports_function(&self, function: &str) -> bool {
                matches!(
                    function,
                    "state"
                        | "save"
                        | "delete"
                        | "touch"
                        | "save_bound"
                        | "delete_bound"
                        | "enable_auto_persist"
                        | "disable_auto_persist"
                )
                    || self.__functions.contains_key(function)
            }

            fn available_functions(&self) -> Vec<$crate::persist::FunctionDescriptor> {
                let mut functions = vec![
                    $crate::persist::FunctionDescriptor {
                        name: "state".to_string(),
                        arg_count: 0,
                        mutates_state: false,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "save".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "delete".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "touch".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "save_bound".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "delete_bound".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "enable_auto_persist".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "disable_auto_persist".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                ];

                let mut custom_names: Vec<String> = self.__functions.keys().cloned().collect();
                custom_names.sort();
                for name in custom_names {
                    functions.push($crate::persist::FunctionDescriptor {
                        name,
                        arg_count: 0,
                        mutates_state: true,
                    });
                }

                functions
            }

            async fn ensure_table(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                if self.__table_ready {
                    return Ok(());
                }
                session.execute(&self.__create_table_sql()).await?;
                let migration_plan = <Self as $crate::persist::PersistEntityFactory>::migration_plan();
                migration_plan
                    .ensure_table_schema_version(session, &self.__table_name)
                    .await?;
                for statement in self.__create_index_sqls() {
                    session.execute(&statement).await?;
                }
                self.__table_ready = true;
                Ok(())
            }

            async fn save(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.ensure_table(session).await?;
                self.__metadata.schema_version = self
                    .__metadata
                    .schema_version
                    .max(<Self as $crate::persist::PersistEntityFactory>::schema_version());
                let now = chrono::Utc::now();

                if !self.__metadata.persisted {
                    if self.__metadata.version <= 0 {
                        self.__metadata.version = 1;
                    }
                    if self.__metadata.touch_count == 0 {
                        self.__metadata.touch_count = 1;
                    }
                    self.__metadata.updated_at = now;
                    self.__metadata.last_touch_at = now;

                    let sql = self.__insert_sql();
                    session.execute(&sql).await?;
                    self.__metadata.persisted = true;
                    self.__dirty_fields.clear();
                    return Ok(());
                }

                if self.__dirty_fields.is_empty() {
                    return Ok(());
                }

                if self.__metadata.touch_count == 0 {
                    self.__metadata.touch_count = 1;
                }
                self.__metadata.updated_at = now;
                self.__metadata.last_touch_at = now;

                let expected_version = self.__metadata.version.max(1);
                let new_version = expected_version + 1;
                let sql = self
                    .__update_sql(expected_version, new_version)
                    .ok_or_else(|| $crate::core::DbError::ExecutionError(
                        "No changed fields to update".to_string()
                    ))?;

                let result = session.execute(&sql).await?;
                if matches!(result.affected_rows(), Some(0)) {
                    return Err($crate::core::DbError::ExecutionError(format!(
                        "Optimistic lock conflict for {}:{}",
                        self.__table_name,
                        self.__persist_id
                    )));
                }

                self.__metadata.version = new_version;
                self.__dirty_fields.clear();
                Ok(())
            }

            async fn delete(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                if !self.__metadata.persisted {
                    return Ok(());
                }

                let sql = format!(
                    "DELETE FROM {} WHERE __persist_id = '{}'",
                    self.__table_name,
                    $crate::persist::sql_escape_string(&self.__persist_id)
                );

                session.execute(&sql).await?;
                self.__metadata.persisted = false;
                self.__dirty_fields.clear();
                Ok(())
            }

            async fn invoke(
                &mut self,
                function: &str,
                args: Vec<$crate::core::Value>,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<$crate::core::Value> {
                match function {
                    "state" => {
                        Self::__require_no_args(function, &args)?;
                        let json = serde_json::to_value(self.state())
                            .map_err(|err| $crate::persist::serde_to_db_error("serialize state", err))?;
                        Ok($crate::core::Value::Json(json))
                    }
                    "touch" => {
                        Self::__require_no_args(function, &args)?;
                        self.touch();
                        Ok($crate::core::Value::Integer(self.__metadata.touch_count as i64))
                    }
                    "save" => {
                        Self::__require_no_args(function, &args)?;
                        self.save(session).await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "delete" => {
                        Self::__require_no_args(function, &args)?;
                        self.delete(session).await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "save_bound" => {
                        Self::__require_no_args(function, &args)?;
                        self.save_bound().await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "delete_bound" => {
                        Self::__require_no_args(function, &args)?;
                        self.delete_bound().await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "enable_auto_persist" => {
                        Self::__require_no_args(function, &args)?;
                        self.set_auto_persist(true)?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "disable_auto_persist" => {
                        Self::__require_no_args(function, &args)?;
                        self.set_auto_persist(false)?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    custom => {
                        if let Some(handler) = self.__functions.get(custom).cloned() {
                            return handler(self, args);
                        }
                        Err($crate::core::DbError::ExecutionError(format!(
                            "Function '{}' is not available for {}",
                            custom,
                            stringify!($name)
                        )))
                    }
                }
            }
        }

        #[async_trait::async_trait]
        impl $crate::persist::PersistEntityFactory for $name {
            fn entity_type_name() -> &'static str {
                stringify!($name)
            }

            fn default_table_name() -> String {
                Self::default_table_name()
            }

            fn create_table_sql(table_name: &str) -> String {
                Self::create_table_sql_for(table_name)
            }

            fn from_state(state: &$crate::persist::PersistState) -> $crate::core::Result<Self> {
                let fields = state
                    .fields
                    .as_object()
                    .ok_or_else(|| $crate::core::DbError::ExecutionError(
                        "Persist state 'fields' must be a JSON object".to_string()
                    ))?;

                Self::__type_checks();

                $(
                    let $field: $field_ty = serde_json::from_value(
                        fields
                            .get(stringify!($field))
                            .cloned()
                            .ok_or_else(|| $crate::core::DbError::ExecutionError(
                                format!("Field '{}' missing in persisted state", stringify!($field))
                            ))?
                    )
                    .map_err(|err| {
                        $crate::persist::serde_to_db_error(
                            &format!("deserialize field '{}'", stringify!($field)),
                            err
                        )
                    })?;
                )+

                let mut metadata = state.metadata.clone();
                // On restore we re-persist state into current DB instance.
                metadata.persisted = false;

                Ok(Self {
                    $( $field, )+
                    __persist_id: state.persist_id.clone(),
                    __table_name: state.table_name.clone(),
                    __metadata: metadata,
                    __dirty_fields: std::collections::HashSet::new(),
                    __table_ready: false,
                    __bound_session: None,
                    __auto_persist: false,
                    __functions: std::collections::HashMap::new(),
                })
            }
        }

        $crate::paste::paste! {
            impl $crate::persist::PersistCommandModel for $name {
                type Draft = [<$name Draft>];
                type Patch = [<$name Patch>];
                type Command = [<$name Command>];

                fn from_draft(draft: Self::Draft) -> Self {
                    Self::new($(draft.$field),+)
                }

                fn apply_patch_model(&mut self, patch: Self::Patch) -> $crate::core::Result<bool> {
                    self.patch(patch)
                }

                fn apply_command_model(
                    &mut self,
                    command: Self::Command,
                ) -> $crate::core::Result<bool> {
                    self.apply(command)
                }

                fn validate_patch_payload(patch: &Self::Patch) -> $crate::core::Result<()> {
                    patch.validate()
                }

                fn patch_contract() -> Vec<$crate::persist::PersistPatchContract> {
                    vec![
                        $(
                            $crate::persist::PersistPatchContract {
                                field: stringify!($field).to_string(),
                                rust_type: stringify!($field_ty).to_string(),
                                optional: true,
                            },
                        )+
                    ]
                }

                fn command_contract() -> Vec<$crate::persist::PersistCommandContract> {
                    let mut contracts = vec![
                        $(
                            $crate::persist::PersistCommandContract {
                                name: stringify!([<Set $field:camel>]).to_string(),
                                fields: vec![
                                    $crate::persist::PersistCommandFieldContract {
                                        name: stringify!($field).to_string(),
                                        rust_type: stringify!($field_ty).to_string(),
                                        optional: false,
                                    },
                                ],
                                mutates_state: true,
                            },
                        )+
                    ];

                    contracts.push($crate::persist::PersistCommandContract {
                        name: "Touch".to_string(),
                        fields: Vec::new(),
                        mutates_state: true,
                    });

                    contracts
                }
            }
        }
    };
    (@impl_dynamic $vis:vis $name:ident $table_expr:expr; $source_kind:ident; $source:expr) => {
        $vis struct $name {
            __fields: std::collections::BTreeMap<String, $crate::core::Value>,
            __schema: $crate::persist::DynamicSchema,
            __persist_id: String,
            __metadata: $crate::persist::PersistMetadata,
            __dirty_fields: std::collections::HashSet<String>,
            __table_ready: bool,
            __bound_session: Option<$crate::persist::PersistSession>,
            __auto_persist: bool,
            __functions: std::collections::HashMap<
                String,
                std::sync::Arc<
                    dyn Fn(
                            &mut Self,
                            Vec<$crate::core::Value>
                        ) -> $crate::core::Result<$crate::core::Value>
                        + Send
                        + Sync
                >,
            >,
        }

        $crate::paste::paste! {
            #[derive(Debug, Clone)]
            $vis struct [<$name Draft>] {
                __fields: std::collections::BTreeMap<String, $crate::core::Value>,
                __schema: $crate::persist::DynamicSchema,
            }

            impl Default for [<$name Draft>] {
                fn default() -> Self {
                    let table_name = $name::default_table_name();
                    let schema = $name::__schema_from_source(table_name).unwrap_or_else(|err| {
                        panic!(
                            "failed to build draft schema for {}: {}",
                            stringify!($name),
                            err
                        )
                    });
                    Self {
                        __fields: std::collections::BTreeMap::new(),
                        __schema: schema,
                    }
                }
            }

            impl [<$name Draft>] {
                /// Creates an empty dynamic draft payload.
                pub fn new() -> Self {
                    Self::default()
                }

                /// Creates an empty dynamic draft bound to a specific table name.
                pub fn with_table_name(table_name: impl Into<String>) -> $crate::core::Result<Self> {
                    let schema = $name::__schema_from_source(table_name.into())?;
                    Ok(Self {
                        __fields: std::collections::BTreeMap::new(),
                        __schema: schema,
                    })
                }

                /// Builds draft from provided field map with schema validation.
                pub fn from_fields(
                    fields: std::collections::BTreeMap<String, $crate::core::Value>,
                ) -> $crate::core::Result<Self> {
                    let mut draft = Self::default();
                    for (name, value) in fields {
                        draft.insert(name, value)?;
                    }
                    Ok(draft)
                }

                /// Inserts/overwrites one draft field with validation.
                pub fn insert(
                    &mut self,
                    name: impl Into<String>,
                    value: $crate::core::Value,
                ) -> $crate::core::Result<()> {
                    let field_name = name.into();
                    $name::__validate_field_in_schema(&self.__schema, &field_name, &value)?;
                    self.__fields.insert(field_name, value);
                    Ok(())
                }

                /// Builder-style variant of `insert`.
                pub fn with_field(
                    mut self,
                    name: impl Into<String>,
                    value: $crate::core::Value,
                ) -> $crate::core::Result<Self> {
                    self.insert(name, value)?;
                    Ok(self)
                }

                /// Returns `true` when draft has no fields.
                pub fn is_empty(&self) -> bool {
                    self.__fields.is_empty()
                }

                /// Returns table name associated with this draft schema.
                pub fn table_name(&self) -> &str {
                    &self.__schema.table_name
                }

                /// Returns immutable access to raw draft field map.
                pub fn fields(&self) -> &std::collections::BTreeMap<String, $crate::core::Value> {
                    &self.__fields
                }
            }

            #[derive(Debug, Clone, Default)]
            $vis struct [<$name Patch>] {
                __fields: std::collections::BTreeMap<String, $crate::core::Value>,
            }

            impl [<$name Patch>] {
                /// Creates an empty dynamic patch payload.
                pub fn new() -> Self {
                    Self::default()
                }

                /// Builds patch from provided field map and validates it.
                pub fn from_fields(
                    fields: std::collections::BTreeMap<String, $crate::core::Value>,
                ) -> $crate::core::Result<Self> {
                    let patch = Self { __fields: fields };
                    patch.validate()?;
                    Ok(patch)
                }

                /// Inserts/overwrites one patch field with validation.
                pub fn insert(
                    &mut self,
                    name: impl Into<String>,
                    value: $crate::core::Value,
                ) -> $crate::core::Result<()> {
                    let field_name = name.into();
                    let schema = $name::__schema_for_contracts()?;
                    $name::__validate_field_in_schema(&schema, &field_name, &value)?;
                    self.__fields.insert(field_name, value);
                    Ok(())
                }

                /// Builder-style variant of `insert`.
                pub fn with_field(
                    mut self,
                    name: impl Into<String>,
                    value: $crate::core::Value,
                ) -> $crate::core::Result<Self> {
                    self.insert(name, value)?;
                    Ok(self)
                }

                /// Returns `true` when patch has no fields.
                pub fn is_empty(&self) -> bool {
                    self.__fields.is_empty()
                }

                /// Returns immutable access to raw patch field map.
                pub fn fields(&self) -> &std::collections::BTreeMap<String, $crate::core::Value> {
                    &self.__fields
                }

                /// Validates patch payload against dynamic schema contracts.
                pub fn validate(&self) -> $crate::core::Result<()> {
                    if self.is_empty() {
                        return Err($crate::core::DbError::ExecutionError(
                            "Patch payload must include at least one field".to_string(),
                        ));
                    }

                    let schema = $name::__schema_for_contracts()?;
                    $name::__validate_fields_map(&schema, &self.__fields, true)
                }
            }

            #[derive(Debug, Clone)]
            $vis enum [<$name Command>] {
                SetField {
                    field: String,
                    value: $crate::core::Value,
                },
                Touch,
            }

            impl [<$name Command>] {
                /// Creates `SetField` command for dynamic entities.
                pub fn set(field: impl Into<String>, value: $crate::core::Value) -> Self {
                    Self::SetField {
                        field: field.into(),
                        value,
                    }
                }

                /// Returns stable command name for telemetry/audit use.
                pub fn name(&self) -> &'static str {
                    match self {
                        Self::SetField { .. } => "SetField",
                        Self::Touch => "Touch",
                    }
                }
            }

            impl $crate::persist::PersistCommandName for [<$name Command>] {
                fn command_name(&self) -> &'static str {
                    self.name()
                }
            }
        }

        impl $name {
            /// Returns default table name generated for this dynamic model.
            pub fn default_table_name() -> String {
                ($table_expr).to_string()
            }

            fn __schema_from_source(table_name: String) -> $crate::core::Result<$crate::persist::DynamicSchema> {
                match stringify!($source_kind) {
                    "ddl" => $crate::persist::dynamic_schema_from_ddl($source, table_name),
                    "json_schema" => $crate::persist::dynamic_schema_from_json_schema($source, table_name),
                    kind => Err($crate::core::DbError::ExecutionError(format!(
                        "Unsupported dynamic schema kind '{}'",
                        kind
                    ))),
                }
            }

            fn __schema_for_contracts() -> $crate::core::Result<$crate::persist::DynamicSchema> {
                Self::__schema_from_source(Self::default_table_name())
            }

            fn __validate_field_in_schema(
                schema: &$crate::persist::DynamicSchema,
                name: &str,
                value: &$crate::core::Value,
            ) -> $crate::core::Result<()> {
                let field = schema.field(name).ok_or_else(|| {
                    $crate::core::DbError::ColumnNotFound(name.to_string(), schema.table_name.clone())
                })?;

                if !field.nullable && matches!(value, $crate::core::Value::Null) {
                    return Err($crate::core::DbError::ConstraintViolation(format!(
                        "Field '{}' cannot be NULL",
                        name
                    )));
                }

                if !$crate::persist::value_matches_sql_type(value, &field.sql_type) {
                    return Err($crate::core::DbError::TypeMismatch(format!(
                        "Field '{}' expects SQL type '{}', got {}",
                        name,
                        field.sql_type,
                        value.type_name()
                    )));
                }

                Ok(())
            }

            fn __validate_fields_map(
                schema: &$crate::persist::DynamicSchema,
                fields: &std::collections::BTreeMap<String, $crate::core::Value>,
                require_non_empty: bool,
            ) -> $crate::core::Result<()> {
                if require_non_empty && fields.is_empty() {
                    return Err($crate::core::DbError::ExecutionError(
                        "Patch payload must include at least one field".to_string(),
                    ));
                }

                for (name, value) in fields {
                    Self::__validate_field_in_schema(schema, name, value)?;
                }

                Ok(())
            }

            /// Creates a new dynamic entity initialized from schema defaults.
            pub fn new() -> $crate::core::Result<Self> {
                let table_name = Self::default_table_name();
                let schema = Self::__schema_from_source(table_name)?;
                let now = chrono::Utc::now();

                Ok(Self {
                    __fields: schema.default_value_map(),
                    __schema: schema,
                    __persist_id: $crate::persist::new_persist_id(),
                    __metadata: $crate::persist::PersistMetadata::new(now),
                    __dirty_fields: std::collections::HashSet::new(),
                    __table_ready: false,
                    __bound_session: None,
                    __auto_persist: false,
                    __functions: std::collections::HashMap::new(),
                })
            }

            /// Creates a new dynamic entity with explicit table name.
            pub fn with_table_name(table_name: impl Into<String>) -> $crate::core::Result<Self> {
                let schema = Self::__schema_from_source(table_name.into())?;
                let now = chrono::Utc::now();

                Ok(Self {
                    __fields: schema.default_value_map(),
                    __schema: schema,
                    __persist_id: $crate::persist::new_persist_id(),
                    __metadata: $crate::persist::PersistMetadata::new(now),
                    __dirty_fields: std::collections::HashSet::new(),
                    __table_ready: false,
                    __bound_session: None,
                    __auto_persist: false,
                    __functions: std::collections::HashMap::new(),
                })
            }

            fn __set_field_internal(
                &mut self,
                name: impl Into<String>,
                value: $crate::core::Value,
            ) -> $crate::core::Result<bool> {
                let name = name.into();
                Self::__validate_field_in_schema(&self.__schema, &name, &value)?;

                let needs_update = self
                    .__fields
                    .get(&name)
                    .is_none_or(|current| current != &value);
                if needs_update {
                    self.__fields.insert(name.clone(), value);
                    self.__dirty_fields.insert(name);
                    self.touch();
                }

                Ok(needs_update)
            }

            /// Sets one dynamic field with schema/type validation.
            pub fn set_field(
                &mut self,
                name: impl Into<String>,
                value: $crate::core::Value,
            ) -> $crate::core::Result<()> {
                self.__set_field_internal(name, value).map(|_| ())
            }

            /// Sets one dynamic field and flushes auto-persist if configured.
            pub async fn set_field_persisted(
                &mut self,
                name: impl Into<String>,
                value: $crate::core::Value,
            ) -> $crate::core::Result<bool> {
                let changed = self.__set_field_internal(name, value)?;
                self.__auto_persist_if_enabled().await?;
                Ok(changed)
            }

            /// Returns one dynamic field by name.
            pub fn get_field(&self, name: &str) -> Option<&$crate::core::Value> {
                self.__fields.get(name)
            }

            /// Returns immutable access to dynamic field map.
            pub fn fields(&self) -> &std::collections::BTreeMap<String, $crate::core::Value> {
                &self.__fields
            }

            /// Returns entity persistence identifier.
            pub fn persist_id(&self) -> &str {
                &self.__persist_id
            }

            /// Returns physical table name used for this entity.
            pub fn table_name(&self) -> &str {
                &self.__schema.table_name
            }

            /// Returns persistence metadata (version/timestamps/state flags).
            pub fn metadata(&self) -> &$crate::persist::PersistMetadata {
                &self.__metadata
            }

            /// Binds a session so helper persisted methods can execute without explicit session argument.
            pub fn bind_session(&mut self, session: $crate::persist::PersistSession) {
                self.__bound_session = Some(session);
            }

            /// Unbinds current session and disables auto-persist mode.
            pub fn unbind_session(&mut self) {
                self.__bound_session = None;
                self.__auto_persist = false;
            }

            /// Returns `true` when a session is currently bound.
            pub fn has_bound_session(&self) -> bool {
                self.__bound_session.is_some()
            }

            /// Returns current auto-persist flag.
            pub fn auto_persist_enabled(&self) -> bool {
                self.__auto_persist
            }

            /// Enables or disables auto-persist for bound-session workflows.
            pub fn set_auto_persist(&mut self, enabled: bool) -> $crate::core::Result<()> {
                if enabled && self.__bound_session.is_none() {
                    return Err($crate::core::DbError::ExecutionError(
                        "Auto-persist requires a bound PersistSession".to_string()
                    ));
                }

                self.__auto_persist = enabled;
                Ok(())
            }

            /// Saves entity through bound session.
            pub async fn save_bound(&mut self) -> $crate::core::Result<()> {
                let session = self.__bound_session.clone().ok_or_else(|| {
                    $crate::core::DbError::ExecutionError(
                        "No bound PersistSession for save_bound".to_string()
                    )
                })?;
                <Self as $crate::persist::PersistEntity>::save(self, &session).await
            }

            /// Deletes entity through bound session.
            pub async fn delete_bound(&mut self) -> $crate::core::Result<()> {
                let session = self.__bound_session.clone().ok_or_else(|| {
                    $crate::core::DbError::ExecutionError(
                        "No bound PersistSession for delete_bound".to_string()
                    )
                })?;
                <Self as $crate::persist::PersistEntity>::delete(self, &session).await
            }

            async fn __auto_persist_if_enabled(&mut self) -> $crate::core::Result<()> {
                if !self.__auto_persist || self.__dirty_fields.is_empty() {
                    return Ok(());
                }

                let session = self.__bound_session.clone().ok_or_else(|| {
                    $crate::core::DbError::ExecutionError(
                        "Auto-persist is enabled but no PersistSession is bound".to_string()
                    )
                })?;
                <Self as $crate::persist::PersistEntity>::save(self, &session).await
            }

            /// Applies a local mutation and, when enabled, flushes one auto-persist save.
            pub async fn mutate_persisted<F>(&mut self, mutator: F) -> $crate::core::Result<()>
            where
                F: FnOnce(&mut Self),
            {
                mutator(self);
                self.__auto_persist_if_enabled().await
            }

            $crate::paste::paste! {
                /// Builds entity from command-first draft payload.
                pub fn from_draft(draft: [<$name Draft>]) -> Self {
                    match <Self as $crate::persist::PersistCommandModel>::try_from_draft(draft) {
                        Ok(entity) => entity,
                        Err(err) => panic!(
                            "failed to build {} from draft: {}",
                            stringify!($name),
                            err
                        ),
                    }
                }

                /// Applies patch payload and returns whether state changed.
                pub fn patch(&mut self, patch: [<$name Patch>]) -> $crate::core::Result<bool> {
                    patch.validate()?;

                    let mut changed = false;
                    for (field, value) in patch.__fields {
                        if self.__set_field_internal(field, value)? {
                            changed = true;
                        }
                    }

                    Ok(changed)
                }

                /// Applies command payload and returns whether state changed.
                pub fn apply(&mut self, command: [<$name Command>]) -> $crate::core::Result<bool> {
                    match command {
                        [<$name Command>]::SetField { field, value } => {
                            self.__set_field_internal(field, value)
                        }
                        [<$name Command>]::Touch => {
                            self.touch();
                            Ok(true)
                        }
                    }
                }

                /// Applies patch and flushes auto-persist if configured.
                pub async fn patch_persisted(
                    &mut self,
                    patch: [<$name Patch>],
                ) -> $crate::core::Result<bool> {
                    let changed = self.patch(patch)?;
                    self.__auto_persist_if_enabled().await?;
                    Ok(changed)
                }

                /// Applies command and flushes auto-persist if configured.
                pub async fn apply_persisted(
                    &mut self,
                    command: [<$name Command>],
                ) -> $crate::core::Result<bool> {
                    let changed = self.apply(command)?;
                    self.__auto_persist_if_enabled().await?;
                    Ok(changed)
                }
            }

            /// Increments touch metadata counters/timestamps.
            pub fn touch(&mut self) {
                self.__metadata.touch_count = self.__metadata.touch_count.saturating_add(1);
                self.__metadata.last_touch_at = chrono::Utc::now();
            }

            /// Registers a named dynamic function handler on this entity instance.
            pub fn register_function<F>(&mut self, name: impl Into<String>, handler: F)
            where
                F: Fn(
                        &mut Self,
                        Vec<$crate::core::Value>
                    ) -> $crate::core::Result<$crate::core::Value>
                    + Send
                    + Sync
                    + 'static,
            {
                self.__functions.insert(name.into(), std::sync::Arc::new(handler));
            }

            /// Returns entity business state as JSON object.
            pub fn state_json(&self) -> serde_json::Value {
                serde_json::to_value(&self.__fields).unwrap_or_else(|_| serde_json::json!({}))
            }

            fn __insert_sql(&self) -> String {
                let mut columns = vec![
                    "__persist_id".to_string(),
                    "__version".to_string(),
                    "__schema_version".to_string(),
                    "__touch_count".to_string(),
                    "__created_at".to_string(),
                    "__updated_at".to_string(),
                    "__last_touch_at".to_string(),
                ];
                let mut values = vec![
                    format!("'{}'", $crate::persist::sql_escape_string(&self.__persist_id)),
                    self.__metadata.version.to_string(),
                    self.__metadata.schema_version.to_string(),
                    self.__metadata.touch_count.to_string(),
                    format!("'{}'", self.__metadata.created_at.to_rfc3339()),
                    format!("'{}'", self.__metadata.updated_at.to_rfc3339()),
                    format!("'{}'", self.__metadata.last_touch_at.to_rfc3339()),
                ];

                for field in &self.__schema.fields {
                    columns.push(field.name.clone());
                    let value = self
                        .__fields
                        .get(&field.name)
                        .cloned()
                        .unwrap_or($crate::core::Value::Null);
                    values.push($crate::persist::value_to_sql_literal(&value));
                }

                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.__schema.table_name,
                    columns.join(", "),
                    values.join(", ")
                )
            }

            fn __update_sql(&self, expected_version: i64, new_version: i64) -> Option<String> {
                if self.__dirty_fields.is_empty() {
                    return None;
                }

                let mut assignments = Vec::new();
                for field_name in &self.__dirty_fields {
                    if let Some(value) = self.__fields.get(field_name) {
                        assignments.push(format!(
                            "{} = {}",
                            field_name,
                            $crate::persist::value_to_sql_literal(value)
                        ));
                    }
                }

                assignments.push(format!("__version = {}", new_version));
                assignments.push(format!(
                    "__schema_version = {}",
                    self.__metadata.schema_version
                ));
                assignments.push(format!(
                    "__updated_at = '{}'",
                    self.__metadata.updated_at.to_rfc3339()
                ));
                assignments.push(format!(
                    "__last_touch_at = '{}'",
                    self.__metadata.last_touch_at.to_rfc3339()
                ));
                assignments.push(format!("__touch_count = {}", self.__metadata.touch_count));

                Some(format!(
                    "UPDATE {} SET {} WHERE __persist_id = '{}' AND __version = {}",
                    self.__schema.table_name,
                    assignments.join(", "),
                    $crate::persist::sql_escape_string(&self.__persist_id),
                    expected_version
                ))
            }

            fn __require_no_args(
                function: &str,
                args: &[$crate::core::Value],
            ) -> $crate::core::Result<()> {
                if args.is_empty() {
                    return Ok(());
                }
                Err($crate::core::DbError::ExecutionError(format!(
                    "Function '{}' expects 0 arguments, got {}",
                    function,
                    args.len()
                )))
            }
        }

        #[async_trait::async_trait]
        impl $crate::persist::PersistEntity for $name {
            fn type_name(&self) -> &'static str {
                stringify!($name)
            }

            fn table_name(&self) -> &str {
                &self.__schema.table_name
            }

            fn persist_id(&self) -> &str {
                &self.__persist_id
            }

            fn metadata(&self) -> &$crate::persist::PersistMetadata {
                &self.__metadata
            }

            fn metadata_mut(&mut self) -> &mut $crate::persist::PersistMetadata {
                &mut self.__metadata
            }

            fn descriptor(&self) -> $crate::persist::ObjectDescriptor {
                $crate::persist::ObjectDescriptor {
                    type_name: stringify!($name).to_string(),
                    table_name: self.__schema.table_name.clone(),
                    functions: self.available_functions(),
                }
            }

            fn state(&self) -> $crate::persist::PersistState {
                $crate::persist::PersistState {
                    persist_id: self.__persist_id.clone(),
                    type_name: stringify!($name).to_string(),
                    table_name: self.__schema.table_name.clone(),
                    metadata: self.__metadata.clone(),
                    fields: self.state_json(),
                }
            }

            fn supports_function(&self, function: &str) -> bool {
                matches!(
                    function,
                    "state"
                        | "save"
                        | "delete"
                        | "touch"
                        | "save_bound"
                        | "delete_bound"
                        | "enable_auto_persist"
                        | "disable_auto_persist"
                )
                    || self.__functions.contains_key(function)
            }

            fn available_functions(&self) -> Vec<$crate::persist::FunctionDescriptor> {
                let mut functions = vec![
                    $crate::persist::FunctionDescriptor {
                        name: "state".to_string(),
                        arg_count: 0,
                        mutates_state: false,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "save".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "delete".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "touch".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "save_bound".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "delete_bound".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "enable_auto_persist".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    $crate::persist::FunctionDescriptor {
                        name: "disable_auto_persist".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                ];

                let mut custom_names: Vec<String> = self.__functions.keys().cloned().collect();
                custom_names.sort();
                for name in custom_names {
                    functions.push($crate::persist::FunctionDescriptor {
                        name,
                        arg_count: 0,
                        mutates_state: true,
                    });
                }

                functions
            }

            async fn ensure_table(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                if self.__table_ready {
                    return Ok(());
                }
                session.execute(&self.__schema.create_table_sql()).await?;
                let migration_plan = <Self as $crate::persist::PersistEntityFactory>::migration_plan();
                migration_plan
                    .ensure_table_schema_version(session, &self.__schema.table_name)
                    .await?;
                self.__table_ready = true;
                Ok(())
            }

            async fn save(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                self.ensure_table(session).await?;
                self.__metadata.schema_version = self
                    .__metadata
                    .schema_version
                    .max(<Self as $crate::persist::PersistEntityFactory>::schema_version());
                let now = chrono::Utc::now();

                if !self.__metadata.persisted {
                    if self.__metadata.version <= 0 {
                        self.__metadata.version = 1;
                    }
                    if self.__metadata.touch_count == 0 {
                        self.__metadata.touch_count = 1;
                    }
                    self.__metadata.updated_at = now;
                    self.__metadata.last_touch_at = now;

                    let sql = self.__insert_sql();
                    session.execute(&sql).await?;
                    self.__metadata.persisted = true;
                    self.__dirty_fields.clear();
                    return Ok(());
                }

                if self.__dirty_fields.is_empty() {
                    return Ok(());
                }

                if self.__metadata.touch_count == 0 {
                    self.__metadata.touch_count = 1;
                }
                self.__metadata.updated_at = now;
                self.__metadata.last_touch_at = now;

                let expected_version = self.__metadata.version.max(1);
                let new_version = expected_version + 1;
                let sql = self
                    .__update_sql(expected_version, new_version)
                    .ok_or_else(|| $crate::core::DbError::ExecutionError(
                        "No changed fields to update".to_string()
                    ))?;

                let result = session.execute(&sql).await?;
                if matches!(result.affected_rows(), Some(0)) {
                    return Err($crate::core::DbError::ExecutionError(format!(
                        "Optimistic lock conflict for {}:{}",
                        self.__schema.table_name,
                        self.__persist_id
                    )));
                }

                self.__metadata.version = new_version;
                self.__dirty_fields.clear();
                Ok(())
            }

            async fn delete(
                &mut self,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<()> {
                if !self.__metadata.persisted {
                    return Ok(());
                }

                let sql = format!(
                    "DELETE FROM {} WHERE __persist_id = '{}'",
                    self.__schema.table_name,
                    $crate::persist::sql_escape_string(&self.__persist_id)
                );
                session.execute(&sql).await?;
                self.__metadata.persisted = false;
                self.__dirty_fields.clear();
                Ok(())
            }

            async fn invoke(
                &mut self,
                function: &str,
                args: Vec<$crate::core::Value>,
                session: &$crate::persist::PersistSession,
            ) -> $crate::core::Result<$crate::core::Value> {
                match function {
                    "state" => {
                        Self::__require_no_args(function, &args)?;
                        let json = serde_json::to_value(self.state())
                            .map_err(|err| $crate::persist::serde_to_db_error("serialize state", err))?;
                        Ok($crate::core::Value::Json(json))
                    }
                    "touch" => {
                        Self::__require_no_args(function, &args)?;
                        self.touch();
                        Ok($crate::core::Value::Integer(self.__metadata.touch_count as i64))
                    }
                    "save" => {
                        Self::__require_no_args(function, &args)?;
                        self.save(session).await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "delete" => {
                        Self::__require_no_args(function, &args)?;
                        self.delete(session).await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "save_bound" => {
                        Self::__require_no_args(function, &args)?;
                        self.save_bound().await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "delete_bound" => {
                        Self::__require_no_args(function, &args)?;
                        self.delete_bound().await?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "enable_auto_persist" => {
                        Self::__require_no_args(function, &args)?;
                        self.set_auto_persist(true)?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    "disable_auto_persist" => {
                        Self::__require_no_args(function, &args)?;
                        self.set_auto_persist(false)?;
                        Ok($crate::core::Value::Boolean(true))
                    }
                    custom => {
                        if let Some(handler) = self.__functions.get(custom).cloned() {
                            return handler(self, args);
                        }
                        Err($crate::core::DbError::ExecutionError(format!(
                            "Function '{}' is not available for {}",
                            custom,
                            stringify!($name)
                        )))
                    }
                }
            }
        }

        #[async_trait::async_trait]
        impl $crate::persist::PersistEntityFactory for $name {
            fn entity_type_name() -> &'static str {
                stringify!($name)
            }

            fn default_table_name() -> String {
                Self::default_table_name()
            }

            fn create_table_sql(table_name: &str) -> String {
                match Self::__schema_from_source(table_name.to_string()) {
                    Ok(schema) => schema.create_table_sql(),
                    Err(_) => format!(
                        "CREATE TABLE IF NOT EXISTS {} (__persist_id TEXT PRIMARY KEY)",
                        table_name
                    ),
                }
            }

            fn from_state(state: &$crate::persist::PersistState) -> $crate::core::Result<Self> {
                let schema = Self::__schema_from_source(state.table_name.clone())?;
                let now = chrono::Utc::now();

                let fields_map: std::collections::BTreeMap<String, $crate::core::Value> =
                    serde_json::from_value(state.fields.clone()).map_err(|err| {
                        $crate::persist::serde_to_db_error("deserialize dynamic fields", err)
                    })?;

                let mut instance = Self {
                    __fields: schema.default_value_map(),
                    __schema: schema,
                    __persist_id: state.persist_id.clone(),
                    __metadata: $crate::persist::PersistMetadata::new(now),
                    __dirty_fields: std::collections::HashSet::new(),
                    __table_ready: false,
                    __bound_session: None,
                    __auto_persist: false,
                    __functions: std::collections::HashMap::new(),
                };

                for (key, value) in fields_map {
                    if instance.__schema.has_field(&key) {
                        instance.__fields.insert(key, value);
                    }
                }

                let mut metadata = state.metadata.clone();
                metadata.persisted = false;
                instance.__metadata = metadata;

                Ok(instance)
            }
        }

        $crate::paste::paste! {
            impl $crate::persist::PersistCommandModel for $name {
                type Draft = [<$name Draft>];
                type Patch = [<$name Patch>];
                type Command = [<$name Command>];

                fn from_draft(draft: Self::Draft) -> Self {
                    match Self::try_from_draft(draft) {
                        Ok(entity) => entity,
                        Err(err) => panic!(
                            "failed to build {} from draft: {}",
                            stringify!($name),
                            err
                        ),
                    }
                }

                fn try_from_draft(draft: Self::Draft) -> $crate::core::Result<Self> {
                    Self::validate_draft_payload(&draft)?;
                    let now = chrono::Utc::now();

                    let [<$name Draft>] { __fields, __schema } = draft;

                    let mut entity = Self {
                        __fields: __schema.default_value_map(),
                        __schema,
                        __persist_id: $crate::persist::new_persist_id(),
                        __metadata: $crate::persist::PersistMetadata::new(now),
                        __dirty_fields: std::collections::HashSet::new(),
                        __table_ready: false,
                        __bound_session: None,
                        __auto_persist: false,
                        __functions: std::collections::HashMap::new(),
                    };

                    for (field, value) in __fields {
                        entity.__fields.insert(field, value);
                    }

                    Ok(entity)
                }

                fn apply_patch_model(&mut self, patch: Self::Patch) -> $crate::core::Result<bool> {
                    self.patch(patch)
                }

                fn apply_command_model(
                    &mut self,
                    command: Self::Command,
                ) -> $crate::core::Result<bool> {
                    self.apply(command)
                }

                fn validate_draft_payload(draft: &Self::Draft) -> $crate::core::Result<()> {
                    Self::__validate_fields_map(&draft.__schema, &draft.__fields, false)?;
                    for field in &draft.__schema.fields {
                        if field.nullable {
                            continue;
                        }

                        let value = draft.__fields.get(&field.name).ok_or_else(|| {
                            $crate::core::DbError::ConstraintViolation(format!(
                                "Draft payload must include non-null field '{}'",
                                field.name
                            ))
                        })?;

                        if matches!(value, $crate::core::Value::Null) {
                            return Err($crate::core::DbError::ConstraintViolation(format!(
                                "Draft payload must include non-null field '{}'",
                                field.name
                            )));
                        }
                    }
                    Ok(())
                }

                fn validate_patch_payload(patch: &Self::Patch) -> $crate::core::Result<()> {
                    patch.validate()
                }

                fn validate_command_payload(command: &Self::Command) -> $crate::core::Result<()> {
                    let schema = Self::__schema_for_contracts()?;
                    match command {
                        [<$name Command>]::SetField { field, value } => {
                            Self::__validate_field_in_schema(&schema, field, value)
                        }
                        [<$name Command>]::Touch => Ok(()),
                    }
                }

                fn patch_contract() -> Vec<$crate::persist::PersistPatchContract> {
                    let schema = match Self::__schema_for_contracts() {
                        Ok(schema) => schema,
                        Err(_) => return Vec::new(),
                    };

                    schema
                        .fields
                        .into_iter()
                        .map(|field| $crate::persist::PersistPatchContract {
                            field: field.name,
                            rust_type: format!("Value<{}>", field.sql_type),
                            optional: true,
                        })
                        .collect()
                }

                fn command_contract() -> Vec<$crate::persist::PersistCommandContract> {
                    vec![
                        $crate::persist::PersistCommandContract {
                            name: "SetField".to_string(),
                            fields: vec![
                                $crate::persist::PersistCommandFieldContract {
                                    name: "field".to_string(),
                                    rust_type: "String".to_string(),
                                    optional: false,
                                },
                                $crate::persist::PersistCommandFieldContract {
                                    name: "value".to_string(),
                                    rust_type: "Value".to_string(),
                                    optional: false,
                                },
                            ],
                            mutates_state: true,
                        },
                        $crate::persist::PersistCommandContract {
                            name: "Touch".to_string(),
                            fields: Vec::new(),
                            mutates_state: true,
                        },
                    ]
                }
            }
        }
    };
}
