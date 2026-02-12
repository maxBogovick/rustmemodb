use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, Ident, LitStr, Type, parse_macro_input, spanned::Spanned,
};

#[proc_macro_derive(PersistModel, attributes(persist_model))]
pub fn derive_persist_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_persist_model(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_persist_model(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = input.ident;
    let vis = input.vis;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "PersistModel does not support generic structs yet",
        ));
    }

    let model_options = parse_persist_model_options(&input.attrs)?;

    let data_struct = match input.data {
        Data::Struct(data) => data,
        _ => {
            return Err(syn::Error::new(
                struct_name.span(),
                "PersistModel can only be derived for structs",
            ));
        }
    };

    let named_fields = match data_struct.fields {
        Fields::Named(fields) => fields,
        _ => {
            return Err(syn::Error::new(
                struct_name.span(),
                "PersistModel requires named fields",
            ));
        }
    };

    let mut field_idents = Vec::<Ident>::new();
    let mut field_types = Vec::<Type>::new();

    for field in named_fields.named {
        let ident = field.ident.clone().ok_or_else(|| {
            syn::Error::new(field.span(), "PersistModel requires named fields")
        })?;
        field_idents.push(ident);
        field_types.push(field.ty);
    }

    if field_idents.is_empty() {
        return Err(syn::Error::new(
            struct_name.span(),
            "PersistModel requires at least one field",
        ));
    }

    let persisted_name = format_ident!("{}Persisted", struct_name);
    let draft_name = format_ident!("{}Draft", persisted_name);
    let patch_name = format_ident!("{}Patch", persisted_name);
    let command_name = format_ident!("{}Command", persisted_name);
    let command_variant_idents = field_idents
        .iter()
        .map(|field| format_ident!("Set{}", to_pascal_case(&field.to_string())))
        .collect::<Vec<_>>();

    let default_table_expr = match model_options.table_name {
        Some(table_name) => quote! { #table_name.to_string() },
        None => quote! { ::rustmemodb::persist::default_table_name_stable(stringify!(#struct_name)) },
    };
    let schema_version_literal = model_options
        .schema_version
        .unwrap_or(1u32);

    let setter_methods = field_idents.iter().zip(field_types.iter()).map(|(field, ty)| {
        let setter = format_ident!("set_{}", field);
        let setter_persisted = format_ident!("set_{}_persisted", field);

        quote! {
            pub fn #setter(&mut self, value: #ty) {
                if self.data.#field != value {
                    self.data.#field = value;
                    self.__mark_dirty(stringify!(#field));
                }
            }

            pub async fn #setter_persisted(&mut self, value: #ty) -> ::rustmemodb::Result<bool> {
                let changed = if self.data.#field != value {
                    self.data.#field = value;
                    self.__mark_dirty(stringify!(#field));
                    true
                } else {
                    false
                };

                self.__auto_persist_if_enabled().await?;
                Ok(changed)
            }

            pub fn #field(&self) -> &#ty {
                &self.data.#field
            }
        }
    });

    let dirty_all_fields = field_idents.iter().map(|field| {
        quote! {
            self.__dirty_fields.insert(stringify!(#field));
        }
    });

    let sql_columns = field_idents.iter().zip(field_types.iter()).map(|(field, ty)| {
        quote! {
            columns.push(format!(
                "{} {}",
                stringify!(#field),
                <#ty as ::rustmemodb::PersistValue>::sql_type()
            ));
        }
    });

    let insert_columns = field_idents.iter().map(|field| {
        quote! {
            columns.push(stringify!(#field).to_string());
        }
    });

    let insert_values = field_idents.iter().zip(field_types.iter()).map(|(field, ty)| {
        quote! {
            values.push(
                <#ty as ::rustmemodb::PersistValue>::to_sql_literal(&self.data.#field)
            );
        }
    });

    let update_assignments = field_idents.iter().zip(field_types.iter()).map(|(field, ty)| {
        quote! {
            if self.__dirty_fields.contains(stringify!(#field)) {
                set_clauses.push(format!(
                    "{} = {}",
                    stringify!(#field),
                    <#ty as ::rustmemodb::PersistValue>::to_sql_literal(&self.data.#field)
                ));
            }
        }
    });

    let state_json_fields = field_idents.iter().map(|field| {
        quote! {
            stringify!(#field): &self.data.#field,
        }
    });

    let from_state_fields = field_idents.iter().zip(field_types.iter()).map(|(field, ty)| {
        quote! {
            let #field: #ty = serde_json::from_value(
                fields
                    .get(stringify!(#field))
                    .cloned()
                    .ok_or_else(|| ::rustmemodb::DbError::ExecutionError(
                        format!("Field '{}' missing in persisted state", stringify!(#field))
                    ))?
            )
            .map_err(|err| {
                ::rustmemodb::persist::serde_to_db_error(
                    &format!("deserialize field '{}'", stringify!(#field)),
                    err,
                )
            })?;
        }
    });

    let from_parts_args = field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(field, ty)| quote! { #field: #ty })
        .collect::<Vec<_>>();

    let from_parts_struct_fields = field_idents
        .iter()
        .map(|field| quote! { #field })
        .collect::<Vec<_>>();

    let patch_apply_steps = field_idents.iter().map(|field| {
        quote! {
            if let Some(value) = patch.#field {
                if self.data.#field != value {
                    self.data.#field = value;
                    self.__mark_dirty(stringify!(#field));
                    changed = true;
                }
            }
        }
    });

    let command_apply_arms = command_variant_idents
        .iter()
        .zip(field_idents.iter())
        .map(|(variant, field)| {
            quote! {
                #command_name::#variant(value) => {
                    let changed = self.data.#field != value;
                    if changed {
                        self.data.#field = value;
                        self.__mark_dirty(stringify!(#field));
                    }
                    Ok(changed)
                }
            }
        });

    Ok(quote! {
        #vis struct #draft_name {
            #( pub #field_idents: #field_types, )*
        }

        impl #draft_name {
            pub fn new(#(#from_parts_args),*) -> Self {
                Self {
                    #(#from_parts_struct_fields,)*
                }
            }
        }

        #vis struct #patch_name {
            #( pub #field_idents: Option<#field_types>, )*
        }

        impl Default for #patch_name {
            fn default() -> Self {
                Self {
                    #(#field_idents: None,)*
                }
            }
        }

        impl #patch_name {
            pub fn is_empty(&self) -> bool {
                true #(&& self.#field_idents.is_none())*
            }

            pub fn validate(&self) -> ::rustmemodb::Result<()> {
                if self.is_empty() {
                    return Err(::rustmemodb::DbError::ExecutionError(
                        "Patch payload must include at least one field".to_string(),
                    ));
                }
                Ok(())
            }
        }

        #vis enum #command_name {
            #( #command_variant_idents(#field_types), )*
            Touch,
        }

        impl #command_name {
            pub fn name(&self) -> &'static str {
                match self {
                    #( Self::#command_variant_idents(_) => stringify!(#command_variant_idents), )*
                    Self::Touch => "Touch",
                }
            }
        }

        impl #struct_name {
            pub fn into_persisted(self) -> #persisted_name {
                #persisted_name::new(self)
            }

            pub fn into_persisted_with_table(self, table_name: impl Into<String>) -> #persisted_name {
                #persisted_name::with_table_name(table_name, self)
            }
        }

        impl ::rustmemodb::persist::PersistModelExt for #struct_name {
            type Persisted = #persisted_name;

            fn into_persisted(self) -> Self::Persisted {
                #persisted_name::new(self)
            }
        }

        impl From<#struct_name> for #persisted_name {
            fn from(value: #struct_name) -> Self {
                Self::new(value)
            }
        }

        impl From<#persisted_name> for #struct_name {
            fn from(value: #persisted_name) -> Self {
                value.into_inner()
            }
        }

        #vis struct #persisted_name {
            data: #struct_name,
            __persist_id: String,
            __table_name: String,
            __metadata: ::rustmemodb::PersistMetadata,
            __dirty_fields: std::collections::HashSet<&'static str>,
            __table_ready: bool,
            __bound_session: Option<::rustmemodb::PersistSession>,
            __auto_persist: bool,
            __functions: std::collections::HashMap<
                String,
                std::sync::Arc<
                    dyn Fn(
                            &mut Self,
                            Vec<::rustmemodb::Value>
                        ) -> ::rustmemodb::Result<::rustmemodb::Value>
                        + Send
                        + Sync,
                >,
            >,
        }

        impl #persisted_name {
            fn __type_checks()
            where
                #( #field_types: ::rustmemodb::PersistValue, )*
            {}

            pub fn default_table_name() -> String {
                #default_table_expr
            }

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

                #( #sql_columns )*

                format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns.join(", "))
            }

            pub fn new(data: #struct_name) -> Self {
                Self::__type_checks();
                let now = chrono::Utc::now();
                Self {
                    data,
                    __persist_id: ::rustmemodb::persist::new_persist_id(),
                    __table_name: Self::default_table_name(),
                    __metadata: ::rustmemodb::PersistMetadata::new(now),
                    __dirty_fields: std::collections::HashSet::new(),
                    __table_ready: false,
                    __bound_session: None,
                    __auto_persist: false,
                    __functions: std::collections::HashMap::new(),
                }
            }

            pub fn with_table_name(table_name: impl Into<String>, data: #struct_name) -> Self {
                let mut this = Self::new(data);
                this.__table_name = table_name.into();
                this
            }

            pub fn from_parts(#(#from_parts_args),*) -> Self {
                Self::new(#struct_name {
                    #(#from_parts_struct_fields,)*
                })
            }

            pub fn data(&self) -> &#struct_name {
                &self.data
            }

            pub fn data_mut(&mut self) -> &mut #struct_name {
                &mut self.data
            }

            pub fn mark_all_dirty(&mut self) {
                #( #dirty_all_fields )*
                self.touch();
            }

            pub fn clear_dirty(&mut self) {
                self.__dirty_fields.clear();
            }

            pub fn into_inner(self) -> #struct_name {
                self.data
            }

            pub fn persist_id(&self) -> &str {
                &self.__persist_id
            }

            pub fn table_name(&self) -> &str {
                &self.__table_name
            }

            pub fn metadata(&self) -> &::rustmemodb::PersistMetadata {
                &self.__metadata
            }

            pub fn bind_session(&mut self, session: ::rustmemodb::PersistSession) {
                self.__bound_session = Some(session);
            }

            pub fn unbind_session(&mut self) {
                self.__bound_session = None;
                self.__auto_persist = false;
            }

            pub fn has_bound_session(&self) -> bool {
                self.__bound_session.is_some()
            }

            pub fn auto_persist_enabled(&self) -> bool {
                self.__auto_persist
            }

            pub fn set_auto_persist(&mut self, enabled: bool) -> ::rustmemodb::Result<()> {
                if enabled && self.__bound_session.is_none() {
                    return Err(::rustmemodb::DbError::ExecutionError(
                        "Auto-persist requires a bound PersistSession".to_string(),
                    ));
                }

                self.__auto_persist = enabled;
                Ok(())
            }

            pub async fn save_bound(&mut self) -> ::rustmemodb::Result<()> {
                let session = self.__bound_session.clone().ok_or_else(|| {
                    ::rustmemodb::DbError::ExecutionError(
                        "No bound PersistSession for save_bound".to_string(),
                    )
                })?;
                <Self as ::rustmemodb::PersistEntity>::save(self, &session).await
            }

            pub async fn delete_bound(&mut self) -> ::rustmemodb::Result<()> {
                let session = self.__bound_session.clone().ok_or_else(|| {
                    ::rustmemodb::DbError::ExecutionError(
                        "No bound PersistSession for delete_bound".to_string(),
                    )
                })?;
                <Self as ::rustmemodb::PersistEntity>::delete(self, &session).await
            }

            async fn __auto_persist_if_enabled(&mut self) -> ::rustmemodb::Result<()> {
                if !self.__auto_persist || self.__dirty_fields.is_empty() {
                    return Ok(());
                }

                let session = self.__bound_session.clone().ok_or_else(|| {
                    ::rustmemodb::DbError::ExecutionError(
                        "Auto-persist is enabled but no PersistSession is bound".to_string(),
                    )
                })?;
                <Self as ::rustmemodb::PersistEntity>::save(self, &session).await
            }

            pub async fn mutate_persisted<F>(&mut self, mutator: F) -> ::rustmemodb::Result<()>
            where
                F: FnOnce(&mut Self),
            {
                mutator(self);
                self.__auto_persist_if_enabled().await
            }

            pub fn from_draft(draft: #draft_name) -> Self {
                Self::from_parts(#(draft.#field_idents),*)
            }

            pub fn patch(&mut self, patch: #patch_name) -> ::rustmemodb::Result<bool> {
                patch.validate()?;
                let mut changed = false;
                #( #patch_apply_steps )*
                Ok(changed)
            }

            pub fn apply(&mut self, command: #command_name) -> ::rustmemodb::Result<bool> {
                match command {
                    #( #command_apply_arms, )*
                    #command_name::Touch => {
                        self.touch();
                        Ok(true)
                    }
                }
            }

            pub async fn patch_persisted(&mut self, patch: #patch_name) -> ::rustmemodb::Result<bool> {
                let changed = self.patch(patch)?;
                self.__auto_persist_if_enabled().await?;
                Ok(changed)
            }

            pub async fn apply_persisted(
                &mut self,
                command: #command_name,
            ) -> ::rustmemodb::Result<bool> {
                let changed = self.apply(command)?;
                self.__auto_persist_if_enabled().await?;
                Ok(changed)
            }

            pub fn touch(&mut self) {
                self.__metadata.touch_count = self.__metadata.touch_count.saturating_add(1);
                self.__metadata.last_touch_at = chrono::Utc::now();
            }

            fn __mark_dirty(&mut self, field: &'static str) {
                self.__dirty_fields.insert(field);
                self.touch();
            }

            pub fn register_function<F>(&mut self, name: impl Into<String>, handler: F)
            where
                F: Fn(
                        &mut Self,
                        Vec<::rustmemodb::Value>,
                    ) -> ::rustmemodb::Result<::rustmemodb::Value>
                    + Send
                    + Sync
                    + 'static,
            {
                self.__functions.insert(name.into(), std::sync::Arc::new(handler));
            }

            pub fn state_json(&self) -> serde_json::Value {
                serde_json::json!({
                    #( #state_json_fields )*
                })
            }

            pub fn descriptor(&self) -> ::rustmemodb::ObjectDescriptor {
                <Self as ::rustmemodb::PersistEntity>::descriptor(self)
            }

            pub fn available_functions(&self) -> Vec<::rustmemodb::FunctionDescriptor> {
                <Self as ::rustmemodb::PersistEntity>::available_functions(self)
            }

            fn __create_table_sql(&self) -> String {
                Self::create_table_sql_for(&self.__table_name)
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
                        ::rustmemodb::persist::sql_escape_string(&self.__persist_id)
                    ),
                    self.__metadata.version.to_string(),
                    self.__metadata.schema_version.to_string(),
                    self.__metadata.touch_count.to_string(),
                    format!("'{}'", self.__metadata.created_at.to_rfc3339()),
                    format!("'{}'", self.__metadata.updated_at.to_rfc3339()),
                    format!("'{}'", self.__metadata.last_touch_at.to_rfc3339()),
                ];

                #( #insert_columns )*
                #( #insert_values )*

                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.__table_name,
                    columns.join(", "),
                    values.join(", "),
                )
            }

            fn __update_sql(&self, expected_version: i64, new_version: i64) -> Option<String> {
                if self.__dirty_fields.is_empty() {
                    return None;
                }

                let mut set_clauses = Vec::new();

                #( #update_assignments )*

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
                    ::rustmemodb::persist::sql_escape_string(&self.__persist_id),
                    expected_version,
                ))
            }

            fn __require_no_args(function: &str, args: &[::rustmemodb::Value]) -> ::rustmemodb::Result<()> {
                if args.is_empty() {
                    return Ok(());
                }

                Err(::rustmemodb::DbError::ExecutionError(format!(
                    "Function '{}' expects 0 arguments, got {}",
                    function,
                    args.len(),
                )))
            }

            #( #setter_methods )*
        }

        #[async_trait::async_trait]
        impl ::rustmemodb::PersistEntity for #persisted_name {
            fn type_name(&self) -> &'static str {
                stringify!(#struct_name)
            }

            fn table_name(&self) -> &str {
                &self.__table_name
            }

            fn persist_id(&self) -> &str {
                &self.__persist_id
            }

            fn metadata(&self) -> &::rustmemodb::PersistMetadata {
                &self.__metadata
            }

            fn metadata_mut(&mut self) -> &mut ::rustmemodb::PersistMetadata {
                &mut self.__metadata
            }

            fn descriptor(&self) -> ::rustmemodb::ObjectDescriptor {
                ::rustmemodb::ObjectDescriptor {
                    type_name: stringify!(#struct_name).to_string(),
                    table_name: self.__table_name.clone(),
                    functions: self.available_functions(),
                }
            }

            fn state(&self) -> ::rustmemodb::PersistState {
                ::rustmemodb::PersistState {
                    persist_id: self.__persist_id.clone(),
                    type_name: stringify!(#struct_name).to_string(),
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
                ) || self.__functions.contains_key(function)
            }

            fn available_functions(&self) -> Vec<::rustmemodb::FunctionDescriptor> {
                let mut functions = vec![
                    ::rustmemodb::FunctionDescriptor {
                        name: "state".to_string(),
                        arg_count: 0,
                        mutates_state: false,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "save".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "delete".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "touch".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "save_bound".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "delete_bound".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "enable_auto_persist".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                    ::rustmemodb::FunctionDescriptor {
                        name: "disable_auto_persist".to_string(),
                        arg_count: 0,
                        mutates_state: true,
                    },
                ];

                let mut custom_names: Vec<String> = self.__functions.keys().cloned().collect();
                custom_names.sort();
                for name in custom_names {
                    functions.push(::rustmemodb::FunctionDescriptor {
                        name,
                        arg_count: 0,
                        mutates_state: true,
                    });
                }

                functions
            }

            async fn ensure_table(
                &mut self,
                session: &::rustmemodb::PersistSession,
            ) -> ::rustmemodb::Result<()> {
                if self.__table_ready {
                    return Ok(());
                }
                session.execute(&self.__create_table_sql()).await?;
                let migration_plan = <Self as ::rustmemodb::PersistEntityFactory>::migration_plan();
                migration_plan
                    .ensure_table_schema_version(session, &self.__table_name)
                    .await?;
                self.__table_ready = true;
                Ok(())
            }

            async fn save(
                &mut self,
                session: &::rustmemodb::PersistSession,
            ) -> ::rustmemodb::Result<()> {
                self.ensure_table(session).await?;
                self.__metadata.schema_version = self
                    .__metadata
                    .schema_version
                    .max(<Self as ::rustmemodb::PersistEntityFactory>::schema_version());
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
                    .ok_or_else(|| ::rustmemodb::DbError::ExecutionError(
                        "No changed fields to update".to_string(),
                    ))?;

                let result = session.execute(&sql).await?;
                if matches!(result.affected_rows(), Some(0)) {
                    return Err(::rustmemodb::DbError::ExecutionError(format!(
                        "Optimistic lock conflict for {}:{}",
                        self.__table_name,
                        self.__persist_id,
                    )));
                }

                self.__metadata.version = new_version;
                self.__dirty_fields.clear();
                Ok(())
            }

            async fn delete(
                &mut self,
                session: &::rustmemodb::PersistSession,
            ) -> ::rustmemodb::Result<()> {
                if !self.__metadata.persisted {
                    return Ok(());
                }

                let sql = format!(
                    "DELETE FROM {} WHERE __persist_id = '{}'",
                    self.__table_name,
                    ::rustmemodb::persist::sql_escape_string(&self.__persist_id),
                );

                session.execute(&sql).await?;
                self.__metadata.persisted = false;
                self.__dirty_fields.clear();
                Ok(())
            }

            async fn invoke(
                &mut self,
                function: &str,
                args: Vec<::rustmemodb::Value>,
                session: &::rustmemodb::PersistSession,
            ) -> ::rustmemodb::Result<::rustmemodb::Value> {
                match function {
                    "state" => {
                        Self::__require_no_args(function, &args)?;
                        let json = serde_json::to_value(self.state())
                            .map_err(|err| ::rustmemodb::persist::serde_to_db_error("serialize state", err))?;
                        Ok(::rustmemodb::Value::Json(json))
                    }
                    "touch" => {
                        Self::__require_no_args(function, &args)?;
                        self.touch();
                        Ok(::rustmemodb::Value::Integer(self.__metadata.touch_count as i64))
                    }
                    "save" => {
                        Self::__require_no_args(function, &args)?;
                        self.save(session).await?;
                        Ok(::rustmemodb::Value::Boolean(true))
                    }
                    "delete" => {
                        Self::__require_no_args(function, &args)?;
                        self.delete(session).await?;
                        Ok(::rustmemodb::Value::Boolean(true))
                    }
                    "save_bound" => {
                        Self::__require_no_args(function, &args)?;
                        self.save_bound().await?;
                        Ok(::rustmemodb::Value::Boolean(true))
                    }
                    "delete_bound" => {
                        Self::__require_no_args(function, &args)?;
                        self.delete_bound().await?;
                        Ok(::rustmemodb::Value::Boolean(true))
                    }
                    "enable_auto_persist" => {
                        Self::__require_no_args(function, &args)?;
                        self.set_auto_persist(true)?;
                        Ok(::rustmemodb::Value::Boolean(true))
                    }
                    "disable_auto_persist" => {
                        Self::__require_no_args(function, &args)?;
                        self.set_auto_persist(false)?;
                        Ok(::rustmemodb::Value::Boolean(true))
                    }
                    custom => {
                        if let Some(handler) = self.__functions.get(custom).cloned() {
                            return handler(self, args);
                        }
                        Err(::rustmemodb::DbError::ExecutionError(format!(
                            "Function '{}' is not available for {}",
                            custom,
                            stringify!(#persisted_name),
                        )))
                    }
                }
            }
        }

        #[async_trait::async_trait]
        impl ::rustmemodb::PersistEntityFactory for #persisted_name {
            fn entity_type_name() -> &'static str {
                stringify!(#struct_name)
            }

            fn default_table_name() -> String {
                Self::default_table_name()
            }

            fn create_table_sql(table_name: &str) -> String {
                Self::create_table_sql_for(table_name)
            }

            fn schema_version() -> u32 {
                #schema_version_literal
            }

            fn from_state(state: &::rustmemodb::PersistState) -> ::rustmemodb::Result<Self> {
                let fields = state
                    .fields
                    .as_object()
                    .ok_or_else(|| ::rustmemodb::DbError::ExecutionError(
                        "Persist state 'fields' must be a JSON object".to_string(),
                    ))?;

                Self::__type_checks();

                #( #from_state_fields )*

                let data = #struct_name {
                    #(#field_idents,)*
                };

                let mut metadata = state.metadata.clone();
                metadata.persisted = false;

                Ok(Self {
                    data,
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

        impl ::rustmemodb::persist::PersistCommandModel for #persisted_name {
            type Draft = #draft_name;
            type Patch = #patch_name;
            type Command = #command_name;

            fn from_draft(draft: Self::Draft) -> Self {
                Self::from_parts(#(draft.#field_idents),*)
            }

            fn apply_patch_model(&mut self, patch: Self::Patch) -> ::rustmemodb::Result<bool> {
                self.patch(patch)
            }

            fn apply_command_model(
                &mut self,
                command: Self::Command,
            ) -> ::rustmemodb::Result<bool> {
                self.apply(command)
            }

            fn validate_patch_payload(patch: &Self::Patch) -> ::rustmemodb::Result<()> {
                patch.validate()
            }

            fn patch_contract() -> Vec<::rustmemodb::persist::PersistPatchContract> {
                vec![
                    #(
                        ::rustmemodb::persist::PersistPatchContract {
                            field: stringify!(#field_idents).to_string(),
                            rust_type: stringify!(#field_types).to_string(),
                            optional: true,
                        },
                    )*
                ]
            }

            fn command_contract() -> Vec<::rustmemodb::persist::PersistCommandContract> {
                let mut contracts = vec![
                    #(
                        ::rustmemodb::persist::PersistCommandContract {
                            name: stringify!(#command_variant_idents).to_string(),
                            fields: vec![
                                ::rustmemodb::persist::PersistCommandFieldContract {
                                    name: stringify!(#field_idents).to_string(),
                                    rust_type: stringify!(#field_types).to_string(),
                                    optional: false,
                                },
                            ],
                            mutates_state: true,
                        },
                    )*
                ];

                contracts.push(::rustmemodb::persist::PersistCommandContract {
                    name: "Touch".to_string(),
                    fields: Vec::new(),
                    mutates_state: true,
                });

                contracts
            }
        }
    })
}

struct PersistModelOptions {
    table_name: Option<String>,
    schema_version: Option<u32>,
}

fn to_pascal_case(value: &str) -> String {
    let mut out = String::new();
    for chunk in value.split('_').filter(|part| !part.is_empty()) {
        let mut chars = chunk.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    if out.is_empty() {
        value.to_string()
    } else {
        out
    }
}

fn parse_persist_model_options(attrs: &[syn::Attribute]) -> syn::Result<PersistModelOptions> {
    let mut options = PersistModelOptions {
        table_name: None,
        schema_version: None,
    };

    for attr in attrs {
        if !attr.path().is_ident("persist_model") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("table") {
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                options.table_name = Some(lit.value());
                return Ok(());
            }

            if meta.path.is_ident("schema_version") {
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                options.schema_version = Some(lit.base10_parse::<u32>()?);
                return Ok(());
            }

            Err(meta.error(
                "Unsupported persist_model attribute. Supported: table = \"...\", schema_version = <u32>",
            ))
        })?;
    }

    Ok(options)
}
