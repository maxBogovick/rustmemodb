use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    Data, DeriveInput, Fields, FnArg, Ident, ImplItem, ImplItemFn, ItemFn, ItemImpl, ItemStruct,
    LitStr, Pat, PatType, ReturnType, Token, Type, TypePath, parse_macro_input,
    spanned::Spanned,
};

#[proc_macro_derive(PersistModel, attributes(persist_model, sql))]
pub fn derive_persist_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_persist_model(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn persistent(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    match expand_persistent_attr(attr.into(), input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn command(attr: TokenStream, item: TokenStream) -> TokenStream {
    let marker = match parse_command_attr_tokens(attr.into()) {
        Ok(marker) => marker,
        Err(err) => return err.to_compile_error().into(),
    };

    let marker_value = marker
        .name
        .as_ref()
        .map(|name| format!("__rustmemodb_command:{name}"))
        .unwrap_or_else(|| "__rustmemodb_command".to_string());

    if let Ok(mut method) = syn::parse::<ImplItemFn>(item.clone()) {
        method
            .attrs
            .push(syn::parse_quote!(#[doc = #marker_value]));
        return quote!(#method).into();
    }

    if let Ok(mut func) = syn::parse::<ItemFn>(item.clone()) {
        func.attrs.push(syn::parse_quote!(#[doc = #marker_value]));
        return quote!(#func).into();
    }

    syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[command] can only be applied to functions or impl methods",
    )
    .to_compile_error()
    .into()
}

#[proc_macro_attribute]
pub fn persistent_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[persistent_impl] does not accept arguments",
        )
        .to_compile_error()
        .into();
    }

    let input = parse_macro_input!(item as ItemImpl);
    match expand_persistent_impl_attr(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_persistent_attr(attr: TokenStream2, item_struct: ItemStruct) -> syn::Result<TokenStream2> {
    let options = parse_persistent_attr_options(attr)?;
    let has_derive = has_derive_trait(&item_struct.attrs, "PersistModel");
    let has_persist_model_attr = item_struct
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("persist_model"));

    if has_persist_model_attr && (options.table_name.is_some() || options.schema_version.is_some()) {
        return Err(syn::Error::new(
            item_struct.span(),
            "#[persistent(...)] options conflict with existing #[persist_model(...)] attribute",
        ));
    }

    let mut injected = Vec::new();
    if !has_derive {
        injected.push(quote!(#[derive(::rustmemodb::PersistModel)]));
    }

    if !has_persist_model_attr && (options.table_name.is_some() || options.schema_version.is_some())
    {
        let table_part = options.table_name.as_ref().map(|table| {
            quote!(table = #table)
        });
        let schema_part = options.schema_version.map(|version| {
            quote!(schema_version = #version)
        });

        let persist_model_attr = match (table_part, schema_part) {
            (Some(table), Some(schema)) => quote!(#[persist_model(#table, #schema)]),
            (Some(table), None) => quote!(#[persist_model(#table)]),
            (None, Some(schema)) => quote!(#[persist_model(#schema)]),
            (None, None) => quote!(),
        };
        if !persist_model_attr.is_empty() {
            injected.push(persist_model_attr);
        }
    }

    Ok(quote! {
        #(#injected)*
        #item_struct
    })
}

fn expand_persistent_impl_attr(mut item_impl: ItemImpl) -> syn::Result<TokenStream2> {
    if item_impl.trait_.is_some() {
        return Err(syn::Error::new(
            item_impl.span(),
            "#[persistent_impl] can only be used on inherent impl blocks",
        ));
    }

    let model_ident = extract_impl_self_type_ident(&item_impl.self_ty)?;
    let persisted_ident = format_ident!("{}Persisted", model_ident);
    let command_enum_ident = format_ident!("{}PersistentCommand", model_ident);

    let mut commands = Vec::<PersistentCommandMethod>::new();
    for item in &mut item_impl.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };

        let marker = extract_command_marker(&mut method.attrs)?;
        let Some(marker) = marker else {
            continue;
        };

        commands.push(PersistentCommandMethod::from_impl_method(method, marker)?);
    }

    if commands.is_empty() {
        return Ok(quote!(#item_impl));
    }

    let enum_variants = commands.iter().map(|cmd| {
        let variant = &cmd.variant_ident;
        if cmd.args.is_empty() {
            quote!(#variant)
        } else {
            let args = cmd.args.iter().map(|arg| {
                let ident = &arg.ident;
                let ty = &arg.ty;
                quote!(#ident: #ty)
            });
            quote!(#variant { #(#args),* })
        }
    });

    let enum_name_arms = commands.iter().map(|cmd| {
        let variant = &cmd.variant_ident;
        let label = cmd.command_name.as_str();
        if cmd.args.is_empty() {
            quote!(Self::#variant => #label)
        } else {
            quote!(Self::#variant { .. } => #label)
        }
    });

    let payload_arms = commands.iter().map(|cmd| {
        let variant = &cmd.variant_ident;
        let arg_idents = cmd
            .args
            .iter()
            .map(|arg| arg.ident.clone())
            .collect::<Vec<_>>();
        let payload_inserts = cmd.args.iter().map(|arg| {
            let field_name = arg.ident.to_string();
            let ident = &arg.ident;
            quote! {
                payload.insert(
                    #field_name.to_string(),
                    serde_json::to_value(#ident)
                        .map_err(|err| ::rustmemodb::persist::serde_to_db_error("serialize command payload", err))?,
                );
            }
        });

        if arg_idents.is_empty() {
            quote! {
                Self::#variant => {
                    Ok(serde_json::Value::Object(serde_json::Map::new()))
                }
            }
        } else {
            quote! {
                Self::#variant { #(#arg_idents),* } => {
                    let mut payload = serde_json::Map::new();
                    #(#payload_inserts)*
                    Ok(serde_json::Value::Object(payload))
                }
            }
        }
    });

    let schema_arms = commands.iter().map(|cmd| {
        let variant = &cmd.variant_ident;
        let schema_expr = build_runtime_payload_schema_expr(&cmd.args);
        if cmd.args.is_empty() {
            quote! {
                Self::#variant => #schema_expr
            }
        } else {
            quote! {
                Self::#variant { .. } => #schema_expr
            }
        }
    });

    let schema_by_name_arms = commands.iter().map(|cmd| {
        let command_name = cmd.command_name.as_str();
        let schema_expr = build_runtime_payload_schema_expr(&cmd.args);
        quote! {
            #command_name => Some(#schema_expr)
        }
    });

    let command_match_arms = commands.iter().map(|cmd| {
        let variant = &cmd.variant_ident;
        let method_ident = &cmd.method_ident;
        let args = cmd.args.iter().map(|arg| arg.ident.clone()).collect::<Vec<_>>();
        let pattern = if args.is_empty() {
            quote!(#command_enum_ident::#variant)
        } else {
            quote!(#command_enum_ident::#variant { #(#args),* })
        };

        let method_call = quote!(self.data.#method_ident(#(#args),*));
        let body = cmd.return_kind.build_command_body(method_call);
        quote! {
            #pattern => {
                #body
            }
        }
    });

    let command_contract_entries = commands.iter().map(|cmd| {
        let command_name = cmd.command_name.as_str();
        let field_entries = cmd.args.iter().map(|arg| {
            let field_name = arg.ident.to_string();
            let ty = &arg.ty;
            quote! {
                ::rustmemodb::persist::PersistCommandFieldContract {
                    name: #field_name.to_string(),
                    rust_type: stringify!(#ty).to_string(),
                    optional: false,
                }
            }
        });

        quote! {
            ::rustmemodb::persist::PersistCommandContract {
                name: #command_name.to_string(),
                fields: vec![#(#field_entries),*],
                mutates_state: true,
            }
        }
    });

    let runtime_registration_entries = commands.iter().map(|cmd| {
        let command_name = cmd.command_name.as_str();
        let variant = &cmd.variant_ident;
        let schema_expr = build_runtime_payload_schema_expr(&cmd.args);

        let deserialize_args = cmd.args.iter().map(|arg| {
            let field_name = arg.ident.to_string();
            let ident = &arg.ident;
            let ty = &arg.ty;
            quote! {
                let #ident: #ty = serde_json::from_value(
                    payload_obj
                        .get(#field_name)
                        .cloned()
                        .ok_or_else(|| ::rustmemodb::DbError::ExecutionError(
                            format!(
                                "Missing payload field '{}' for command '{}'",
                                #field_name,
                                #command_name,
                            )
                        ))?,
                )
                .map_err(|err| ::rustmemodb::persist::serde_to_db_error(
                    "deserialize command payload",
                    err,
                ))?;
            }
        });

        let command_expr = if cmd.args.is_empty() {
            quote!(#command_enum_ident::#variant)
        } else {
            let fields = cmd.args.iter().map(|arg| {
                let ident = &arg.ident;
                quote!(#ident)
            });
            quote!(#command_enum_ident::#variant { #(#fields),* })
        };

        quote! {
            runtime.register_deterministic_context_command_with_schema(
                stringify!(#model_ident),
                #command_name,
                #schema_expr,
                ::std::sync::Arc::new(|state, payload, _ctx| {
                    let payload_obj = payload.as_object().ok_or_else(|| {
                        ::rustmemodb::DbError::ExecutionError(
                            "Command payload must be a JSON object".to_string(),
                        )
                    })?;
                    #(#deserialize_args)*
                    let mut entity =
                        <#persisted_ident as ::rustmemodb::PersistEntityFactory>::from_state(state)?;
                    let _ = entity.apply_domain_command(#command_expr)?;
                    state.fields = entity.state_json();
                    state.metadata.version = state.metadata.version.saturating_add(1);
                    Ok(Vec::new())
                }),
            );
        }
    });

    Ok(quote! {
        #item_impl

        pub enum #command_enum_ident {
            #(#enum_variants),*
        }

        impl #command_enum_ident {
            pub fn name(&self) -> &'static str {
                match self {
                    #(#enum_name_arms),*
                }
            }

            pub fn payload_json(&self) -> ::rustmemodb::Result<serde_json::Value> {
                match self {
                    #(#payload_arms),*
                }
            }

            pub fn runtime_payload_schema(&self) -> ::rustmemodb::RuntimeCommandPayloadSchema {
                match self {
                    #(#schema_arms),*
                }
            }

            pub fn runtime_payload_schema_by_name(
                command_name: &str,
            ) -> Option<::rustmemodb::RuntimeCommandPayloadSchema> {
                match command_name {
                    #(#schema_by_name_arms),*,
                    _ => None,
                }
            }

            pub fn to_runtime_envelope(
                &self,
                entity_id: impl Into<String>,
            ) -> ::rustmemodb::Result<::rustmemodb::RuntimeCommandEnvelope> {
                Ok(::rustmemodb::RuntimeCommandEnvelope::new(
                    stringify!(#model_ident),
                    entity_id.into(),
                    self.name(),
                    self.payload_json()?,
                ))
            }
        }

        impl #persisted_ident {
            pub fn apply_domain_command(
                &mut self,
                command: #command_enum_ident,
            ) -> ::rustmemodb::Result<serde_json::Value> {
                match command {
                    #(#command_match_arms),*
                }
            }

            pub async fn apply_domain_command_persisted(
                &mut self,
                command: #command_enum_ident,
            ) -> ::rustmemodb::Result<serde_json::Value> {
                let result = self.apply_domain_command(command)?;
                self.save_bound().await?;
                Ok(result)
            }

            pub fn domain_command_envelope(
                &self,
                command: &#command_enum_ident,
            ) -> ::rustmemodb::Result<::rustmemodb::RuntimeCommandEnvelope> {
                command.to_runtime_envelope(self.persist_id())
            }

            pub fn domain_command_envelope_with_expected_version(
                &self,
                command: &#command_enum_ident,
            ) -> ::rustmemodb::Result<::rustmemodb::RuntimeCommandEnvelope> {
                let expected_version = self.metadata().version.max(0) as u64;
                Ok(self
                    .domain_command_envelope(command)?
                    .with_expected_version(expected_version))
            }

            pub fn domain_command_contract() -> Vec<::rustmemodb::persist::PersistCommandContract> {
                vec![#(#command_contract_entries),*]
            }

            pub fn register_domain_commands_in_runtime(
                runtime: &mut ::rustmemodb::PersistEntityRuntime,
            ) {
                Self::try_register_domain_commands_in_runtime(runtime).expect(
                    "failed to register domain commands and projection contract in runtime",
                );
            }

            pub fn try_register_domain_commands_in_runtime(
                runtime: &mut ::rustmemodb::PersistEntityRuntime,
            ) -> ::rustmemodb::Result<()> {
                Self::register_projection_in_runtime(runtime)?;
                #(#runtime_registration_entries)*
                Ok(())
            }
        }
    })
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
    let mut field_sql_options = Vec::<Option<SqlFieldOptions>>::new();

    for field in named_fields.named {
        let ident = field.ident.clone().ok_or_else(|| {
            syn::Error::new(field.span(), "PersistModel requires named fields")
        })?;
        let sql_options = parse_sql_field_options(&field.attrs)?;
        field_idents.push(ident);
        field_types.push(field.ty);
        field_sql_options.push(sql_options);
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
    let has_explicit_projection_attrs = field_sql_options.iter().any(|options| options.is_some());

    let mut projection_contract_fields = Vec::<TokenStream2>::new();
    let mut projection_index_helpers = Vec::<TokenStream2>::new();
    for ((field_ident, field_ty), field_sql) in field_idents
        .iter()
        .zip(field_types.iter())
        .zip(field_sql_options.iter())
    {
        let include = match field_sql {
            Some(options) => options.include,
            None => !has_explicit_projection_attrs,
        };
        if !include {
            continue;
        }

        let indexed = field_sql.as_ref().map(|options| options.indexed).unwrap_or(false);
        let state_field_name = field_ident.to_string();
        let column_name = field_sql
            .as_ref()
            .and_then(|options| options.column_name.clone())
            .unwrap_or_else(|| state_field_name.clone());
        let payload_type = runtime_payload_type_tokens(field_ty);
        projection_contract_fields.push(quote! {
            ::rustmemodb::RuntimeProjectionField::new(
                #state_field_name,
                #column_name,
                #payload_type,
            ).indexed(#indexed)
        });

        if indexed {
            let helper_suffix = state_field_name.trim_start_matches("r#").to_string();
            let rows_helper_name = format_ident!("find_projection_rows_by_{}", helper_suffix);
            let ids_helper_name = format_ident!("find_projection_ids_by_{}", helper_suffix);
            projection_index_helpers.push(quote! {
                pub fn #rows_helper_name(
                    runtime: &::rustmemodb::PersistEntityRuntime,
                    value: #field_ty,
                ) -> ::rustmemodb::Result<Vec<::rustmemodb::RuntimeProjectionRow>> {
                    let json_value = serde_json::to_value(value)
                        .map_err(|err| ::rustmemodb::persist::serde_to_db_error("serialize projection index value", err))?;
                    runtime.find_projection_rows_by_index(
                        stringify!(#struct_name),
                        #column_name,
                        &json_value,
                    )
                }

                pub fn #ids_helper_name(
                    runtime: &::rustmemodb::PersistEntityRuntime,
                    value: #field_ty,
                ) -> ::rustmemodb::Result<Vec<String>> {
                    let json_value = serde_json::to_value(value)
                        .map_err(|err| ::rustmemodb::persist::serde_to_db_error("serialize projection index value", err))?;
                    runtime.find_projection_entity_ids_by_index(
                        stringify!(#struct_name),
                        #column_name,
                        &json_value,
                    )
                }
            });
        }
    }

    let projection_methods = if projection_contract_fields.is_empty() {
        quote! {
            pub fn projection_contract() -> Option<::rustmemodb::RuntimeProjectionContract> {
                None
            }

            pub fn register_projection_in_runtime(
                _runtime: &mut ::rustmemodb::PersistEntityRuntime,
            ) -> ::rustmemodb::Result<()> {
                Ok(())
            }
        }
    } else {
        quote! {
            pub fn projection_contract() -> Option<::rustmemodb::RuntimeProjectionContract> {
                let table_name = format!("{}_projection", Self::default_table_name());
                let mut contract = ::rustmemodb::RuntimeProjectionContract::new(
                    stringify!(#struct_name),
                    table_name,
                )
                .with_schema_version(#schema_version_literal);
                #( contract = contract.with_field(#projection_contract_fields); )*
                Some(contract)
            }

            pub fn register_projection_in_runtime(
                runtime: &mut ::rustmemodb::PersistEntityRuntime,
            ) -> ::rustmemodb::Result<()> {
                if let Some(contract) = Self::projection_contract() {
                    runtime.register_projection_contract(contract)?;
                }
                Ok(())
            }

            #( #projection_index_helpers )*
        }
    };

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

            #projection_methods

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

#[derive(Clone)]
struct SqlFieldOptions {
    include: bool,
    indexed: bool,
    column_name: Option<String>,
}

impl Default for SqlFieldOptions {
    fn default() -> Self {
        Self {
            include: true,
            indexed: false,
            column_name: None,
        }
    }
}

struct PersistentAttrOptions {
    table_name: Option<LitStr>,
    schema_version: Option<u32>,
}

#[derive(Clone)]
struct CommandAttrOptions {
    name: Option<String>,
}

struct PersistentCommandArg {
    ident: Ident,
    ty: Type,
}

enum PersistentMethodReturnKind {
    Unit,
    Plain(Type),
    RustResult(Type),
}

impl PersistentMethodReturnKind {
    fn from_signature(signature: &syn::Signature) -> Self {
        match &signature.output {
            ReturnType::Default => Self::Unit,
            ReturnType::Type(_, ty) => {
                if let Some(ok_ty) = extract_result_ok_type(ty) {
                    return Self::RustResult(ok_ty);
                }
                Self::Plain((**ty).clone())
            }
        }
    }

    fn build_command_body(&self, method_call: TokenStream2) -> TokenStream2 {
        match self {
            Self::Unit => quote! {
                #method_call;
                self.mark_all_dirty();
                Ok(serde_json::Value::Null)
            },
            Self::Plain(ty) => quote! {
                let output: #ty = #method_call;
                self.mark_all_dirty();
                let json = serde_json::to_value(&output)
                    .map_err(|err| ::rustmemodb::persist::serde_to_db_error("serialize command output", err))?;
                Ok(json)
            },
            Self::RustResult(ok_ty) => quote! {
                let output: #ok_ty = #method_call?;
                self.mark_all_dirty();
                let json = serde_json::to_value(&output)
                    .map_err(|err| ::rustmemodb::persist::serde_to_db_error("serialize command output", err))?;
                Ok(json)
            },
        }
    }
}

struct PersistentCommandMethod {
    method_ident: Ident,
    variant_ident: Ident,
    command_name: String,
    args: Vec<PersistentCommandArg>,
    return_kind: PersistentMethodReturnKind,
}

impl PersistentCommandMethod {
    fn from_impl_method(method: &ImplItemFn, marker: CommandAttrOptions) -> syn::Result<Self> {
        if method.sig.asyncness.is_some() {
            return Err(syn::Error::new(
                method.sig.span(),
                "#[command] methods in #[persistent_impl] must be synchronous",
            ));
        }
        if !method.sig.generics.params.is_empty() {
            return Err(syn::Error::new(
                method.sig.generics.span(),
                "#[command] methods in #[persistent_impl] cannot have generic parameters",
            ));
        }

        let mut inputs_iter = method.sig.inputs.iter();
        let receiver = inputs_iter.next().ok_or_else(|| {
            syn::Error::new(
                method.sig.span(),
                "#[command] method must have &mut self receiver",
            )
        })?;

        match receiver {
            FnArg::Receiver(receiver) if receiver.reference.is_some() && receiver.mutability.is_some() => {}
            _ => {
                return Err(syn::Error::new(
                    receiver.span(),
                    "#[command] method receiver must be `&mut self`",
                ));
            }
        }

        let mut args = Vec::new();
        for input in inputs_iter {
            let FnArg::Typed(PatType { pat, ty, .. }) = input else {
                return Err(syn::Error::new(
                    input.span(),
                    "Unsupported #[command] argument pattern",
                ));
            };

            let Pat::Ident(pat_ident) = pat.as_ref() else {
                return Err(syn::Error::new(
                    pat.span(),
                    "#[command] arguments must be simple identifiers",
                ));
            };

            args.push(PersistentCommandArg {
                ident: pat_ident.ident.clone(),
                ty: (**ty).clone(),
            });
        }

        let method_name = method.sig.ident.to_string();
        let command_name = marker.name.unwrap_or_else(|| method_name.clone());
        let variant_ident = format_ident!("{}", to_pascal_case(&method_name));

        Ok(Self {
            method_ident: method.sig.ident.clone(),
            variant_ident,
            command_name,
            args,
            return_kind: PersistentMethodReturnKind::from_signature(&method.sig),
        })
    }
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

fn has_derive_trait(attrs: &[syn::Attribute], trait_name: &str) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("derive") {
            continue;
        }

        if let Ok(paths) = attr.parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
        {
            if paths
                .iter()
                .any(|path| path.segments.last().map(|segment| segment.ident == trait_name).unwrap_or(false))
            {
                return true;
            }
        }
    }

    false
}

fn parse_persistent_attr_options(attr: TokenStream2) -> syn::Result<PersistentAttrOptions> {
    let mut options = PersistentAttrOptions {
        table_name: None,
        schema_version: None,
    };

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("table") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            options.table_name = Some(lit);
            return Ok(());
        }

        if meta.path.is_ident("schema_version") {
            let value = meta.value()?;
            let lit: syn::LitInt = value.parse()?;
            options.schema_version = Some(lit.base10_parse::<u32>()?);
            return Ok(());
        }

        Err(meta.error(
            "Unsupported #[persistent(...)] option. Supported: table = \"...\", schema_version = <u32>",
        ))
    });

    parser.parse2(attr)?;
    Ok(options)
}

fn parse_command_attr_tokens(attr: TokenStream2) -> syn::Result<CommandAttrOptions> {
    let mut options = CommandAttrOptions { name: None };
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            options.name = Some(lit.value());
            return Ok(());
        }
        Err(meta.error("Unsupported #[command(...)] option. Supported: name = \"...\""))
    });

    parser.parse2(attr)?;
    Ok(options)
}

fn parse_command_doc_marker(value: &str) -> Option<CommandAttrOptions> {
    const MARKER: &str = "__rustmemodb_command";
    if value == MARKER {
        return Some(CommandAttrOptions { name: None });
    }
    value
        .strip_prefix("__rustmemodb_command:")
        .map(|name| CommandAttrOptions {
            name: if name.trim().is_empty() {
                None
            } else {
                Some(name.to_string())
            },
        })
}

fn extract_command_marker(attrs: &mut Vec<syn::Attribute>) -> syn::Result<Option<CommandAttrOptions>> {
    let mut found: Option<CommandAttrOptions> = None;
    let mut kept = Vec::with_capacity(attrs.len());

    for attr in attrs.drain(..) {
        if path_ends_with_ident(attr.path(), "command") {
            let parsed = parse_command_attr_tokens(
                attr.meta
                    .require_list()
                    .map(|list| list.tokens.clone())
                    .unwrap_or_default(),
            )?;
            if found.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "Duplicate #[command] marker on method",
                ));
            }
            found = Some(parsed);
            continue;
        }

        if attr.path().is_ident("doc") {
            if let Ok(marker) = attr.parse_args::<LitStr>() {
                if let Some(parsed) = parse_command_doc_marker(&marker.value()) {
                    if found.is_some() {
                        return Err(syn::Error::new(
                            attr.span(),
                            "Duplicate command marker on method",
                        ));
                    }
                    found = Some(parsed);
                    continue;
                }
            }
        }

        kept.push(attr);
    }

    *attrs = kept;
    Ok(found)
}

fn extract_impl_self_type_ident(self_ty: &Type) -> syn::Result<Ident> {
    let Type::Path(TypePath { qself: None, path }) = self_ty else {
        return Err(syn::Error::new(
            self_ty.span(),
            "#[persistent_impl] requires a concrete struct type",
        ));
    };

    let Some(segment) = path.segments.last() else {
        return Err(syn::Error::new(
            self_ty.span(),
            "Unable to extract impl self type identifier",
        ));
    };

    Ok(segment.ident.clone())
}

fn path_ends_with_ident(path: &syn::Path, ident: &str) -> bool {
    path.segments
        .last()
        .map(|segment| segment.ident == ident)
        .unwrap_or(false)
}

fn build_runtime_payload_schema_expr(args: &[PersistentCommandArg]) -> TokenStream2 {
    let mut expr = quote!(::rustmemodb::RuntimeCommandPayloadSchema::object());
    for arg in args {
        let field_name = arg.ident.to_string();
        let payload_type = runtime_payload_type_tokens(&arg.ty);
        expr = quote!(#expr.require_field(#field_name, #payload_type));
    }
    quote!(#expr.allow_extra_fields(false))
}

fn runtime_payload_type_tokens(ty: &Type) -> TokenStream2 {
    match ty {
        Type::Reference(reference) => {
            return runtime_payload_type_tokens(reference.elem.as_ref());
        }
        Type::Slice(_) | Type::Array(_) => {
            return quote!(::rustmemodb::RuntimePayloadType::Array);
        }
        Type::Path(path) => {
            if let Some(segment) = path.path.segments.last() {
                let ident = segment.ident.to_string();
                match ident.as_str() {
                    "bool" => return quote!(::rustmemodb::RuntimePayloadType::Boolean),
                    "String" | "str" => return quote!(::rustmemodb::RuntimePayloadType::Text),
                    "f32" | "f64" => return quote!(::rustmemodb::RuntimePayloadType::Float),
                    "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32"
                    | "u64" | "u128" | "usize" => {
                        return quote!(::rustmemodb::RuntimePayloadType::Integer);
                    }
                    "Vec" => return quote!(::rustmemodb::RuntimePayloadType::Array),
                    "HashMap" | "BTreeMap" => {
                        return quote!(::rustmemodb::RuntimePayloadType::Object);
                    }
                    "Option" => {
                        if let Some(inner_ty) = first_generic_type(segment) {
                            return runtime_payload_type_tokens(&inner_ty);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }

    quote!(::rustmemodb::RuntimePayloadType::Object)
}

fn first_generic_type(segment: &syn::PathSegment) -> Option<Type> {
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };

    for arg in &arguments.args {
        if let syn::GenericArgument::Type(ty) = arg {
            return Some(ty.clone());
        }
    }
    None
}

fn extract_result_ok_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    for arg in &arguments.args {
        if let syn::GenericArgument::Type(ok_ty) = arg {
            return Some(ok_ty.clone());
        }
    }
    None
}

fn parse_sql_field_options(attrs: &[syn::Attribute]) -> syn::Result<Option<SqlFieldOptions>> {
    let mut options: Option<SqlFieldOptions> = None;

    for attr in attrs {
        if !path_ends_with_ident(attr.path(), "sql") {
            continue;
        }

        if options.is_some() {
            return Err(syn::Error::new(
                attr.span(),
                "Duplicate #[sql(...)] attribute on field",
            ));
        }

        let mut parsed = SqlFieldOptions::default();
        match &attr.meta {
            syn::Meta::Path(_) => {}
            syn::Meta::List(list) => {
                list.parse_nested_meta(|meta| {
                    if meta.path.is_ident("index") {
                        parsed.indexed = true;
                        return Ok(());
                    }

                    if meta.path.is_ident("skip") {
                        parsed.include = false;
                        return Ok(());
                    }

                    if meta.path.is_ident("name") || meta.path.is_ident("column") {
                        let value = meta.value()?;
                        let lit: LitStr = value.parse()?;
                        parsed.column_name = Some(lit.value());
                        return Ok(());
                    }

                    Err(meta.error(
                        "Unsupported #[sql(...)] option. Supported: index, skip, name = \"...\", column = \"...\"",
                    ))
                })?;
            }
            syn::Meta::NameValue(_) => {
                return Err(syn::Error::new(
                    attr.span(),
                    "Unsupported #[sql = ...] syntax. Use #[sql], #[sql(index)], #[sql(skip)], #[sql(name = \"...\")]",
                ));
            }
        }

        if !parsed.include && parsed.indexed {
            return Err(syn::Error::new(
                attr.span(),
                "#[sql(skip)] cannot be combined with #[sql(index)]",
            ));
        }

        if !parsed.include && parsed.column_name.is_some() {
            return Err(syn::Error::new(
                attr.span(),
                "#[sql(skip)] cannot define a custom column name",
            ));
        }

        options = Some(parsed);
    }

    Ok(options)
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
