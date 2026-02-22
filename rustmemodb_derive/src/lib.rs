mod api_service_impl;

use api_service_impl::expand_api_service;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    Data, DeriveInput, Fields, FnArg, Ident, ImplItem, ImplItemFn, ItemFn, ItemImpl, ItemStruct,
    ItemTrait, LitStr, Pat, PatType, ReturnType, Token, Type, TypePath, parse_macro_input,
    spanned::Spanned,
};

#[proc_macro_attribute]
pub fn api_service(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemTrait);
    match expand_api_service(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

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

    let marker_value = build_command_doc_marker(&marker);

    if let Ok(mut method) = syn::parse::<ImplItemFn>(item.clone()) {
        method.attrs.push(syn::parse_quote!(#[doc = #marker_value]));
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
pub fn view(attr: TokenStream, item: TokenStream) -> TokenStream {
    let marker = match parse_view_attr_tokens(attr.into()) {
        Ok(marker) => marker,
        Err(err) => return err.to_compile_error().into(),
    };

    let marker_value = build_view_like_doc_marker("__rustmemodb_view", &marker);

    if let Ok(mut method) = syn::parse::<ImplItemFn>(item.clone()) {
        method.attrs.push(syn::parse_quote!(#[doc = #marker_value]));
        return quote!(#method).into();
    }

    if let Ok(mut func) = syn::parse::<ItemFn>(item.clone()) {
        func.attrs.push(syn::parse_quote!(#[doc = #marker_value]));
        return quote!(#func).into();
    }

    syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[view] can only be applied to functions or impl methods",
    )
    .to_compile_error()
    .into()
}

#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    let marker = match parse_query_attr_tokens(attr.into()) {
        Ok(marker) => marker,
        Err(err) => return err.to_compile_error().into(),
    };

    let marker_value = build_view_like_doc_marker("__rustmemodb_query", &marker);

    if let Ok(mut method) = syn::parse::<ImplItemFn>(item.clone()) {
        method.attrs.push(syn::parse_quote!(#[doc = #marker_value]));
        return quote!(#method).into();
    }

    if let Ok(mut func) = syn::parse::<ItemFn>(item.clone()) {
        func.attrs.push(syn::parse_quote!(#[doc = #marker_value]));
        return quote!(#func).into();
    }

    syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[query] can only be applied to functions or impl methods",
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

#[proc_macro_attribute]
pub fn autonomous_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[autonomous_impl] does not accept arguments",
        )
        .to_compile_error()
        .into();
    }

    let input = parse_macro_input!(item as ItemImpl);
    match expand_autonomous_impl_attr(input, false) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn expose_rest(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[expose_rest] does not accept arguments",
        )
        .to_compile_error()
        .into();
    }

    let input = parse_macro_input!(item as ItemImpl);
    match expand_autonomous_impl_attr(input, true) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(PersistAutonomousIntent, attributes(persist_intent, persist_case))]
pub fn derive_persist_autonomous_intent(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_persist_autonomous_intent(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(Autonomous, attributes(persist_model, sql))]
pub fn derive_autonomous(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_autonomous(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(PersistJsonValue)]
pub fn derive_persist_json_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_persist_json_value(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(ApiError, attributes(api_error))]
pub fn derive_api_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_api_error(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_persistent_attr(
    attr: TokenStream2,
    item_struct: ItemStruct,
) -> syn::Result<TokenStream2> {
    let options = parse_persistent_attr_options(attr)?;
    let has_derive = has_derive_trait(&item_struct.attrs, "PersistModel");
    let has_persist_model_attr = item_struct
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("persist_model"));

    if has_persist_model_attr && (options.table_name.is_some() || options.schema_version.is_some())
    {
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
        let table_part = options
            .table_name
            .as_ref()
            .map(|table| quote!(table = #table));
        let schema_part = options
            .schema_version
            .map(|version| quote!(schema_version = #version));

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
        let args = cmd
            .args
            .iter()
            .map(|arg| arg.ident.clone())
            .collect::<Vec<_>>();
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

fn expand_autonomous_impl_attr(
    mut item_impl: ItemImpl,
    expose_rest: bool,
) -> syn::Result<TokenStream2> {
    if item_impl.trait_.is_some() {
        return Err(syn::Error::new(
            item_impl.span(),
            "#[autonomous_impl] can only be used on inherent impl blocks",
        ));
    }

    let model_ident = extract_impl_self_type_ident(&item_impl.self_ty)?;
    let trait_ident = format_ident!("{}AutonomousOps", model_ident);
    let rest_ext_trait_ident = format_ident!("{}AutonomousRestExt", model_ident);

    let mut methods = Vec::<AutonomousExposedMethod>::new();
    let mut views = Vec::<AutonomousViewMethod>::new();
    let mut constructor_args: Option<Vec<PersistentCommandArg>> = None;
    for item in &mut item_impl.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };

        if constructor_args.is_none() {
            constructor_args = parse_autonomous_constructor_args(method, &model_ident)?;
        }

        let command_marker = extract_command_marker(&mut method.attrs)?;
        let view_marker = extract_view_marker(&mut method.attrs)?;

        if command_marker.is_some() && view_marker.is_some() {
            return Err(syn::Error::new(
                method.sig.span(),
                "Method cannot be both #[command] and #[view]/#[query]",
            ));
        }

        if let Some(marker) = command_marker {
            methods.push(AutonomousExposedMethod::from_impl_method(method, marker)?);
            continue;
        }

        if let Some(marker) = view_marker {
            if !expose_rest {
                return Err(syn::Error::new(
                    method.sig.span(),
                    "#[view] requires #[expose_rest] on the impl block",
                ));
            }
            views.push(AutonomousViewMethod::from_impl_method(method, marker)?);
        }
    }

    if methods.is_empty() && views.is_empty() {
        return Ok(quote!(#item_impl));
    }

    let command_trait_tokens = if methods.is_empty() {
        quote! {}
    } else {
        let trait_methods = methods.iter().map(|method| method.trait_method_tokens());
        let impl_methods = methods
            .iter()
            .map(|method| method.impl_method_tokens(&model_ident));
        quote! {
            pub trait #trait_ident {
                #(#trait_methods)*
            }

            impl #trait_ident for ::rustmemodb::PersistAutonomousModelHandle<#model_ident> {
                #(#impl_methods)*
            }
        }
    };

    let rest_tokens = if expose_rest {
        generate_autonomous_rest_tokens(
            &model_ident,
            &rest_ext_trait_ident,
            &methods,
            &views,
            constructor_args.as_deref(),
        )?
    } else {
        quote! {}
    };

    Ok(quote! {
        #item_impl

        #command_trait_tokens
        #rest_tokens
    })
}

fn generate_autonomous_rest_tokens(
    model_ident: &Ident,
    rest_ext_trait_ident: &Ident,
    methods: &[AutonomousExposedMethod],
    views: &[AutonomousViewMethod],
    constructor_args: Option<&[PersistentCommandArg]>,
) -> syn::Result<TokenStream2> {
    use std::collections::HashSet;

    let mut routes = HashSet::<String>::new();
    for method in methods {
        let route = method.route_literal();
        if !routes.insert(route.clone()) {
            return Err(syn::Error::new(
                method.method_ident.span(),
                format!("Duplicate REST route segment generated: {route}"),
            ));
        }
    }
    for view in views {
        let route = view.route_literal();
        if !routes.insert(route.clone()) {
            return Err(syn::Error::new(
                view.method_ident.span(),
                format!("Duplicate REST route segment generated: {route}"),
            ));
        }
    }

    let server_ident = format_ident!("{}AutonomousRestServer", model_ident);
    let command_request_structs = methods
        .iter()
        .filter_map(|method| method.request_struct_tokens(model_ident));
    let view_request_structs = views
        .iter()
        .filter_map(|view| view.request_struct_tokens(model_ident));
    let create_request_ident = format_ident!("{}CreateRequest", model_ident);
    let create_request_type_name = if constructor_args.is_some() {
        create_request_ident.to_string()
    } else {
        model_ident.to_string()
    };
    let create_request_struct = constructor_args.map(|args| {
        let fields = args.iter().map(|arg| {
            let ident = &arg.ident;
            let ty = &arg.ty;
            quote!(pub #ident: #ty)
        });
        quote! {
            #[derive(::serde::Deserialize, ::serde::Serialize, ::core::fmt::Debug, ::core::clone::Clone)]
            pub struct #create_request_ident {
                #( #fields, )*
            }
        }
    });
    let command_routes = methods.iter().map(|method| {
        let route = method.route_literal();
        let handler_ident = format_ident!("handle_command_{}", method.method_ident);
        quote! {
            .route(concat!("/:id/", #route), axum::routing::post(Self::#handler_ident))
        }
    });
    let command_handlers = methods
        .iter()
        .map(|method| method.command_handler_tokens(model_ident));
    let view_routes = views.iter().map(|view| {
        let route = view.route_literal();
        let handler_ident = format_ident!("handle_view_{}", view.method_ident);
        let routing_method = match view.input_mode {
            ViewInputMode::Query => quote!(get),
            ViewInputMode::Body => quote!(post),
        };
        quote! {
            .route(concat!("/:id/", #route), axum::routing::#routing_method(Self::#handler_ident))
        }
    });
    let view_handlers = views
        .iter()
        .map(|view| view.view_handler_tokens(model_ident));
    let model_name = model_ident.to_string();
    let record_type_name = format!("PersistAutonomousRecord<{}>", model_name);
    let list_record_type_name = format!("Vec<{}>", record_type_name);
    let command_openapi_ops = methods.iter().map(|method| {
        let path = format!("/{{id}}/{}", method.route_literal());
        let operation_id = format!("{}_{}", to_snake_case(&model_name), method.method_ident);
        let summary = format!("{} command", method.method_ident);
        let request_type = method.openapi_request_rust_type_literal(model_ident);
        let request_type_tokens = if let Some(request_type) = request_type {
            quote!(Some(#request_type))
        } else {
            quote!(None)
        };
        let response_type = method.openapi_response_rust_type_literal();
        let response_type_tokens = if let Some(response_type) = response_type {
            quote!(Some(#response_type))
        } else {
            quote!(None)
        };
        let success_status = method.openapi_success_status_code();
        let idempotent = method.idempotent;
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: "post",
                path: #path,
                operation_id: #operation_id,
                summary: #summary,
                request_rust_type: #request_type_tokens,
                request_location: Some(::rustmemodb::persist::web::PersistOpenApiInputLocation::Body),
                response_rust_type: #response_type_tokens,
                success_status: #success_status,
                idempotent: #idempotent,
            }
        }
    });
    let view_openapi_ops = views.iter().map(|view| {
        let path = format!("/{{id}}/{}", view.route_literal());
        let operation_id = format!("{}_{}", to_snake_case(&model_name), view.method_ident);
        let summary = format!("{} query", view.method_ident);
        let method_lit = match view.input_mode {
            ViewInputMode::Query => "get",
            ViewInputMode::Body => "post",
        };
        let request_type = view.openapi_request_rust_type_literal(model_ident);
        let request_type_tokens = if let Some(request_type) = request_type {
            quote!(Some(#request_type))
        } else {
            quote!(None)
        };
        let request_location_tokens = if view.args.is_empty() {
            quote!(None)
        } else {
            match view.input_mode {
                ViewInputMode::Query => {
                    quote!(Some(
                        ::rustmemodb::persist::web::PersistOpenApiInputLocation::Query
                    ))
                }
                ViewInputMode::Body => {
                    quote!(Some(
                        ::rustmemodb::persist::web::PersistOpenApiInputLocation::Body
                    ))
                }
            }
        };
        let response_type = view.openapi_response_rust_type_literal();
        let response_type_tokens = if let Some(response_type) = response_type {
            quote!(Some(#response_type))
        } else {
            quote!(None)
        };
        let success_status = view.openapi_success_status_code();
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: #method_lit,
                path: #path,
                operation_id: #operation_id,
                summary: #summary,
                request_rust_type: #request_type_tokens,
                request_location: #request_location_tokens,
                response_rust_type: #response_type_tokens,
                success_status: #success_status,
                idempotent: false,
            }
        }
    });
    let openapi_ops = vec![
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: "get",
                path: "/",
                operation_id: "list",
                summary: "List entities",
                request_rust_type: None,
                request_location: None,
                response_rust_type: Some(#list_record_type_name),
                success_status: 200,
                idempotent: false,
            }
        },
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: "post",
                path: "/",
                operation_id: "create",
                summary: "Create entity",
                request_rust_type: Some(#create_request_type_name),
                request_location: Some(::rustmemodb::persist::web::PersistOpenApiInputLocation::Body),
                response_rust_type: Some(#record_type_name),
                success_status: 201,
                idempotent: false,
            }
        },
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: "get",
                path: "/{id}",
                operation_id: "get",
                summary: "Get entity by id",
                request_rust_type: None,
                request_location: None,
                response_rust_type: Some(#record_type_name),
                success_status: 200,
                idempotent: false,
            }
        },
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: "delete",
                path: "/{id}",
                operation_id: "delete",
                summary: "Delete entity by id",
                request_rust_type: None,
                request_location: None,
                response_rust_type: None,
                success_status: 204,
                idempotent: false,
            }
        },
        quote! {
            ::rustmemodb::persist::web::PersistOpenApiOperation {
                method: "get",
                path: "/{id}/_audits",
                operation_id: "audits",
                summary: "List audit trail for entity",
                request_rust_type: None,
                request_location: None,
                response_rust_type: Some("Vec<PersistGeneratedAuditLine>"),
                success_status: 200,
                idempotent: false,
            }
        },
    ];
    let create_handler = if let Some(args) = constructor_args {
        let ctor_fields = args.iter().map(|arg| &arg.ident);
        let ctor_args = args.iter().map(|arg| {
            let ident = &arg.ident;
            quote!(request.#ident)
        });
        quote! {
            async fn handle_create(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                axum::extract::Json(request): axum::extract::Json<#create_request_ident>,
            ) -> axum::response::Response
            where
                #model_ident: ::serde::Serialize,
            {
                let _ = (#(&request.#ctor_fields),*);
                let model = #model_ident::new(#(#ctor_args),*);
                match handle.create_one(model).await {
                    Ok(record) => axum::response::IntoResponse::into_response((
                        axum::http::StatusCode::CREATED,
                        axum::Json(record),
                    )),
                    Err(err) => {
                        let web_err: ::rustmemodb::web::WebError = err.into();
                        axum::response::IntoResponse::into_response(web_err)
                    }
                }
            }
        }
    } else {
        quote! {
            async fn handle_create(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                axum::extract::Json(model): axum::extract::Json<#model_ident>,
            ) -> axum::response::Response
            where
                #model_ident: ::serde::Serialize,
            {
                match handle.create_one(model).await {
                    Ok(record) => axum::response::IntoResponse::into_response((
                        axum::http::StatusCode::CREATED,
                        axum::Json(record),
                    )),
                    Err(err) => {
                        let web_err: ::rustmemodb::web::WebError = err.into();
                        axum::response::IntoResponse::into_response(web_err)
                    }
                }
            }
        }
    };

    Ok(quote! {
        #create_request_struct
        #(#command_request_structs)*
        #(#view_request_structs)*

        pub struct #server_ident {
            handle: ::rustmemodb::PersistAutonomousModelHandle<#model_ident>,
        }

        impl #server_ident {
            pub fn new(handle: ::rustmemodb::PersistAutonomousModelHandle<#model_ident>) -> Self {
                Self { handle }
            }

            pub fn router(self) -> axum::Router {
                axum::Router::new()
                    .route("/", axum::routing::get(Self::handle_list).post(Self::handle_create))
                    .route("/:id", axum::routing::get(Self::handle_get).delete(Self::handle_delete))
                    .route("/:id/_audits", axum::routing::get(Self::handle_audits))
                    .route("/_openapi.json", axum::routing::get(Self::handle_openapi))
                    #(#command_routes)*
                    #(#view_routes)*
                    .with_state(self.handle)
            }

            #create_handler

            async fn handle_list(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
            ) -> axum::response::Response
            where
                #model_ident: ::serde::Serialize,
            {
                let records = handle.list().await;
                axum::response::IntoResponse::into_response((
                    axum::http::StatusCode::OK,
                    axum::Json(records),
                ))
            }

            async fn handle_get(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                axum::extract::Path(id): axum::extract::Path<String>,
            ) -> axum::response::Response
            where
                #model_ident: ::serde::Serialize,
            {
                match handle.get_one(id.as_str()).await {
                    Some(record) => axum::response::IntoResponse::into_response((
                        axum::http::StatusCode::OK,
                        axum::Json(record),
                    )),
                    None => axum::response::IntoResponse::into_response(::rustmemodb::web::WebError::NotFound(format!("entity not found: {}", id))),
                }
            }

            async fn handle_delete(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                axum::extract::Path(id): axum::extract::Path<String>,
            ) -> axum::response::Response {
                match handle.remove_one(id.as_str()).await {
                    Ok(()) => axum::response::IntoResponse::into_response(axum::http::StatusCode::NO_CONTENT),
                    Err(err) => {
                        let web_err: ::rustmemodb::web::WebError = err.into();
                        axum::response::IntoResponse::into_response(web_err)
                    }
                }
            }

            async fn handle_audits(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                axum::extract::Path(id): axum::extract::Path<String>,
            ) -> axum::response::Response {
                #[derive(::serde::Serialize)]
                struct PersistGeneratedAuditLine {
                    aggregate_persist_id: String,
                    event_type: String,
                    message: String,
                    resulting_version: i64,
                }

                if handle.get_one(id.as_str()).await.is_none() {
                    return axum::response::IntoResponse::into_response(
                        ::rustmemodb::web::WebError::NotFound(format!("entity not found: {}", id)),
                    );
                }
                let audits = handle.domain_handle().list_audits_for(id.as_str()).await;
                let lines = audits
                    .into_iter()
                    .map(|event| PersistGeneratedAuditLine {
                        aggregate_persist_id: event.aggregate_persist_id().to_string(),
                        event_type: event.event_type().to_string(),
                        message: event.message().to_string(),
                        resulting_version: *event.resulting_version(),
                    })
                    .collect::<Vec<_>>();
                axum::response::IntoResponse::into_response((
                    axum::http::StatusCode::OK,
                    axum::Json(lines),
                ))
            }

            async fn handle_openapi() -> axum::response::Response {
                let operations = vec![
                    #(#openapi_ops),*,
                    #(#command_openapi_ops),*,
                    #(#view_openapi_ops),*
                ];
                let title = format!("{} Autonomous REST API", stringify!(#model_ident));
                let doc = ::rustmemodb::persist::web::build_autonomous_openapi_document(
                    &title,
                    &operations,
                );
                axum::response::IntoResponse::into_response((
                    axum::http::StatusCode::OK,
                    axum::Json(doc),
                ))
            }

            #(#command_handlers)*
            #(#view_handlers)*
        }

        pub trait #rest_ext_trait_ident {
            fn rest_router(self) -> axum::Router;
        }

        impl #rest_ext_trait_ident for ::rustmemodb::PersistAutonomousModelHandle<#model_ident> {
            fn rest_router(self) -> axum::Router {
                #server_ident::new(self).router()
            }
        }

        impl ::rustmemodb::PersistAutonomousRestModel for #model_ident {
            fn mount_router(handle: ::rustmemodb::PersistAutonomousModelHandle<Self>) -> axum::Router {
                <::rustmemodb::PersistAutonomousModelHandle<#model_ident> as #rest_ext_trait_ident>::rest_router(handle)
            }
        }
    })
}

fn expand_persist_autonomous_intent(input: DeriveInput) -> syn::Result<TokenStream2> {
    let enum_ident = input.ident;
    let enum_data = match input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new(
                enum_ident.span(),
                "#[derive(PersistAutonomousIntent)] can only be used with enums",
            ));
        }
    };

    let options = parse_persist_intent_options(&input.attrs)?;
    let model_ty = options.model.ok_or_else(|| {
        syn::Error::new(
            enum_ident.span(),
            "Missing #[persist_intent(model = <Type>, ...)] option",
        )
    })?;
    let command_ty = quote!(<#model_ty as ::rustmemodb::PersistCommandModel>::Command);

    if let Some(to_command) = options.to_command {
        let event_type_impl = options.event_type.map(|event_type| {
            quote! {
                fn audit_event_type(&self, _command: &#command_ty) -> String {
                    self.clone().#event_type().to_string()
                }
            }
        });
        let event_message_impl = options.event_message.map(|event_message| {
            quote! {
                fn audit_message(&self, _command: &#command_ty) -> String {
                    self.clone().#event_message().to_string()
                }
            }
        });
        let bulk_event_type_impl = options.bulk_event_type.map(|bulk_event_type| {
            quote! {
                fn bulk_audit_event_type(&self, _command: &#command_ty) -> String {
                    self.clone().#bulk_event_type().to_string()
                }
            }
        });
        let bulk_event_message_impl = options.bulk_event_message.map(|bulk_event_message| {
            quote! {
                fn bulk_audit_message(&self, _command: &#command_ty) -> String {
                    self.clone().#bulk_event_message().to_string()
                }
            }
        });

        return Ok(quote! {
            impl ::rustmemodb::PersistAutonomousCommand<#model_ty> for #enum_ident {
                fn to_persist_command(self) -> <#model_ty as ::rustmemodb::PersistCommandModel>::Command {
                    self.#to_command()
                }

                #event_type_impl
                #event_message_impl
                #bulk_event_type_impl
                #bulk_event_message_impl
            }
        });
    }

    let mut to_command_arms = Vec::new();
    let mut event_type_arms = Vec::new();
    let mut event_type_count = 0usize;
    let mut event_message_arms = Vec::new();
    let mut event_message_count = 0usize;
    let mut bulk_event_type_arms = Vec::new();
    let mut bulk_event_type_count = 0usize;
    let mut bulk_event_message_arms = Vec::new();
    let mut bulk_event_message_count = 0usize;
    let total_variants = enum_data.variants.len();

    for variant in &enum_data.variants {
        let spec = parse_persist_case_options(&variant.attrs)?.ok_or_else(|| {
            syn::Error::new(
                variant.ident.span(),
                "Missing #[persist_case(...)] for enum variant; alternatively specify #[persist_intent(to_command = ...)] and methods",
            )
        })?;

        let command_expr = spec.command.ok_or_else(|| {
            syn::Error::new(
                variant.ident.span(),
                "Missing persist_case option: command = <expr>",
            )
        })?;

        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new(
                variant.ident.span(),
                "persist_case mapping currently supports only unit enum variants; use #[persist_intent(to_command = ...)] for payload variants",
            ));
        }

        let pattern = variant_match_pattern(&enum_ident, variant);
        to_command_arms.push(quote! {
            #pattern => { #command_expr }
        });

        if let Some(event_type_lit) = spec.event_type {
            event_type_count += 1;
            event_type_arms.push(quote! {
                #pattern => #event_type_lit.to_string()
            });
        }
        if let Some(event_message_lit) = spec.event_message {
            event_message_count += 1;
            event_message_arms.push(quote! {
                #pattern => #event_message_lit.to_string()
            });
        }
        if let Some(bulk_event_type_lit) = spec.bulk_event_type {
            bulk_event_type_count += 1;
            bulk_event_type_arms.push(quote! {
                #pattern => #bulk_event_type_lit.to_string()
            });
        }
        if let Some(bulk_event_message_lit) = spec.bulk_event_message {
            bulk_event_message_count += 1;
            bulk_event_message_arms.push(quote! {
                #pattern => #bulk_event_message_lit.to_string()
            });
        }
    }

    if event_type_count != 0 && event_type_count != total_variants {
        return Err(syn::Error::new(
            enum_ident.span(),
            "When using persist_case(event_type = \"...\"), define it for every enum variant or omit it entirely",
        ));
    }
    if event_message_count != 0 && event_message_count != total_variants {
        return Err(syn::Error::new(
            enum_ident.span(),
            "When using persist_case(event_message = \"...\"), define it for every enum variant or omit it entirely",
        ));
    }
    if bulk_event_type_count != 0 && bulk_event_type_count != total_variants {
        return Err(syn::Error::new(
            enum_ident.span(),
            "When using persist_case(bulk_event_type = \"...\"), define it for every enum variant or omit it entirely",
        ));
    }
    if bulk_event_message_count != 0 && bulk_event_message_count != total_variants {
        return Err(syn::Error::new(
            enum_ident.span(),
            "When using persist_case(bulk_event_message = \"...\"), define it for every enum variant or omit it entirely",
        ));
    }

    let event_type_impl = if event_type_count == total_variants {
        quote! {
            fn audit_event_type(&self, _command: &#command_ty) -> String {
                match self.clone() {
                    #(#event_type_arms),*
                }
            }
        }
    } else {
        quote! {}
    };
    let event_message_impl = if event_message_count == total_variants {
        quote! {
            fn audit_message(&self, _command: &#command_ty) -> String {
                match self.clone() {
                    #(#event_message_arms),*
                }
            }
        }
    } else {
        quote! {}
    };
    let bulk_event_type_impl = if bulk_event_type_count == total_variants {
        quote! {
            fn bulk_audit_event_type(&self, _command: &#command_ty) -> String {
                match self.clone() {
                    #(#bulk_event_type_arms),*
                }
            }
        }
    } else {
        quote! {}
    };
    let bulk_event_message_impl = if bulk_event_message_count == total_variants {
        quote! {
            fn bulk_audit_message(&self, _command: &#command_ty) -> String {
                match self.clone() {
                    #(#bulk_event_message_arms),*
                }
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        impl ::rustmemodb::PersistAutonomousCommand<#model_ty> for #enum_ident {
            fn to_persist_command(self) -> <#model_ty as ::rustmemodb::PersistCommandModel>::Command {
                match self {
                    #(#to_command_arms),*
                }
            }

            #event_type_impl
            #event_message_impl
            #bulk_event_type_impl
            #bulk_event_message_impl
        }
    })
}

fn expand_persist_json_value(input: DeriveInput) -> syn::Result<TokenStream2> {
    let ident = input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            ident.span(),
            "#[derive(PersistJsonValue)] does not support generic types",
        ));
    }

    match input.data {
        Data::Struct(_) | Data::Enum(_) => {}
        Data::Union(_) => {
            return Err(syn::Error::new(
                ident.span(),
                "#[derive(PersistJsonValue)] can only be used with structs or enums",
            ));
        }
    }

    Ok(quote! {
        impl ::rustmemodb::PersistValue for #ident {
            fn sql_type() -> &'static str {
                "TEXT"
            }

            fn to_sql_literal(&self) -> String {
                ::rustmemodb::persist::json_to_sql_literal(self)
            }
        }
    })
}

fn expand_api_error(input: DeriveInput) -> syn::Result<TokenStream2> {
    let enum_ident = input.ident;
    let enum_data = match input.data {
        Data::Enum(data) => data,
        _ => {
            return Err(syn::Error::new(
                enum_ident.span(),
                "#[derive(ApiError)] can only be used with enums",
            ));
        }
    };

    let mut arms = Vec::<TokenStream2>::new();
    for variant in &enum_data.variants {
        let options = parse_api_error_options(&variant.attrs)?;
        let status = options.status.unwrap_or(422u16);
        let default_code = to_snake_case(&variant.ident.to_string());
        let code = options.code.unwrap_or(default_code);
        let pattern = variant_match_pattern(&enum_ident, variant);
        let mapped = quote!(::rustmemodb::PersistServiceError::custom(#status, #code, message));
        arms.push(quote! {
            #pattern => #mapped
        });
    }

    Ok(quote! {
        impl ::core::convert::From<#enum_ident> for ::rustmemodb::PersistServiceError {
            fn from(value: #enum_ident) -> Self {
                let message = value.to_string();
                match value {
                    #(#arms),*
                }
            }
        }
    })
}

fn expand_autonomous(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = input.ident.clone();
    let vis = input.vis.clone();

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "Autonomous does not support generic structs yet",
        ));
    }

    let persisted_name = format_ident!("{}Persisted", struct_name);
    let collection_name = format_ident!("{}AutonomousVec", struct_name);
    let persist_model_tokens = expand_persist_model(input)?;

    Ok(quote! {
        #persist_model_tokens

        ::rustmemodb::persist_vec!(#vis #collection_name, #persisted_name);

        impl ::core::clone::Clone for #persisted_name {
            fn clone(&self) -> Self {
                Self {
                    data: self.data.clone(),
                    __persist_id: self.__persist_id.clone(),
                    __table_name: self.__table_name.clone(),
                    __metadata: self.__metadata.clone(),
                    __dirty_fields: self.__dirty_fields.clone(),
                    __table_ready: self.__table_ready,
                    __bound_session: self.__bound_session.clone(),
                    __auto_persist: self.__auto_persist,
                    __functions: self.__functions.clone(),
                }
            }
        }

        impl ::rustmemodb::PersistBackedModel<#struct_name> for #persisted_name {
            fn model(&self) -> &#struct_name {
                self.data()
            }

            fn model_mut(&mut self) -> &mut #struct_name {
                self.data_mut()
            }
        }

        impl ::rustmemodb::PersistAutonomousModel for #struct_name {
            type Persisted = #persisted_name;
            type Collection = #collection_name;

            fn into_persisted(self) -> Self::Persisted {
                <Self as ::rustmemodb::persist::PersistModelExt>::into_persisted(self)
            }

            fn from_persisted(persisted: Self::Persisted) -> Self {
                persisted.into_inner()
            }
        }
    })
}

fn variant_match_pattern(enum_ident: &Ident, variant: &syn::Variant) -> TokenStream2 {
    let variant_ident = &variant.ident;
    match &variant.fields {
        Fields::Unit => quote!(#enum_ident::#variant_ident),
        Fields::Unnamed(_) => quote!(#enum_ident::#variant_ident(..)),
        Fields::Named(_) => quote!(#enum_ident::#variant_ident { .. }),
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
    let mut field_sql_options = Vec::<Option<SqlFieldOptions>>::new();

    for field in named_fields.named {
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(field.span(), "PersistModel requires named fields"))?;
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
        None => {
            quote! { ::rustmemodb::persist::default_table_name_stable(stringify!(#struct_name)) }
        }
    };
    let schema_version_literal = model_options.schema_version.unwrap_or(1u32);
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

        let indexed = field_sql
            .as_ref()
            .map(|options| options.indexed)
            .unwrap_or(false);
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

    let sql_columns = field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(field, ty)| {
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

    let insert_values = field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(field, ty)| {
            quote! {
                values.push(
                    <#ty as ::rustmemodb::PersistValue>::to_sql_literal(&self.data.#field)
                );
            }
        });

    let update_assignments = field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(field, ty)| {
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

    let from_state_fields = field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(field, ty)| {
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

    let command_apply_arms =
        command_variant_idents
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

        impl ::rustmemodb::persist::PersistCommandName for #command_name {
            fn command_name(&self) -> &'static str {
                self.name()
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
    idempotent: bool,
    input: Option<Type>,
    output: Option<Type>,
}

impl Default for CommandAttrOptions {
    fn default() -> Self {
        Self {
            name: None,
            idempotent: true,
            input: None,
            output: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewInputMode {
    Query,
    Body,
}

impl Default for ViewInputMode {
    fn default() -> Self {
        Self::Query
    }
}

#[derive(Clone, Default)]
struct ViewAttrOptions {
    name: Option<String>,
    input: ViewInputMode,
    input_ty: Option<Type>,
    output: Option<Type>,
}

struct ApiErrorAttrOptions {
    status: Option<u16>,
    code: Option<String>,
}

#[derive(Default)]
struct PersistIntentOptions {
    model: Option<Type>,
    to_command: Option<Ident>,
    event_type: Option<Ident>,
    event_message: Option<Ident>,
    bulk_event_type: Option<Ident>,
    bulk_event_message: Option<Ident>,
}

#[derive(Default)]
struct PersistCaseOptions {
    command: Option<syn::Expr>,
    event_type: Option<LitStr>,
    event_message: Option<LitStr>,
    bulk_event_type: Option<LitStr>,
    bulk_event_message: Option<LitStr>,
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
            FnArg::Receiver(receiver)
                if receiver.reference.is_some() && receiver.mutability.is_some() => {}
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

enum AutonomousMethodReturnKind {
    Unit,
    Plain(Type),
    RustResult { ok: Type, err: Type },
}

impl AutonomousMethodReturnKind {
    fn from_signature(signature: &syn::Signature) -> Self {
        match &signature.output {
            ReturnType::Default => Self::Unit,
            ReturnType::Type(_, ty) => {
                if let Some((ok, err)) = extract_result_types(ty) {
                    return Self::RustResult { ok, err };
                }
                Self::Plain((**ty).clone())
            }
        }
    }

    fn output_ty_tokens(&self) -> TokenStream2 {
        match self {
            Self::Unit => quote!(()),
            Self::Plain(ty) => quote!(#ty),
            Self::RustResult { ok, .. } => quote!(#ok),
        }
    }

    fn error_ty_tokens(&self) -> TokenStream2 {
        match self {
            Self::Unit | Self::Plain(_) => quote!(::core::convert::Infallible),
            Self::RustResult { err, .. } => quote!(#err),
        }
    }

    fn mutation_closure_body(&self, method_call: TokenStream2) -> TokenStream2 {
        match self {
            Self::Unit => quote! {
                #method_call;
                Ok::<(), ::core::convert::Infallible>(())
            },
            Self::Plain(ty) => quote! {
                let output: #ty = #method_call;
                Ok::<#ty, ::core::convert::Infallible>(output)
            },
            Self::RustResult { .. } => quote! {
                #method_call
            },
        }
    }

    fn success_is_no_content(&self) -> bool {
        match self {
            Self::Unit => true,
            Self::Plain(_) => false,
            Self::RustResult { ok, .. } => is_unit_type(ok),
        }
    }
}

struct AutonomousExposedMethod {
    method_ident: Ident,
    args: Vec<PersistentCommandArg>,
    return_kind: AutonomousMethodReturnKind,
    route_name: String,
    idempotent: bool,
    input_ty: Option<Type>,
    output_ty: Option<Type>,
}

impl AutonomousExposedMethod {
    fn from_impl_method(method: &ImplItemFn, marker: CommandAttrOptions) -> syn::Result<Self> {
        if method.sig.asyncness.is_some() {
            return Err(syn::Error::new(
                method.sig.span(),
                "#[command] methods in #[autonomous_impl] must be synchronous",
            ));
        }
        if !method.sig.generics.params.is_empty() {
            return Err(syn::Error::new(
                method.sig.generics.span(),
                "#[command] methods in #[autonomous_impl] cannot have generic parameters",
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
            FnArg::Receiver(receiver)
                if receiver.reference.is_some() && receiver.mutability.is_some() => {}
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

        if marker.input.is_some() && args.len() != 1 {
            return Err(syn::Error::new(
                method.sig.span(),
                "#[command(input = <Type>)] requires exactly one method argument (besides &mut self)",
            ));
        }

        let route_name = marker
            .name
            .clone()
            .unwrap_or_else(|| method.sig.ident.to_string());

        Ok(Self {
            method_ident: method.sig.ident.clone(),
            args,
            return_kind: AutonomousMethodReturnKind::from_signature(&method.sig),
            route_name,
            idempotent: marker.idempotent,
            input_ty: marker.input,
            output_ty: marker.output,
        })
    }

    fn trait_method_tokens(&self) -> TokenStream2 {
        let method_ident = &self.method_ident;
        let args = self.args.iter().map(|arg| {
            let ident = &arg.ident;
            let ty = &arg.ty;
            quote!(#ident: #ty)
        });
        let output_ty = self.return_kind.output_ty_tokens();
        let error_ty = self.return_kind.error_ty_tokens();

        quote! {
            async fn #method_ident(
                &self,
                persist_id: &str
                #(, #args)*
            ) -> ::std::result::Result<#output_ty, ::rustmemodb::PersistDomainMutationError<#error_ty>>;
        }
    }

    fn impl_method_tokens(&self, model_ident: &Ident) -> TokenStream2 {
        let method_ident = &self.method_ident;
        let args = self.args.iter().map(|arg| {
            let ident = &arg.ident;
            let ty = &arg.ty;
            quote!(#ident: #ty)
        });
        let arg_idents = self.args.iter().map(|arg| &arg.ident);
        let output_ty = self.return_kind.output_ty_tokens();
        let error_ty = self.return_kind.error_ty_tokens();
        let method_call = quote!(model.#method_ident(#(#arg_idents),*));
        let mutation_body = self.return_kind.mutation_closure_body(method_call);

        quote! {
            async fn #method_ident(
                &self,
                persist_id: &str
                #(, #args)*
            ) -> ::std::result::Result<#output_ty, ::rustmemodb::PersistDomainMutationError<#error_ty>> {
                let (_, output) = self
                    .mutate_one_with_result_named(
                        persist_id,
                        stringify!(#method_ident),
                        move |model: &mut #model_ident| {
                            #mutation_body
                        },
                    )
                    .await?;
                Ok(output)
            }
        }
    }

    fn route_literal(&self) -> String {
        self.route_name.clone()
    }

    fn inferred_input_ty(&self) -> Option<&Type> {
        self.input_ty
            .as_ref()
            .or_else(|| self.auto_infer_single_payload_input_ty())
    }

    fn auto_infer_single_payload_input_ty(&self) -> Option<&Type> {
        if self.args.len() != 1 || self.input_ty.is_some() {
            return None;
        }
        let ty = &self.args[0].ty;
        if supports_direct_payload_input(ty) {
            Some(ty)
        } else {
            None
        }
    }

    fn request_ident(&self, model_ident: &Ident) -> Ident {
        format_ident!(
            "{}{}Request",
            model_ident,
            to_pascal_case(&self.method_ident.to_string())
        )
    }

    fn request_struct_tokens(&self, model_ident: &Ident) -> Option<TokenStream2> {
        if self.inferred_input_ty().is_some() {
            return None;
        }
        let request_ident = self.request_ident(model_ident);
        let fields = self.args.iter().map(|arg| {
            let ident = &arg.ident;
            let ty = &arg.ty;
            quote!(pub #ident: #ty)
        });
        Some(quote! {
            #[derive(::serde::Deserialize, ::serde::Serialize, ::core::fmt::Debug, ::core::clone::Clone)]
            pub struct #request_ident {
                #( #fields, )*
            }
        })
    }

    fn request_ty_tokens(&self, model_ident: &Ident) -> TokenStream2 {
        if let Some(input_ty) = self.inferred_input_ty() {
            quote!(#input_ty)
        } else {
            let request_ident = self.request_ident(model_ident);
            quote!(#request_ident)
        }
    }

    fn openapi_request_rust_type_literal(&self, model_ident: &Ident) -> Option<String> {
        if let Some(input_ty) = self.inferred_input_ty() {
            Some(type_to_marker_string(input_ty))
        } else {
            Some(self.request_ident(model_ident).to_string())
        }
    }

    fn openapi_response_rust_type_literal(&self) -> Option<String> {
        if self.return_kind.success_is_no_content() {
            return None;
        }
        if let Some(output_ty) = self.output_ty.as_ref() {
            return Some(type_to_marker_string(output_ty));
        }
        match &self.return_kind {
            AutonomousMethodReturnKind::Unit => None,
            AutonomousMethodReturnKind::Plain(ty) => Some(type_to_marker_string(ty)),
            AutonomousMethodReturnKind::RustResult { ok, .. } => Some(type_to_marker_string(ok)),
        }
    }

    fn openapi_success_status_code(&self) -> u16 {
        if self.return_kind.success_is_no_content() {
            204
        } else {
            200
        }
    }

    fn command_handler_tokens(&self, model_ident: &Ident) -> TokenStream2 {
        let method_ident = &self.method_ident;
        let handler_ident = format_ident!("handle_command_{}", method_ident);
        let request_ty = self.request_ty_tokens(model_ident);
        let response_ok = self.return_kind.output_ty_tokens();
        let response_err = self.return_kind.error_ty_tokens();
        let command_call = if self.inferred_input_ty().is_some() {
            let arg_ty = &self
                .args
                .first()
                .expect("validated single arg for command payload input")
                .ty;
            quote!(model.#method_ident(::core::convert::Into::<#arg_ty>::into(request)))
        } else {
            let call_args = self
                .args
                .iter()
                .map(|arg| {
                    let ident = &arg.ident;
                    quote!(request.#ident)
                })
                .collect::<Vec<_>>();
            quote!(model.#method_ident(#(#call_args),*))
        };
        let mutation_body = self.return_kind.mutation_closure_body(command_call);
        let success_status_code = self.openapi_success_status_code();
        let success_is_no_content = self.return_kind.success_is_no_content();
        let success_body = if success_is_no_content {
            quote! {
                axum::response::IntoResponse::into_response(axum::http::StatusCode::NO_CONTENT)
            }
        } else {
            quote! {
                axum::response::IntoResponse::into_response((
                    axum::http::StatusCode::OK,
                    axum::Json(output),
                ))
            }
        };
        let replay_body = if success_is_no_content {
            quote! {
                if status_code == axum::http::StatusCode::NO_CONTENT.as_u16() {
                    axum::response::IntoResponse::into_response(axum::http::StatusCode::NO_CONTENT)
                } else {
                    let status = axum::http::StatusCode::from_u16(status_code)
                        .unwrap_or(axum::http::StatusCode::OK);
                    axum::response::IntoResponse::into_response((
                        status,
                        axum::Json(body),
                    ))
                }
            }
        } else {
            quote! {
                let status = axum::http::StatusCode::from_u16(status_code)
                    .unwrap_or(axum::http::StatusCode::OK);
                axum::response::IntoResponse::into_response((
                    status,
                    axum::Json(body),
                ))
            }
        };
        let response_bound = if success_is_no_content {
            quote! {}
        } else {
            quote!(#response_ok: ::serde::Serialize,)
        };
        let applied_binding = if success_is_no_content {
            quote!(_output)
        } else {
            quote!(output)
        };
        let request_validation = if self.inferred_input_ty().is_some() {
            quote! {}
        } else {
            let request_fields = self.args.iter().map(|arg| &arg.ident);
            quote! {
                let _ = (#(&request.#request_fields),*);
            }
        };
        let idempotency_extract = if self.idempotent {
            let invalid_key_response = quote! {
                let web_err = ::rustmemodb::web::WebError::Input(
                    ::rustmemodb::IDEMPOTENCY_KEY_INVALID_MESSAGE.to_string(),
                );
                return axum::response::IntoResponse::into_response(web_err);
            };
            quote! {
                let raw_idempotency_key = match headers.get("Idempotency-Key") {
                    Some(raw) => {
                        match raw.to_str() {
                            Ok(value) => Some(value),
                            Err(_) => {
                                #invalid_key_response
                            }
                        }
                    }
                    None => None,
                };
                let idempotency_key = match ::rustmemodb::normalize_idempotency_key(raw_idempotency_key) {
                    Ok(value) => value,
                    Err(err) => {
                        let web_err = ::rustmemodb::web::WebError::Input(err.message().to_string());
                        return axum::response::IntoResponse::into_response(web_err);
                    }
                };
            }
        } else {
            quote! {
                let idempotency_key = None;
            }
        };

        quote! {
            async fn #handler_ident(
                axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                axum::extract::Path(id): axum::extract::Path<String>,
                headers: axum::http::HeaderMap,
                axum::extract::Json(request): axum::extract::Json<#request_ty>,
            ) -> axum::response::Response
            where
                #response_bound
                #response_err: ::core::convert::Into<::rustmemodb::PersistServiceError>,
            {
                #request_validation
                #idempotency_extract

                match handle
                    .execute_rest_command_with_idempotency(
                        id.as_str(),
                        stringify!(#method_ident),
                        idempotency_key,
                        #success_status_code,
                        move |model: &mut #model_ident| {
                            #mutation_body
                        },
                    )
                    .await
                {
                    Ok(::rustmemodb::PersistIdempotentCommandResult::Applied(#applied_binding)) => {
                        #success_body
                    }
                    Ok(::rustmemodb::PersistIdempotentCommandResult::Replayed { status_code, body }) => {
                        #replay_body
                    }
                    Err(err) => {
                        let web_err: ::rustmemodb::web::WebError = err.into();
                        axum::response::IntoResponse::into_response(web_err)
                    }
                }
            }
        }
    }
}

enum AutonomousViewReturnKind {
    Unit,
    Plain(Type),
    RustResult { ok: Type, err: Type },
}

impl AutonomousViewReturnKind {
    fn from_signature(signature: &syn::Signature) -> Self {
        match &signature.output {
            ReturnType::Default => Self::Unit,
            ReturnType::Type(_, ty) => {
                if let Some((ok, err)) = extract_result_types(ty) {
                    return Self::RustResult { ok, err };
                }
                Self::Plain((**ty).clone())
            }
        }
    }

    fn success_is_no_content(&self) -> bool {
        match self {
            Self::Unit => true,
            Self::Plain(_) => false,
            Self::RustResult { ok, .. } => is_unit_type(ok),
        }
    }
}

struct AutonomousViewMethod {
    method_ident: Ident,
    route_name: String,
    args: Vec<PersistentCommandArg>,
    input_mode: ViewInputMode,
    input_ty: Option<Type>,
    output_ty: Option<Type>,
    return_kind: AutonomousViewReturnKind,
}

impl AutonomousViewMethod {
    fn from_impl_method(method: &ImplItemFn, marker: ViewAttrOptions) -> syn::Result<Self> {
        if method.sig.asyncness.is_some() {
            return Err(syn::Error::new(
                method.sig.span(),
                "#[view]/#[query] methods in #[expose_rest] must be synchronous",
            ));
        }
        if !method.sig.generics.params.is_empty() {
            return Err(syn::Error::new(
                method.sig.generics.span(),
                "#[view]/#[query] methods in #[expose_rest] cannot have generic parameters",
            ));
        }

        let mut inputs_iter = method.sig.inputs.iter();
        let receiver = inputs_iter.next().ok_or_else(|| {
            syn::Error::new(
                method.sig.span(),
                "#[view]/#[query] method must have &self receiver",
            )
        })?;

        match receiver {
            FnArg::Receiver(receiver)
                if receiver.reference.is_some() && receiver.mutability.is_none() => {}
            _ => {
                return Err(syn::Error::new(
                    receiver.span(),
                    "#[view]/#[query] method receiver must be `&self`",
                ));
            }
        }

        let mut args = Vec::new();
        for input in inputs_iter {
            let FnArg::Typed(PatType { pat, ty, .. }) = input else {
                return Err(syn::Error::new(
                    input.span(),
                    "Unsupported #[view]/#[query] argument pattern",
                ));
            };
            let Pat::Ident(pat_ident) = pat.as_ref() else {
                return Err(syn::Error::new(
                    pat.span(),
                    "#[view]/#[query] arguments must be simple identifiers",
                ));
            };
            args.push(PersistentCommandArg {
                ident: pat_ident.ident.clone(),
                ty: (**ty).clone(),
            });
        }

        if marker.input_ty.is_some() && args.len() != 1 {
            return Err(syn::Error::new(
                method.sig.span(),
                "#[query(input = <Type>)] / #[view(input = <Type>)] requires exactly one method argument (besides &self)",
            ));
        }

        Ok(Self {
            method_ident: method.sig.ident.clone(),
            route_name: marker.name.unwrap_or_else(|| method.sig.ident.to_string()),
            args,
            input_mode: marker.input,
            input_ty: marker.input_ty,
            output_ty: marker.output,
            return_kind: AutonomousViewReturnKind::from_signature(&method.sig),
        })
    }

    fn route_literal(&self) -> String {
        self.route_name.clone()
    }

    fn inferred_input_ty(&self) -> Option<&Type> {
        self.input_ty
            .as_ref()
            .or_else(|| self.auto_infer_single_payload_input_ty())
    }

    fn auto_infer_single_payload_input_ty(&self) -> Option<&Type> {
        if self.args.len() != 1 || self.input_ty.is_some() {
            return None;
        }
        let ty = &self.args[0].ty;
        if supports_direct_payload_input(ty) {
            Some(ty)
        } else {
            None
        }
    }

    fn request_ident(&self, model_ident: &Ident) -> Ident {
        format_ident!(
            "{}{}ViewRequest",
            model_ident,
            to_pascal_case(&self.method_ident.to_string())
        )
    }

    fn request_struct_tokens(&self, model_ident: &Ident) -> Option<TokenStream2> {
        if self.args.is_empty() || self.inferred_input_ty().is_some() {
            return None;
        }
        let request_ident = self.request_ident(model_ident);
        let fields = self.args.iter().map(|arg| {
            let ident = &arg.ident;
            let ty = &arg.ty;
            quote!(pub #ident: #ty)
        });
        Some(quote! {
            #[derive(::serde::Deserialize, ::serde::Serialize, ::core::fmt::Debug, ::core::clone::Clone)]
            pub struct #request_ident {
                #( #fields, )*
            }
        })
    }

    fn request_ty_tokens(&self, model_ident: &Ident) -> Option<TokenStream2> {
        if self.args.is_empty() {
            return None;
        }
        if let Some(input_ty) = self.inferred_input_ty() {
            Some(quote!(#input_ty))
        } else {
            let request_ident = self.request_ident(model_ident);
            Some(quote!(#request_ident))
        }
    }

    fn openapi_request_rust_type_literal(&self, model_ident: &Ident) -> Option<String> {
        if self.args.is_empty() {
            return None;
        }
        if let Some(input_ty) = self.inferred_input_ty() {
            Some(type_to_marker_string(input_ty))
        } else {
            Some(self.request_ident(model_ident).to_string())
        }
    }

    fn openapi_response_rust_type_literal(&self) -> Option<String> {
        if self.return_kind.success_is_no_content() {
            return None;
        }
        if let Some(output_ty) = self.output_ty.as_ref() {
            return Some(type_to_marker_string(output_ty));
        }
        match &self.return_kind {
            AutonomousViewReturnKind::Unit => None,
            AutonomousViewReturnKind::Plain(ok) => Some(type_to_marker_string(ok)),
            AutonomousViewReturnKind::RustResult { ok, .. } => Some(type_to_marker_string(ok)),
        }
    }

    fn openapi_success_status_code(&self) -> u16 {
        if self.return_kind.success_is_no_content() {
            204
        } else {
            200
        }
    }

    fn view_handler_tokens(&self, model_ident: &Ident) -> TokenStream2 {
        let method_ident = &self.method_ident;
        let handler_ident = format_ident!("handle_view_{}", method_ident);
        let request_ty = self.request_ty_tokens(model_ident);
        let call_args = if self.inferred_input_ty().is_some() {
            let arg_ty = &self
                .args
                .first()
                .expect("validated single arg for view/query payload input")
                .ty;
            quote!(::core::convert::Into::<#arg_ty>::into(params))
        } else {
            let args = self.args.iter().map(|arg| {
                let ident = &arg.ident;
                quote!(params.#ident)
            });
            quote!(#(#args),*)
        };
        let query_extractor = if let Some(request_ty) = request_ty {
            match self.input_mode {
                ViewInputMode::Query => {
                    quote! {
                        axum::extract::Query(params): axum::extract::Query<#request_ty>,
                    }
                }
                ViewInputMode::Body => {
                    quote! {
                        axum::extract::Json(params): axum::extract::Json<#request_ty>,
                    }
                }
            }
        } else {
            quote! {}
        };
        match &self.return_kind {
            AutonomousViewReturnKind::Unit => {
                quote! {
                    async fn #handler_ident(
                        axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                        axum::extract::Path(id): axum::extract::Path<String>,
                        #query_extractor
                    ) -> axum::response::Response {
                        let Some(record) = handle.get_one(id.as_str()).await else {
                            return axum::response::IntoResponse::into_response(::rustmemodb::web::WebError::NotFound(format!("entity not found: {}", id)));
                        };
                        record.model.#method_ident(#call_args);
                        axum::response::IntoResponse::into_response(axum::http::StatusCode::NO_CONTENT)
                    }
                }
            }
            AutonomousViewReturnKind::RustResult { ok, err } if is_unit_type(ok) => {
                quote! {
                    async fn #handler_ident(
                        axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                        axum::extract::Path(id): axum::extract::Path<String>,
                        #query_extractor
                    ) -> axum::response::Response
                    where
                        #err: ::core::convert::Into<::rustmemodb::PersistServiceError>,
                    {
                        let Some(record) = handle.get_one(id.as_str()).await else {
                            return axum::response::IntoResponse::into_response(::rustmemodb::web::WebError::NotFound(format!("entity not found: {}", id)));
                        };
                        match record.model.#method_ident(#call_args) {
                            Ok(()) => axum::response::IntoResponse::into_response(axum::http::StatusCode::NO_CONTENT),
                            Err(err) => {
                                let service_error: ::rustmemodb::PersistServiceError = err.into();
                                let web_err: ::rustmemodb::web::WebError = service_error.into();
                                axum::response::IntoResponse::into_response(web_err)
                            }
                        }
                    }
                }
            }
            AutonomousViewReturnKind::Plain(ok_ty) => {
                quote! {
                    async fn #handler_ident(
                        axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                        axum::extract::Path(id): axum::extract::Path<String>,
                        #query_extractor
                    ) -> axum::response::Response
                    where
                        #ok_ty: ::serde::Serialize,
                    {
                        let Some(record) = handle.get_one(id.as_str()).await else {
                            return axum::response::IntoResponse::into_response(::rustmemodb::web::WebError::NotFound(format!("entity not found: {}", id)));
                        };
                        let output = record.model.#method_ident(#call_args);
                        axum::response::IntoResponse::into_response((
                            axum::http::StatusCode::OK,
                            axum::Json(output),
                        ))
                    }
                }
            }
            AutonomousViewReturnKind::RustResult { ok, err } => {
                quote! {
                    async fn #handler_ident(
                        axum::extract::State(handle): axum::extract::State<::rustmemodb::PersistAutonomousModelHandle<#model_ident>>,
                        axum::extract::Path(id): axum::extract::Path<String>,
                        #query_extractor
                    ) -> axum::response::Response
                    where
                        #ok: ::serde::Serialize,
                        #err: ::core::convert::Into<::rustmemodb::PersistServiceError>,
                    {
                        let Some(record) = handle.get_one(id.as_str()).await else {
                            return axum::response::IntoResponse::into_response(::rustmemodb::web::WebError::NotFound(format!("entity not found: {}", id)));
                        };
                        match record.model.#method_ident(#call_args) {
                            Ok(output) => axum::response::IntoResponse::into_response((
                                axum::http::StatusCode::OK,
                                axum::Json(output),
                            )),
                            Err(err) => {
                                let service_error: ::rustmemodb::PersistServiceError = err.into();
                                let web_err: ::rustmemodb::web::WebError = service_error.into();
                                axum::response::IntoResponse::into_response(web_err)
                            }
                        }
                    }
                }
            }
        }
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

fn to_snake_case(value: &str) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else if ch == '-' || ch == ' ' {
            if !out.ends_with('_') {
                out.push('_');
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn supports_direct_payload_input(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => {
            let Some(last) = path.path.segments.last() else {
                return false;
            };
            if !matches!(last.arguments, syn::PathArguments::None) {
                return false;
            }
            let ident = last.ident.to_string();
            let scalar_like = matches!(
                ident.as_str(),
                "String"
                    | "str"
                    | "bool"
                    | "char"
                    | "u8"
                    | "u16"
                    | "u32"
                    | "u64"
                    | "u128"
                    | "usize"
                    | "i8"
                    | "i16"
                    | "i32"
                    | "i64"
                    | "i128"
                    | "isize"
                    | "f32"
                    | "f64"
                    | "Uuid"
                    | "DateTime"
                    | "NaiveDate"
                    | "NaiveDateTime"
                    | "Value"
                    | "JsonValue"
                    | "Bytes"
            );
            if scalar_like {
                return false;
            }
            let container_like = matches!(
                ident.as_str(),
                "Option" | "Vec" | "HashMap" | "BTreeMap" | "HashSet" | "BTreeSet" | "Result"
            );
            !container_like
        }
        _ => false,
    }
}

fn has_derive_trait(attrs: &[syn::Attribute], trait_name: &str) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("derive") {
            continue;
        }

        if let Ok(paths) =
            attr.parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
        {
            if paths.iter().any(|path| {
                path.segments
                    .last()
                    .map(|segment| segment.ident == trait_name)
                    .unwrap_or(false)
            }) {
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

fn parse_persist_intent_options(attrs: &[syn::Attribute]) -> syn::Result<PersistIntentOptions> {
    let mut options = PersistIntentOptions::default();

    for attr in attrs {
        if !path_ends_with_ident(attr.path(), "persist_intent") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("model") {
                if options.model.is_some() {
                    return Err(meta.error("Duplicate persist_intent option: model"));
                }
                let value = meta.value()?;
                options.model = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("to_command") {
                if options.to_command.is_some() {
                    return Err(meta.error("Duplicate persist_intent option: to_command"));
                }
                let value = meta.value()?;
                options.to_command = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("event_type") {
                if options.event_type.is_some() {
                    return Err(meta.error("Duplicate persist_intent option: event_type"));
                }
                let value = meta.value()?;
                options.event_type = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("event_message") {
                if options.event_message.is_some() {
                    return Err(meta.error("Duplicate persist_intent option: event_message"));
                }
                let value = meta.value()?;
                options.event_message = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("bulk_event_type") {
                if options.bulk_event_type.is_some() {
                    return Err(meta.error("Duplicate persist_intent option: bulk_event_type"));
                }
                let value = meta.value()?;
                options.bulk_event_type = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("bulk_event_message") {
                if options.bulk_event_message.is_some() {
                    return Err(meta.error("Duplicate persist_intent option: bulk_event_message"));
                }
                let value = meta.value()?;
                options.bulk_event_message = Some(value.parse()?);
                return Ok(());
            }

            Err(meta.error(
                "Unsupported #[persist_intent(...)] option. Supported: model = <Type>, to_command = <method>, event_type = <method>, event_message = <method>, bulk_event_type = <method>, bulk_event_message = <method>",
            ))
        })?;
    }

    Ok(options)
}

fn parse_persist_case_options(attrs: &[syn::Attribute]) -> syn::Result<Option<PersistCaseOptions>> {
    let mut result: Option<PersistCaseOptions> = None;

    for attr in attrs {
        if !path_ends_with_ident(attr.path(), "persist_case") {
            continue;
        }

        if result.is_some() {
            return Err(syn::Error::new(
                attr.span(),
                "Duplicate #[persist_case(...)] attribute",
            ));
        }

        let mut options = PersistCaseOptions::default();
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("command") {
                if options.command.is_some() {
                    return Err(meta.error("Duplicate persist_case option: command"));
                }
                let value = meta.value()?;
                options.command = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("event_type") {
                if options.event_type.is_some() {
                    return Err(meta.error("Duplicate persist_case option: event_type"));
                }
                let value = meta.value()?;
                options.event_type = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("event_message") {
                if options.event_message.is_some() {
                    return Err(meta.error("Duplicate persist_case option: event_message"));
                }
                let value = meta.value()?;
                options.event_message = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("bulk_event_type") {
                if options.bulk_event_type.is_some() {
                    return Err(meta.error("Duplicate persist_case option: bulk_event_type"));
                }
                let value = meta.value()?;
                options.bulk_event_type = Some(value.parse()?);
                return Ok(());
            }

            if meta.path.is_ident("bulk_event_message") {
                if options.bulk_event_message.is_some() {
                    return Err(meta.error("Duplicate persist_case option: bulk_event_message"));
                }
                let value = meta.value()?;
                options.bulk_event_message = Some(value.parse()?);
                return Ok(());
            }

            Err(meta.error(
                "Unsupported #[persist_case(...)] option. Supported: command = <expr>, event_type = \"...\", event_message = \"...\", bulk_event_type = \"...\", bulk_event_message = \"...\"",
            ))
        })?;

        result = Some(options);
    }

    Ok(result)
}

fn parse_api_error_options(attrs: &[syn::Attribute]) -> syn::Result<ApiErrorAttrOptions> {
    let mut options = ApiErrorAttrOptions {
        status: None,
        code: None,
    };
    for attr in attrs {
        if !path_ends_with_ident(attr.path(), "api_error") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("status") {
                if options.status.is_some() {
                    return Err(meta.error("Duplicate api_error option: status"));
                }
                let value = meta.value()?;
                let lit: syn::LitInt = value.parse()?;
                options.status = Some(lit.base10_parse::<u16>()?);
                return Ok(());
            }
            if meta.path.is_ident("code") {
                if options.code.is_some() {
                    return Err(meta.error("Duplicate api_error option: code"));
                }
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                options.code = Some(lit.value());
                return Ok(());
            }
            Err(meta.error(
                "Unsupported #[api_error(...)] option. Supported: status = <u16>, code = \"...\"",
            ))
        })?;
    }
    Ok(options)
}

fn parse_command_attr_tokens(attr: TokenStream2) -> syn::Result<CommandAttrOptions> {
    let mut options = CommandAttrOptions::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            options.name = Some(lit.value());
            return Ok(());
        }
        if meta.path.is_ident("idempotent") {
            let value = meta.value()?;
            let lit: syn::LitBool = value.parse()?;
            options.idempotent = lit.value;
            return Ok(());
        }
        if meta.path.is_ident("input") {
            let value = meta.value()?;
            options.input = Some(value.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("output") {
            let value = meta.value()?;
            options.output = Some(value.parse()?);
            return Ok(());
        }
        Err(meta.error(
            "Unsupported #[command(...)] option. Supported: name = \"...\", idempotent = <bool>, input = <Type>, output = <Type>",
        ))
    });

    parser.parse2(attr)?;
    Ok(options)
}

fn parse_view_attr_tokens(attr: TokenStream2) -> syn::Result<ViewAttrOptions> {
    let mut options = ViewAttrOptions::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            options.name = Some(lit.value());
            return Ok(());
        }
        if meta.path.is_ident("mode") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            let raw = lit.value().to_lowercase();
            options.input = match raw.as_str() {
                "query" => ViewInputMode::Query,
                "body" => ViewInputMode::Body,
                _ => {
                    return Err(meta.error(
                        "Unsupported #[view(...)] mode. Supported: mode = \"query\" | \"body\"",
                    ));
                }
            };
            return Ok(());
        }
        if meta.path.is_ident("input") {
            let value = meta.value()?;
            if value.peek(LitStr) {
                let lit: LitStr = value.parse()?;
                let raw = lit.value().to_lowercase();
                options.input = match raw.as_str() {
                    "query" => ViewInputMode::Query,
                    "body" => ViewInputMode::Body,
                    _ => {
                        return Err(meta.error(
                            "Unsupported #[view(...)] mode literal. Supported: input = \"query\" | \"body\" or input = <Type>",
                        ))
                    }
                };
                return Ok(());
            }
            options.input_ty = Some(value.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("input_type") {
            let value = meta.value()?;
            options.input_ty = Some(value.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("output") {
            let value = meta.value()?;
            options.output = Some(value.parse()?);
            return Ok(());
        }
        Err(meta.error(
            "Unsupported #[view(...)] option. Supported: name = \"...\", mode = \"query\" | \"body\", input = \"query\" | \"body\" | <Type>, input_type = <Type>, output = <Type>",
        ))
    });

    parser.parse2(attr)?;
    Ok(options)
}

fn parse_query_attr_tokens(attr: TokenStream2) -> syn::Result<ViewAttrOptions> {
    let mut options = ViewAttrOptions::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            options.name = Some(lit.value());
            return Ok(());
        }
        if meta.path.is_ident("input") || meta.path.is_ident("input_type") {
            let value = meta.value()?;
            if value.peek(LitStr) {
                let lit: LitStr = value.parse()?;
                let raw = lit.value().to_lowercase();
                if raw == "query" {
                    return Ok(());
                }
                return Err(meta.error(
                    "Unsupported #[query(...)] input mode. #[query] always uses GET + query parameters; use input = <Type> for typed query payload",
                ));
            }
            options.input_ty = Some(value.parse()?);
            return Ok(());
        }
        if meta.path.is_ident("mode") {
            let value = meta.value()?;
            let lit: LitStr = value.parse()?;
            if lit.value().eq_ignore_ascii_case("query") {
                return Ok(());
            }
            return Err(meta.error(
                "Unsupported #[query(...)] mode. #[query] always uses GET + query parameters",
            ));
        }
        if meta.path.is_ident("output") {
            let value = meta.value()?;
            options.output = Some(value.parse()?);
            return Ok(());
        }
        Err(meta.error(
            "Unsupported #[query(...)] option. Supported: name = \"...\", input = <Type>, output = <Type>",
        ))
    });

    parser.parse2(attr)?;
    options.input = ViewInputMode::Query;
    Ok(options)
}

fn parse_command_doc_marker(value: &str) -> Option<CommandAttrOptions> {
    const MARKER: &str = "__rustmemodb_command";
    if value == MARKER {
        return Some(CommandAttrOptions::default());
    }
    let raw = value.strip_prefix("__rustmemodb_command:")?;
    if !raw.contains('=') {
        let mut parsed = CommandAttrOptions::default();
        if !raw.trim().is_empty() {
            parsed.name = Some(raw.to_string());
        }
        return Some(parsed);
    }

    let mut parsed = CommandAttrOptions::default();
    for part in raw.split(';') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next().unwrap_or_default().trim();
        let value = kv.next().unwrap_or_default().trim();
        match key {
            "name" => {
                if !value.is_empty() {
                    parsed.name = Some(value.to_string());
                }
            }
            "idempotent" => {
                parsed.idempotent = !value.eq_ignore_ascii_case("false");
            }
            "input" => {
                if !value.is_empty() {
                    parsed.input = syn::parse_str::<Type>(value).ok();
                }
            }
            "output" => {
                if !value.is_empty() {
                    parsed.output = syn::parse_str::<Type>(value).ok();
                }
            }
            _ => {}
        }
    }
    Some(parsed)
}

fn parse_view_doc_marker(value: &str) -> Option<ViewAttrOptions> {
    const MARKER: &str = "__rustmemodb_view";
    if value == MARKER {
        return Some(ViewAttrOptions::default());
    }
    let raw = value.strip_prefix("__rustmemodb_view:")?;
    if !raw.contains('=') {
        return Some(ViewAttrOptions {
            name: if raw.trim().is_empty() {
                None
            } else {
                Some(raw.to_string())
            },
            input: ViewInputMode::Query,
            input_ty: None,
            output: None,
        });
    }

    let mut parsed = ViewAttrOptions::default();
    for part in raw.split(';') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next().unwrap_or_default().trim();
        let value = kv.next().unwrap_or_default().trim();
        match key {
            "name" => {
                if !value.is_empty() {
                    parsed.name = Some(value.to_string());
                }
            }
            "input" => {
                if value.eq_ignore_ascii_case("body") || value.eq_ignore_ascii_case("query") {
                    parsed.input = if value.eq_ignore_ascii_case("body") {
                        ViewInputMode::Body
                    } else {
                        ViewInputMode::Query
                    };
                } else {
                    parsed.input_ty = syn::parse_str::<Type>(value).ok();
                }
            }
            "mode" => {
                parsed.input = if value.eq_ignore_ascii_case("body") {
                    ViewInputMode::Body
                } else {
                    ViewInputMode::Query
                };
            }
            "input_type" => {
                parsed.input_ty = syn::parse_str::<Type>(value).ok();
            }
            "output" => {
                parsed.output = syn::parse_str::<Type>(value).ok();
            }
            _ => {}
        }
    }
    Some(parsed)
}

fn parse_query_doc_marker(value: &str) -> Option<ViewAttrOptions> {
    const MARKER: &str = "__rustmemodb_query";
    if value == MARKER {
        return Some(ViewAttrOptions::default());
    }
    let raw = value.strip_prefix("__rustmemodb_query:")?;
    if !raw.contains('=') {
        return Some(ViewAttrOptions {
            name: if raw.trim().is_empty() {
                None
            } else {
                Some(raw.to_string())
            },
            input: ViewInputMode::Query,
            input_ty: None,
            output: None,
        });
    }

    let mut parsed = ViewAttrOptions::default();
    for part in raw.split(';') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next().unwrap_or_default().trim();
        let value = kv.next().unwrap_or_default().trim();
        match key {
            "name" => {
                if !value.is_empty() {
                    parsed.name = Some(value.to_string());
                }
            }
            "input" | "input_type" => {
                if !value.eq_ignore_ascii_case("query") {
                    parsed.input_ty = syn::parse_str::<Type>(value).ok();
                }
            }
            "output" => {
                parsed.output = syn::parse_str::<Type>(value).ok();
            }
            _ => {}
        }
    }
    parsed.input = ViewInputMode::Query;
    Some(parsed)
}

fn extract_command_marker(
    attrs: &mut Vec<syn::Attribute>,
) -> syn::Result<Option<CommandAttrOptions>> {
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

fn extract_view_marker(attrs: &mut Vec<syn::Attribute>) -> syn::Result<Option<ViewAttrOptions>> {
    let mut found: Option<ViewAttrOptions> = None;
    let mut kept = Vec::with_capacity(attrs.len());

    for attr in attrs.drain(..) {
        if path_ends_with_ident(attr.path(), "view") {
            let parsed = parse_view_attr_tokens(
                attr.meta
                    .require_list()
                    .map(|list| list.tokens.clone())
                    .unwrap_or_default(),
            )?;
            if found.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "Duplicate #[view] marker on method",
                ));
            }
            found = Some(parsed);
            continue;
        }

        if path_ends_with_ident(attr.path(), "query") {
            let parsed = parse_query_attr_tokens(
                attr.meta
                    .require_list()
                    .map(|list| list.tokens.clone())
                    .unwrap_or_default(),
            )?;
            if found.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "Duplicate #[view]/#[query] marker on method",
                ));
            }
            found = Some(parsed);
            continue;
        }

        if attr.path().is_ident("doc") {
            if let Ok(marker) = attr.parse_args::<LitStr>() {
                let parsed = parse_view_doc_marker(&marker.value())
                    .or_else(|| parse_query_doc_marker(&marker.value()));
                if let Some(parsed) = parsed {
                    if found.is_some() {
                        return Err(syn::Error::new(
                            attr.span(),
                            "Duplicate view/query marker on method",
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

fn build_view_like_doc_marker(prefix: &str, marker: &ViewAttrOptions) -> String {
    let mut parts = Vec::<String>::new();
    if let Some(name) = marker.name.as_ref() {
        parts.push(format!("name={name}"));
    }
    parts.push(format!(
        "input={}",
        match marker.input {
            ViewInputMode::Query => "query",
            ViewInputMode::Body => "body",
        }
    ));
    if let Some(input_ty) = marker.input_ty.as_ref() {
        parts.push(format!("input_type={}", type_to_marker_string(input_ty)));
    }
    if let Some(output) = marker.output.as_ref() {
        parts.push(format!("output={}", type_to_marker_string(output)));
    }
    if parts.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}:{}", parts.join(";"))
    }
}

fn build_command_doc_marker(marker: &CommandAttrOptions) -> String {
    let mut parts = Vec::<String>::new();
    if let Some(name) = marker.name.as_ref() {
        parts.push(format!("name={name}"));
    }
    if !marker.idempotent {
        parts.push("idempotent=false".to_string());
    }
    if let Some(input) = marker.input.as_ref() {
        parts.push(format!("input={}", type_to_marker_string(input)));
    }
    if let Some(output) = marker.output.as_ref() {
        parts.push(format!("output={}", type_to_marker_string(output)));
    }
    if parts.is_empty() {
        "__rustmemodb_command".to_string()
    } else {
        format!("__rustmemodb_command:{}", parts.join(";"))
    }
}

fn type_to_marker_string(ty: &Type) -> String {
    quote!(#ty).to_string()
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

fn parse_autonomous_constructor_args(
    method: &ImplItemFn,
    model_ident: &Ident,
) -> syn::Result<Option<Vec<PersistentCommandArg>>> {
    if method.sig.ident != "new" {
        return Ok(None);
    }

    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new(
            method.sig.span(),
            "constructor `new` in #[expose_rest] impl must be synchronous",
        ));
    }
    if !method.sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            method.sig.generics.span(),
            "constructor `new` in #[expose_rest] impl cannot have generic parameters",
        ));
    }

    if method
        .sig
        .inputs
        .iter()
        .any(|input| matches!(input, FnArg::Receiver(_)))
    {
        return Ok(None);
    }

    if !returns_model_or_self(&method.sig.output, model_ident) {
        return Ok(None);
    }

    let mut args = Vec::new();
    for input in &method.sig.inputs {
        let FnArg::Typed(PatType { pat, ty, .. }) = input else {
            return Err(syn::Error::new(
                input.span(),
                "Unsupported constructor argument pattern",
            ));
        };
        let Pat::Ident(pat_ident) = pat.as_ref() else {
            return Err(syn::Error::new(
                pat.span(),
                "constructor arguments must be simple identifiers",
            ));
        };
        args.push(PersistentCommandArg {
            ident: pat_ident.ident.clone(),
            ty: (**ty).clone(),
        });
    }
    Ok(Some(args))
}

fn returns_model_or_self(output: &ReturnType, model_ident: &Ident) -> bool {
    let ReturnType::Type(_, ty) = output else {
        return false;
    };
    let Type::Path(type_path) = ty.as_ref() else {
        return false;
    };
    let Some(segment) = type_path.path.segments.last() else {
        return false;
    };
    segment.ident == "Self" || segment.ident == *model_ident
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
    extract_result_types(ty).map(|(ok, _)| ok)
}

fn extract_result_types(ty: &Type) -> Option<(Type, Type)> {
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
    let mut types = arguments.args.iter().filter_map(|arg| {
        if let syn::GenericArgument::Type(ty) = arg {
            Some(ty.clone())
        } else {
            None
        }
    });
    let ok_ty = types.next()?;
    let err_ty = types.next()?;
    Some((ok_ty, err_ty))
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
