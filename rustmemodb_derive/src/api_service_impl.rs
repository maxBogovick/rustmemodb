use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, HashSet};
use syn::{
    FnArg, Ident, ItemTrait, Pat, PatType, ReturnType, Signature, TraitItem, Type, spanned::Spanned,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    fn routing_fn_ident(self) -> Ident {
        match self {
            Self::Get => format_ident!("get"),
            Self::Post => format_ident!("post"),
            Self::Put => format_ident!("put"),
            Self::Patch => format_ident!("patch"),
            Self::Delete => format_ident!("delete"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VerbKind {
    Create,
    Read,
    UpdatePatch,
    UpdatePut,
    Delete,
    ActionPut,
    ActionPost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArgKind {
    Path,
    Query,
    Body,
    Unknown,
}

#[derive(Clone)]
struct MethodArg {
    ident: Ident,
    ty: Type,
    kind: ArgKind,
}

#[derive(Clone)]
struct ServiceMethod {
    ident: Ident,
    http_method: HttpMethod,
    path: String,
    args: Vec<MethodArg>,
    ok_type: Type,
    err_type: Option<Type>,
    success_status: u16,
}

struct PrimaryResource {
    singular: String,
    plural: String,
}

impl PrimaryResource {
    fn from_trait_ident(ident: &Ident) -> Self {
        let mut raw = ident.to_string();
        for suffix in ["Api", "Service", "Controller"] {
            if let Some(stripped) = raw.strip_suffix(suffix) {
                raw = stripped.to_string();
                break;
            }
        }

        let snake = to_snake_case(&raw);
        let tail = snake.split('_').next_back().unwrap_or("entity");
        let singular = singularize(tail);
        let plural = pluralize(&singular);
        Self { singular, plural }
    }
}

pub fn expand_api_service(input: ItemTrait) -> syn::Result<TokenStream> {
    ensure_async_trait_attribute(&input)?;

    let trait_ident = input.ident.clone();
    let vis = input.vis.clone();
    let server_struct_ident = format_ident!("{}Server", trait_ident);
    let primary = PrimaryResource::from_trait_ident(&trait_ident);

    let methods = parse_methods(&input, &primary)?;
    validate_route_collisions(&methods)?;
    let router_impl = generate_router(&methods);
    let handler_impls = generate_handlers(&server_struct_ident, &trait_ident, &methods);

    Ok(quote! {
        #input

        #vis struct #server_struct_ident<T> {
            inner: T,
        }

        impl<T: #trait_ident + Clone + Send + Sync + 'static> #server_struct_ident<T> {
            pub fn new(inner: T) -> Self {
                Self { inner }
            }

            #router_impl
        }

        #handler_impls
    })
}

fn ensure_async_trait_attribute(input: &ItemTrait) -> syn::Result<()> {
    let has_async_trait = input.attrs.iter().any(|attr| {
        attr.path()
            .segments
            .last()
            .map(|segment| segment.ident == "async_trait")
            .unwrap_or(false)
    });

    if has_async_trait {
        Ok(())
    } else {
        Err(syn::Error::new(
            input.ident.span(),
            "#[api_service] requires #[async_trait::async_trait] on the trait (and on its impl) so generated axum handlers have Send futures",
        ))
    }
}

fn parse_methods(
    trait_item: &ItemTrait,
    primary: &PrimaryResource,
) -> syn::Result<Vec<ServiceMethod>> {
    let mut methods = Vec::new();
    for item in &trait_item.items {
        if let TraitItem::Fn(method) = item {
            methods.push(analyze_method(&method.sig, primary)?);
        }
    }
    Ok(methods)
}

fn validate_route_collisions(methods: &[ServiceMethod]) -> syn::Result<()> {
    let mut seen = HashSet::<(String, HttpMethod)>::new();
    for method in methods {
        let key = (method.path.clone(), method.http_method);
        if !seen.insert(key.clone()) {
            return Err(syn::Error::new(
                method.ident.span(),
                format!(
                    "Duplicate generated route: {} {}",
                    http_method_name(key.1),
                    key.0
                ),
            ));
        }
    }
    Ok(())
}

fn analyze_method(sig: &Signature, primary: &PrimaryResource) -> syn::Result<ServiceMethod> {
    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            "#[api_service] methods must be async",
        ));
    }

    let mut inputs = sig.inputs.iter();
    match inputs.next() {
        Some(FnArg::Receiver(_)) => {}
        _ => {
            return Err(syn::Error::new(
                sig.span(),
                "#[api_service] methods must have &self receiver",
            ));
        }
    }

    let fn_name = sig.ident.to_string();
    let name_parts = fn_name
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let verb = name_parts.first().copied().unwrap_or("action");
    let target_token = name_parts
        .get(1)
        .copied()
        .unwrap_or(primary.singular.as_str());
    let target_singular = singularize(target_token);
    let verb_kind = classify_verb(verb);
    let http_method = http_method_for_verb(verb_kind);

    let mut args = Vec::<MethodArg>::new();
    for input in inputs {
        let FnArg::Typed(PatType { pat, ty, .. }) = input else {
            return Err(syn::Error::new(
                input.span(),
                "Unsupported method argument in #[api_service]",
            ));
        };
        let Pat::Ident(pat_ident) = pat.as_ref() else {
            return Err(syn::Error::new(
                pat.span(),
                "Only named arguments are supported in #[api_service] methods",
            ));
        };
        args.push(MethodArg {
            ident: pat_ident.ident.clone(),
            ty: (*ty.clone()),
            kind: ArgKind::Unknown,
        });
    }

    let (path, used_path_args) = build_route_path(
        primary,
        &target_singular,
        verb,
        verb_kind,
        args.iter()
            .map(|arg| arg.ident.to_string())
            .collect::<Vec<_>>()
            .as_slice(),
    );

    for arg in &mut args {
        if used_path_args.contains(&arg.ident.to_string()) {
            arg.kind = ArgKind::Path;
        }
    }

    let non_path_indices = args
        .iter()
        .enumerate()
        .filter_map(|(index, arg)| (arg.kind != ArgKind::Path).then_some(index))
        .collect::<Vec<_>>();
    if matches!(verb_kind, VerbKind::Read | VerbKind::Delete) {
        for index in non_path_indices {
            args[index].kind = ArgKind::Query;
        }
    } else {
        match non_path_indices.len() {
            0 => {}
            1 => args[non_path_indices[0]].kind = ArgKind::Body,
            _ => {
                for index in non_path_indices.iter().take(non_path_indices.len() - 1) {
                    args[*index].kind = ArgKind::Query;
                }
                args[*non_path_indices.last().expect("non-empty checked")].kind = ArgKind::Body;
            }
        }
    }

    let (ok_type, err_type) = parse_method_output(&sig.output);
    let success_status = success_status_code(verb_kind, is_unit_type(&ok_type));

    Ok(ServiceMethod {
        ident: sig.ident.clone(),
        http_method,
        path,
        args,
        ok_type,
        err_type,
        success_status,
    })
}

fn generate_router(methods: &[ServiceMethod]) -> TokenStream {
    let mut grouped = BTreeMap::<String, Vec<(HttpMethod, Ident)>>::new();
    for method in methods {
        grouped
            .entry(method.path.clone())
            .or_default()
            .push((method.http_method, format_ident!("handle_{}", method.ident)));
    }

    let route_entries = grouped.into_iter().map(|(path, bindings)| {
        let mut bindings = bindings.into_iter();
        let (first_method, first_handler) = bindings
            .next()
            .expect("api_service route groups are never empty");
        let first_verb = first_method.routing_fn_ident();
        let mut method_router = quote!(axum::routing::#first_verb(Self::#first_handler));
        for (http_method, handler_ident) in bindings {
            let verb = http_method.routing_fn_ident();
            method_router = quote!(#method_router.#verb(Self::#handler_ident));
        }
        quote! {
            .route(#path, #method_router)
        }
    });

    quote! {
        pub fn router(self) -> axum::Router {
            axum::Router::new()
                #(#route_entries)*
                .with_state(self.inner)
        }
    }
}

fn generate_handlers(
    server_struct: &Ident,
    trait_ident: &Ident,
    methods: &[ServiceMethod],
) -> TokenStream {
    let handlers = methods.iter().map(|method| {
        let method_ident = &method.ident;
        let handler_ident = format_ident!("handle_{}", method.ident);

        let path_args = method
            .args
            .iter()
            .filter(|arg| arg.kind == ArgKind::Path)
            .collect::<Vec<_>>();
        let query_args = method
            .args
            .iter()
            .filter(|arg| arg.kind == ArgKind::Query)
            .collect::<Vec<_>>();
        let body_args = method
            .args
            .iter()
            .filter(|arg| arg.kind == ArgKind::Body)
            .collect::<Vec<_>>();

        let mut extractors = Vec::<TokenStream>::new();
        match path_args.len() {
            0 => {}
            1 => {
                let arg_ident = &path_args[0].ident;
                let arg_ty = &path_args[0].ty;
                extractors.push(quote! {
                    axum::extract::Path(#arg_ident): axum::extract::Path<#arg_ty>
                });
            }
            _ => {
                let idents = path_args.iter().map(|arg| &arg.ident).collect::<Vec<_>>();
                let tys = path_args.iter().map(|arg| &arg.ty).collect::<Vec<_>>();
                extractors.push(quote! {
                    axum::extract::Path((#(#idents),*)): axum::extract::Path<(#(#tys),*)>
                });
            }
        }

        for arg in query_args {
            let arg_ident = &arg.ident;
            let arg_ty = &arg.ty;
            extractors.push(quote! {
                axum::extract::Query(#arg_ident): axum::extract::Query<#arg_ty>
            });
        }

        for arg in body_args {
            let arg_ident = &arg.ident;
            let arg_ty = &arg.ty;
            extractors.push(quote! {
                axum::extract::Json(#arg_ident): axum::extract::Json<#arg_ty>
            });
        }

        let extractor_tokens = if extractors.is_empty() {
            quote! {}
        } else {
            quote! {, #(#extractors),*}
        };

        let call_args = method.args.iter().map(|arg| {
            let ident = &arg.ident;
            quote!(#ident)
        });

        let success_response =
            success_response_tokens(method.success_status, is_unit_type(&method.ok_type));

        let body = if method.err_type.is_some() {
            if is_unit_type(&method.ok_type) {
                quote! {
                    match inner.#method_ident(#(#call_args),*).await {
                        Ok(_value) => #success_response,
                        Err(err) => {
                            let web_err: ::rustmemodb::web::WebError = err.into();
                            axum::response::IntoResponse::into_response(web_err)
                        }
                    }
                }
            } else {
                quote! {
                    match inner.#method_ident(#(#call_args),*).await {
                        Ok(val) => #success_response,
                        Err(err) => {
                            let web_err: ::rustmemodb::web::WebError = err.into();
                            axum::response::IntoResponse::into_response(web_err)
                        }
                    }
                }
            }
        } else if is_unit_type(&method.ok_type) {
            quote! {
                let _value = inner.#method_ident(#(#call_args),*).await;
                #success_response
            }
        } else {
            quote! {
                let val = inner.#method_ident(#(#call_args),*).await;
                #success_response
            }
        };

        quote! {
            async fn #handler_ident(
                axum::extract::State(inner): axum::extract::State<T>
                #extractor_tokens
            ) -> axum::response::Response {
                #body
            }
        }
    });

    quote! {
        impl<T: #trait_ident + Clone + Send + Sync + 'static> #server_struct<T> {
            #(#handlers)*
        }
    }
}

fn build_route_path(
    primary: &PrimaryResource,
    target_singular: &str,
    verb: &str,
    verb_kind: VerbKind,
    arg_names: &[String],
) -> (String, HashSet<String>) {
    let mut path = format!("/api/{}", primary.plural);
    let mut used = HashSet::<String>::new();
    let path_candidates = arg_names
        .iter()
        .filter(|name| is_path_arg_name(name))
        .cloned()
        .collect::<Vec<_>>();

    if target_singular == primary.singular {
        let primary_id = find_id_candidate(
            &path_candidates,
            &[format!("{}_id", primary.singular), "id".to_string()],
        );
        let needs_id = match verb_kind {
            VerbKind::Create => false,
            VerbKind::Read => verb != "list",
            _ => true,
        };
        if needs_id {
            if let Some(name) = primary_id {
                path.push_str(&format!("/:{}", name));
                used.insert(name);
            }
        }

        if matches!(verb_kind, VerbKind::ActionPut | VerbKind::ActionPost) {
            path.push('/');
            path.push_str(verb);
        }
    } else {
        let parent_name = find_id_candidate(
            &path_candidates,
            &[format!("{}_id", primary.singular), "id".to_string()],
        )
        .or_else(|| path_candidates.first().cloned());
        if let Some(parent) = parent_name {
            path.push_str(&format!("/:{}", parent));
            used.insert(parent);
        }

        path.push('/');
        path.push_str(&pluralize(target_singular));

        let child_name = find_id_candidate(&path_candidates, &[format!("{}_id", target_singular)]);
        let include_child_id = !matches!(verb_kind, VerbKind::Create)
            && !(matches!(verb_kind, VerbKind::Read) && verb == "list");
        if include_child_id {
            if let Some(child) = child_name {
                path.push_str(&format!("/:{}", child));
                used.insert(child);
            }
        }

        if matches!(verb_kind, VerbKind::ActionPut | VerbKind::ActionPost) {
            path.push('/');
            path.push_str(verb);
        }
    }

    for candidate in path_candidates {
        if !used.contains(&candidate) {
            path.push_str(&format!("/:{}", candidate));
            used.insert(candidate);
        }
    }

    (path, used)
}

fn parse_method_output(output: &ReturnType) -> (Type, Option<Type>) {
    match output {
        ReturnType::Default => (syn::parse_quote!(()), None),
        ReturnType::Type(_, ty) => {
            if let Some((ok, err)) = extract_result_types(ty) {
                (ok, Some(err))
            } else {
                ((**ty).clone(), None)
            }
        }
    }
}

fn extract_result_types(ty: &Type) -> Option<(Type, Type)> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    let ident = segment.ident.to_string();
    let looks_like_result = segment.ident == "Result" || ident.ends_with("Result");
    if !looks_like_result {
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
    let err_ty = types
        .next()
        .unwrap_or_else(|| syn::parse_quote!(::rustmemodb::web::WebError));
    Some((ok_ty, err_ty))
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn success_status_code(verb_kind: VerbKind, ok_is_unit: bool) -> u16 {
    match verb_kind {
        VerbKind::Create => 201,
        VerbKind::Delete => {
            if ok_is_unit {
                204
            } else {
                200
            }
        }
        VerbKind::UpdatePatch | VerbKind::UpdatePut | VerbKind::ActionPut => {
            if ok_is_unit {
                204
            } else {
                200
            }
        }
        VerbKind::Read | VerbKind::ActionPost => 200,
    }
}

fn success_response_tokens(status: u16, ok_is_unit: bool) -> TokenStream {
    let status_tokens = status_code_tokens(status);
    if ok_is_unit {
        quote!(axum::response::IntoResponse::into_response(#status_tokens))
    } else {
        quote!(axum::response::IntoResponse::into_response((#status_tokens, axum::Json(val))))
    }
}

fn status_code_tokens(status: u16) -> TokenStream {
    match status {
        200 => quote!(axum::http::StatusCode::OK),
        201 => quote!(axum::http::StatusCode::CREATED),
        204 => quote!(axum::http::StatusCode::NO_CONTENT),
        other => quote!(axum::http::StatusCode::from_u16(#other).expect("valid status code")),
    }
}

fn classify_verb(verb: &str) -> VerbKind {
    match verb {
        "create" | "add" | "register" | "insert" => VerbKind::Create,
        "get" | "fetch" | "read" | "list" | "search" | "find" => VerbKind::Read,
        "update" | "change" | "modify" | "patch" => VerbKind::UpdatePatch,
        "replace" | "put" => VerbKind::UpdatePut,
        "delete" | "remove" | "discard" => VerbKind::Delete,
        "move" => VerbKind::ActionPut,
        _ => VerbKind::ActionPost,
    }
}

fn http_method_for_verb(kind: VerbKind) -> HttpMethod {
    match kind {
        VerbKind::Create => HttpMethod::Post,
        VerbKind::Read => HttpMethod::Get,
        VerbKind::UpdatePatch => HttpMethod::Patch,
        VerbKind::UpdatePut => HttpMethod::Put,
        VerbKind::Delete => HttpMethod::Delete,
        VerbKind::ActionPut => HttpMethod::Put,
        VerbKind::ActionPost => HttpMethod::Post,
    }
}

fn http_method_name(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Patch => "PATCH",
        HttpMethod::Delete => "DELETE",
    }
}

fn is_path_arg_name(name: &str) -> bool {
    name == "id" || name.ends_with("_id")
}

fn find_id_candidate(candidates: &[String], preferred: &[String]) -> Option<String> {
    for target in preferred {
        if let Some(found) = candidates.iter().find(|name| *name == target) {
            return Some(found.clone());
        }
    }
    None
}

fn singularize(value: &str) -> String {
    if value.ends_with("ies") && value.len() > 3 {
        return format!("{}y", &value[..value.len() - 3]);
    }
    if value.ends_with('s') && !value.ends_with("ss") && value.len() > 1 {
        return value[..value.len() - 1].to_string();
    }
    value.to_string()
}

fn pluralize(value: &str) -> String {
    if value.ends_with('y')
        && value.len() > 1
        && !matches!(
            value.chars().nth(value.len() - 2),
            Some('a' | 'e' | 'i' | 'o' | 'u')
        )
    {
        return format!("{}ies", &value[..value.len() - 1]);
    }
    if value.ends_with('s') {
        return value.to_string();
    }
    format!("{value}s")
}

fn to_snake_case(value: &str) -> String {
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index != 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
