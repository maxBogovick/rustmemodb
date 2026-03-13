use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields};

pub fn expand_omni_entity(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let patch_name = syn::Ident::new(&format!("{}Patch", name), name.span());

    // Simple snake_case defaults
    let default_table_name = name.to_string().to_lowercase();
    let mut table_name = default_table_name;

    for attr in &input.attrs {
        if attr.path().is_ident("omni") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table_name") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    table_name = lit.value();
                }
                Ok(())
            })?;
        }
    }

    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            name,
            "OmniEntity can only be derived for structs",
        ));
    };

    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(name, "OmniEntity requires named fields"));
    };

    let mut field_metas = Vec::new();
    let mut from_sql_assignments = Vec::new();
    let mut to_sql_pushes = Vec::new();
    let mut patch_fields = Vec::new();
    let mut patch_apply = Vec::new();
    let mut patch_changed = Vec::new();

    let mut pk_index = None;
    let mut pk_name = None;
    let mut pk_type = None;
    let mut field_names_str = Vec::new();

    for (i, field) in fields.named.iter().enumerate() {
        let field_name = field.ident.as_ref().unwrap();
        let field_str = field_name.to_string();
        let ty = &field.ty;

        let mut sql_type = "TEXT".to_string();
        let mut is_primary_key = false;
        let mut is_nullable = false;
        let mut rest_readonly = false;
        let mut rest_hidden = false;

        for attr in &field.attrs {
            if attr.path().is_ident("omni") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("sql_type") {
                        let value = meta.value()?;
                        let lit: syn::LitStr = value.parse()?;
                        sql_type = lit.value();
                    } else if meta.path.is_ident("primary_key") {
                        is_primary_key = true;
                    } else if meta.path.is_ident("readonly") {
                        rest_readonly = true;
                    } else if meta.path.is_ident("hidden") {
                        rest_hidden = true;
                    }
                    Ok(())
                })?;
            }
        }

        if is_primary_key {
            pk_index = Some(i);
            pk_name = Some(field_name.clone());
            pk_type = Some(ty.clone());
        }

        field_names_str.push(field_str.clone());

        // Simplistic check for Option<T>
        let ty_str = quote!(#ty).to_string().replace(" ", "");
        if ty_str.starts_with("Option<") {
            is_nullable = true;
        }

        field_metas.push(quote! {
            rustmemodb::core::omni_entity::FieldMeta {
                name: #field_str,
                sql_type: #sql_type,
                is_primary_key: #is_primary_key,
                is_nullable: #is_nullable,
                rest_readonly: #rest_readonly,
                rest_hidden: #rest_hidden,
            }
        });

        from_sql_assignments.push(quote! {
            #field_name: {
                let val = row.get(offset + #i).cloned().unwrap_or(rustmemodb::core::Value::Null);
                // A complete implementation would handle Option transparently via trait specialization,
                // but we rely on OmniValue to know how to parse the wrapped type or the Option itself.
                // For MVP: assume `ty` implements OmniValue.
                rustmemodb::core::omni_entity::OmniValue::from_db_value(val)?
            }
        });

        to_sql_pushes.push(quote! {
            rustmemodb::core::omni_entity::OmniValue::into_db_value(self.#field_name.clone())
        });

        if !is_primary_key && !rest_readonly {
            patch_fields.push(quote! {
                pub #field_name: Option<#ty>
            });

            patch_apply.push(quote! {
                if let Some(val) = &self.#field_name {
                    target.#field_name = val.clone();
                }
            });

            patch_changed.push(quote! {
                if let Some(val) = &self.#field_name {
                    changes.push((#field_str, rustmemodb::core::omni_entity::OmniValue::into_db_value(val.clone())));
                }
            });
        }
    }

    let check_null_idx = pk_index.unwrap_or(0);
    let null_check = quote! {
        if offset + #check_null_idx < row.len() {
            if matches!(row[offset + #check_null_idx], rustmemodb::core::Value::Null) {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    };

    let crud_impl = if let (Some(pk_n), Some(pk_t)) = (pk_name, pk_type) {
        let pk_str = pk_n.to_string();
        let fields_csv = field_names_str.join(", ");
        let placeholders = (1..=fields.named.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(", ");

        let projection_t = field_names_str
            .iter()
            .map(|name| format!("\"t\".\"{name}\" AS \"t__{name}\""))
            .collect::<Vec<_>>()
            .join(", ");

        let select_query = format!(
            "SELECT {} FROM \"{}\" t WHERE t.\"{}\" = $1",
            projection_t, table_name, pk_str
        );
        let delete_query = format!("DELETE FROM \"{}\" WHERE \"{}\" = $1", table_name, pk_str);
        let insert_query = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table_name, fields_csv, placeholders
        );

        let update_set_clause = field_names_str
            .iter()
            .enumerate()
            .filter(|(idx, _)| Some(*idx) != pk_index)
            .map(|(idx, name)| format!("{} = ${}", name, idx + 1))
            .collect::<Vec<_>>()
            .join(", ");

        let update_query = format!(
            "UPDATE \"{}\" SET {} WHERE \"{}\" = ${}",
            table_name,
            update_set_clause,
            pk_str,
            pk_index.unwrap() + 1
        );

        quote! {
            impl #name {
                pub async fn find_by_pk(db: &rustmemodb::facade::InMemoryDB, pk: #pk_t) -> rustmemodb::core::Result<Option<Self>> {
                    use rustmemodb::core::omni_entity::{OmniQueryExt, OmniSchema, SqlEntity, OmniValue};
                    db.query_as::<Self>().with_sql(#select_query).with_param(pk).fetch_optional().await
                }

                pub async fn delete_by_pk(db: &mut rustmemodb::facade::InMemoryDB, pk: #pk_t) -> rustmemodb::core::Result<()> {
                    use rustmemodb::core::omni_entity::{OmniSchema, OmniValue};
                    db.execute_with_params(#delete_query, None, vec![pk.into_db_value()]).await?;
                    Ok(())
                }

                pub async fn save(&self, db: &mut rustmemodb::facade::InMemoryDB) -> rustmemodb::core::Result<()> {
                    use rustmemodb::core::omni_entity::{OmniSchema, SqlEntity};
                    db.execute_with_params(#insert_query, None, self.to_sql_params()).await?;
                    Ok(())
                }

                pub async fn update(&self, db: &mut rustmemodb::facade::InMemoryDB) -> rustmemodb::core::Result<()> {
                    use rustmemodb::core::omni_entity::{OmniSchema, SqlEntity};
                    db.execute_with_params(#update_query, None, self.to_sql_params()).await?;
                    Ok(())
                }

                pub async fn sync_schema(db: &mut rustmemodb::facade::InMemoryDB) -> rustmemodb::core::Result<()> {
                    use rustmemodb::core::omni_entity::OmniSchema;
                    let table_name = Self::table_name();

                    if !db.table_exists(table_name) {
                        let mut columns = Vec::new();
                        for field in Self::fields() {
                            let mut col_def = format!("\"{}\" {}", field.name, field.sql_type);
                            if field.is_primary_key {
                                col_def.push_str(" PRIMARY KEY");
                            }
                            columns.push(col_def);
                        }
                        let sql = format!("CREATE TABLE \"{}\" ({})", table_name, columns.join(", "));
                        db.execute(&sql).await.map_err(|e| rustmemodb::core::DbError::ExecutionError(e.to_string()))?;
                    } else {
                        let schema = db.get_table_schema(table_name).await.map_err(|e| rustmemodb::core::DbError::ExecutionError(e.to_string()))?;
                        let existing_cols = schema.schema().columns();

                        for field in Self::fields() {
                            let exists = existing_cols.iter().any(|c| c.name == field.name);
                            if !exists {
                                let sql = format!("ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}", table_name, field.name, field.sql_type);
                                db.execute(&sql).await.map_err(|e| rustmemodb::core::DbError::ExecutionError(e.to_string()))?;
                            }
                        }
                    }
                    Ok(())
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        impl rustmemodb::core::omni_entity::OmniSchema for #name {
            fn table_name() -> &'static str {
                #table_name
            }

            fn fields() -> &'static [rustmemodb::core::omni_entity::FieldMeta] {
                static FIELDS: &[rustmemodb::core::omni_entity::FieldMeta] = &[
                    #(#field_metas),*
                ];
                FIELDS
            }
        }

        impl rustmemodb::core::omni_entity::SqlEntity for #name {
            fn from_sql_row(row: &[rustmemodb::core::Value], offset: usize) -> Result<Option<Self>, String> {
                if row.len() <= offset {
                    return Ok(None);
                }

                #null_check

                Ok(Some(Self {
                    #(#from_sql_assignments),*
                }))
            }

            fn to_sql_params(&self) -> Vec<rustmemodb::core::Value> {
                vec![
                    #(#to_sql_pushes),*
                ]
            }
        }

        // Implement the Patch struct
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct #patch_name {
            #(#patch_fields),*
        }

        impl rustmemodb::core::omni_entity::OmniEntityPatch for #patch_name {
            type Target = #name;

            fn apply_to(&self, target: &mut Self::Target) {
                #(#patch_apply)*
            }

            fn changed_fields(&self) -> Vec<(&'static str, rustmemodb::core::Value)> {
                let mut changes = Vec::new();
                #(#patch_changed)*
                changes
            }
        }

        #crud_impl
    };

    Ok(expanded)
}
