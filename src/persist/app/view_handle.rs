impl<M, V> PersistViewHandle<M, V>
where
    M: PersistAutonomousModel,
    V: PersistView<M>,
    M::Persisted: Clone + PersistEntityFactory,
{
    pub(crate) fn new(model: PersistAutonomousModelHandle<M>) -> Self {
        Self {
            model,
            marker: PhantomData,
        }
    }

    /// Computes one registered view for a specific aggregate id.
    pub async fn get(&self, persist_id: impl AsRef<str>) -> std::result::Result<V, PersistDomainError> {
        let id = persist_id.as_ref();
        let record = self.model.get_one(id).await.ok_or(PersistDomainError::NotFound)?;
        Ok(V::compute(&record.model))
    }

    /// Builds an axum router exposing this view at `/:id/views/<name>`.
    pub fn mount_router(self) -> axum::Router {
        let route = format!("/:id/views/{}", V::VIEW_NAME);
        axum::Router::new()
            .route(&route, axum::routing::get(Self::handle_get))
            .with_state(self)
    }

    /// Merges this view route into an existing generated model router.
    pub fn mount_into_router(self, router: axum::Router) -> axum::Router {
        router.merge(self.mount_router())
    }

    async fn handle_get(
        axum::extract::State(handle): axum::extract::State<Self>,
        axum::extract::Path(id): axum::extract::Path<String>,
    ) -> axum::response::Response {
        match handle.get(&id).await {
            Ok(view) => axum::response::IntoResponse::into_response((
                axum::http::StatusCode::OK,
                axum::Json(view),
            )),
            Err(err) => {
                let service_error = crate::PersistServiceError::from_domain_for("entity", &id, err);
                let web_error: crate::web::WebError = service_error.into();
                axum::response::IntoResponse::into_response(web_error)
            }
        }
    }
}
