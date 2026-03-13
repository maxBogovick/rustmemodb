use rustmemodb::PersistAutonomousModelHandle;
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};

#[domain(table = "compile_contract_boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct CompileContractBoard {
    name: String,
    active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, DomainError)]
#[allow(dead_code)]
enum CompileContractError {
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
}

impl std::fmt::Display for CompileContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for CompileContractError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Validate)]
struct RenameInput {
    #[validate(trim, non_empty, len_max = 64)]
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = CompileContractBoard, name = "summary")]
struct CompileContractBoardSummary {
    name: String,
    active: bool,
}

#[api(views(CompileContractBoardSummary))]
impl CompileContractBoard {
    pub fn new(name: String) -> Self {
        Self {
            name: name.trim().to_string(),
            active: false,
        }
    }

    #[command(validate = true)]
    pub fn rename(&mut self, input: RenameInput) -> Result<String, CompileContractError> {
        self.name = input.name;
        Ok(self.name.clone())
    }

    pub fn activate(&mut self) -> bool {
        self.active = true;
        self.active
    }

    pub fn current_name(&self) -> String {
        self.name.clone()
    }
}

#[domain(table = "compile_contract_explicit_boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct CompileExplicitBoard {
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = CompileExplicitBoard, name = "summary")]
struct CompileExplicitBoardSummary {
    name: String,
}

#[expose_rest(views(CompileExplicitBoardSummary))]
impl CompileExplicitBoard {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    #[command]
    pub fn rename(&mut self, name: String) {
        self.name = name.trim().to_string();
    }

    #[query]
    pub fn current_name(&self) -> String {
        self.name.clone()
    }
}

#[allow(dead_code)]
async fn compile_time_dx_contract_surface(
    app: PersistApp,
    handle: PersistAutonomousModelHandle<CompileContractBoard>,
    id: String,
) -> rustmemodb::Result<()> {
    let _router = rustmemodb::serve_domain!(app, CompileContractBoard, "compile_contract_boards")?;
    let _ = handle
        .rename(
            &id,
            RenameInput {
                name: "Next".to_string(),
            },
        )
        .await;
    let _ = handle.activate(&id).await;
    let _ = handle.view::<CompileContractBoardSummary>();
    let _ = handle
        .query()
        .where_eq("active", true)
        .sort_desc("name")
        .per_page(10)
        .fetch()
        .await;
    Ok(())
}

#[test]
fn dx_contract_compiles() {}
