//! Recommended API entrypoints grouped by abstraction level.
//!
//! `dx` is the stable default for business-logic-first applications.
//! `advanced` is an explicit escape hatch for low-level persistence internals.

pub mod dx {
    //! Stable high-level surface for autonomous/domain-first applications.
    //!
    //! Intended usage in app code:
    //! - domain model derives + REST exposure attributes,
    //! - `PersistApp` bootstrap,
    //! - `PersistJson<T>` for nested persisted JSON data.
    pub use crate::{
        AgentCommandOptions, AgentIncidentForensicsReport, AgentReplayRunOptions,
        AgentReplayRunReport, AgentReplayStepReport, AgentSessionMemory, AgentSessionRuntime,
        AgentSessionRuntimeConfig, AgentTimelineQuery, AgentWorkflowExecutor, AgentWorkflowStep,
        ApiError, Autonomous, DomainError, PersistApp, PersistInputValidate, PersistJson,
        PersistJsonValue, PersistQueryBuilder, PersistQueryFilter, PersistQueryOp,
        PersistQuerySort, PersistQuerySortDirection, PersistQuerySpec, PersistView,
        PersistViewHandle, Validate, api, command, domain, expose_rest, query, serve_domain,
        serve_domain_with_view, view,
    };

    #[cfg(feature = "unistructgen")]
    pub use crate::generate_struct_from_json;
}

pub mod advanced {
    //! Escape hatch for advanced persistence internals.
    //!
    //! App-level product code should normally stay on `prelude::dx`.
    pub use crate::persist;
    pub use crate::persist::app::{
        PersistAggregateStore, PersistAutonomousModelHandle, PersistDomainHandle, PersistTx,
    };
}
