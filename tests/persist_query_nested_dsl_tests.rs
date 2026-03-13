use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NestedTask {
    id: String,
    title: String,
    points: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct NestedColumn {
    id: String,
    title: String,
    tasks: Vec<NestedTask>,
}

#[domain(table = "dsl_nested_boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DslNestedBoard {
    name: String,
    columns: PersistJson<Vec<NestedColumn>>,
}

#[api]
impl DslNestedBoard {
    pub fn new(name: String) -> Self {
        Self {
            name: name.trim().to_string(),
            columns: PersistJson::default(),
        }
    }
}

#[domain(table = "dsl_query_workspaces", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DslQueryWorkspace {
    name: String,
    tier: String,
    score: i64,
    active: bool,
}

#[api]
impl DslQueryWorkspace {
    pub fn new(name: String, tier: String, score: i64, active: bool) -> Self {
        Self {
            name: name.trim().to_string(),
            tier: tier.trim().to_string(),
            score,
            active,
        }
    }
}

#[tokio::test]
async fn nested_mutation_api_updates_complex_graph_without_manual_loops() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("nested_dsl"))
        .await
        .expect("open app");
    let boards = app
        .open_autonomous_model::<DslNestedBoard>("dsl_nested_boards")
        .await
        .expect("open boards");

    let created = boards
        .create_one(DslNestedBoard::new("Roadmap".to_string()))
        .await
        .expect("create board");
    let board_id = created.persist_id;

    boards
        .nested_push(
            &board_id,
            "columns",
            NestedColumn {
                id: "todo".to_string(),
                title: "Todo".to_string(),
                tasks: Vec::new(),
            },
        )
        .await
        .expect("add todo column");
    boards
        .nested_push(
            &board_id,
            "columns",
            NestedColumn {
                id: "doing".to_string(),
                title: "Doing".to_string(),
                tasks: Vec::new(),
            },
        )
        .await
        .expect("add doing column");
    boards
        .nested_push(
            &board_id,
            "columns.0.tasks",
            NestedTask {
                id: "task-1".to_string(),
                title: "Design API".to_string(),
                points: 3,
            },
        )
        .await
        .expect("add task-1");
    boards
        .nested_push(
            &board_id,
            "columns.0.tasks",
            NestedTask {
                id: "task-2".to_string(),
                title: "Ship docs".to_string(),
                points: 2,
            },
        )
        .await
        .expect("add task-2");
    boards
        .nested_move_where_eq(
            &board_id,
            "columns.0.tasks",
            "columns.1.tasks",
            "id",
            "task-2",
        )
        .await
        .expect("move task-2");
    boards
        .nested_patch_where_eq(
            &board_id,
            "columns",
            "id",
            "doing",
            json!({"title": "In Progress"}),
        )
        .await
        .expect("patch doing column");
    boards
        .nested_remove_where_eq(&board_id, "columns.1.tasks", "id", "task-2")
        .await
        .expect("remove moved task-2");

    let board = boards.get_one(&board_id).await.expect("load board").model;
    assert_eq!(board.columns.len(), 2);
    assert_eq!(board.columns[0].id, "todo");
    assert_eq!(board.columns[0].tasks.len(), 1);
    assert_eq!(board.columns[0].tasks[0].id, "task-1");
    assert_eq!(board.columns[1].id, "doing");
    assert_eq!(board.columns[1].title, "In Progress");
    assert!(board.columns[1].tasks.is_empty());
}

#[tokio::test]
async fn declarative_query_dsl_filters_sorts_and_pages() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("query_dsl"))
        .await
        .expect("open app");
    let workspaces = app
        .open_autonomous_model::<DslQueryWorkspace>("dsl_query_workspaces")
        .await
        .expect("open workspaces");

    let dataset = [
        ("Acme", "pro", 91, true),
        ("Beacon", "free", 77, true),
        ("Core", "pro", 65, true),
        ("Delta", "pro", 40, false),
        ("Echo", "pro", 82, true),
    ];
    for (name, tier, score, active) in dataset {
        workspaces
            .create_one(DslQueryWorkspace::new(
                name.to_string(),
                tier.to_string(),
                score,
                active,
            ))
            .await
            .expect("create workspace");
    }

    let page1 = workspaces
        .query()
        .where_eq("tier", "pro")
        .where_eq("active", true)
        .where_gte("score", 60)
        .sort_desc("score")
        .page(1)
        .per_page(2)
        .fetch()
        .await;

    assert_eq!(page1.total, 3);
    assert_eq!(page1.total_pages, 2);
    assert_eq!(page1.items.len(), 2);
    assert_eq!(page1.items[0].model.name, "Acme");
    assert_eq!(page1.items[1].model.name, "Echo");

    let page2 = workspaces
        .query()
        .where_eq("tier", "pro")
        .where_eq("active", true)
        .where_gte("score", 60)
        .sort_desc("score")
        .page(2)
        .per_page(2)
        .fetch()
        .await;

    assert_eq!(page2.total, 3);
    assert_eq!(page2.items.len(), 1);
    assert_eq!(page2.items[0].model.name, "Core");
}
