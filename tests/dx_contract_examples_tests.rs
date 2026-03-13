use std::path::Path;

#[test]
fn showcase_examples_follow_generated_rest_runtime_contract() {
    assert!(Path::new("examples/agile_board/src/model.rs").exists());
    assert!(Path::new("examples/agile_board/src/main.rs").exists());
    assert!(!Path::new("examples/agile_board/src/api.rs").exists());
    assert!(!Path::new("examples/agile_board/src/api_new.rs").exists());
    assert!(!Path::new("examples/agile_board/src/store.rs").exists());

    assert!(Path::new("examples/ledger_core/src/model.rs").exists());
    assert!(Path::new("examples/ledger_core/src/main.rs").exists());
    assert!(!Path::new("examples/ledger_core/src/api.rs").exists());
    assert!(!Path::new("examples/ledger_core/src/store.rs").exists());

    assert!(Path::new("examples/pulse_studio/src/model.rs").exists());
    assert!(Path::new("examples/pulse_studio/src/main.rs").exists());
    assert!(!Path::new("examples/pulse_studio/src/api.rs").exists());
    assert!(!Path::new("examples/pulse_studio/src/store.rs").exists());

    let agile_main = std::fs::read_to_string("examples/agile_board/src/main.rs")
        .expect("read agile_board main.rs");
    assert!(
        agile_main.contains("serve_domain!(app, Board, \"boards\")")
            || agile_main.contains("serve_autonomous_model::<Board>"),
        "agile_board runtime must mount generated autonomous router through high-level helper or direct API"
    );
    assert!(
        agile_main.contains("prelude::dx::PersistApp"),
        "agile_board runtime should use stable dx prelude path"
    );
    assert!(
        !agile_main.contains("register_view"),
        "agile_board runtime must not manually register view routes"
    );
    assert!(
        !agile_main.contains("serve_autonomous_model_with_view"),
        "agile_board runtime must not manually compose view router variants"
    );
    let agile_model = std::fs::read_to_string("examples/agile_board/src/model.rs")
        .expect("read agile_board model.rs");
    assert!(
        agile_model.contains("prelude::dx::*"),
        "agile_board model should use stable dx prelude path"
    );
    assert!(
        !agile_model.contains("snapshot_for_external_transaction"),
        "agile_board model must not orchestrate low-level persistence snapshots"
    );
    assert!(
        !agile_model.contains("execute_intent_if_match_auto_audit"),
        "agile_board model must not use low-level audit orchestration helpers"
    );

    let ledger_main = std::fs::read_to_string("examples/ledger_core/src/main.rs")
        .expect("read ledger_core main.rs");
    assert!(
        ledger_main.contains("serve_domain!(app, LedgerBook, \"ledgers\")")
            || ledger_main.contains("serve_autonomous_model::<LedgerBook>"),
        "ledger_core runtime must mount generated autonomous router through high-level helper or direct API"
    );
    assert!(
        ledger_main.contains("prelude::dx::PersistApp"),
        "ledger_core runtime should use stable dx prelude path"
    );
    assert!(
        !ledger_main.contains("register_view"),
        "ledger_core runtime must not manually register view routes"
    );
    assert!(
        !ledger_main.contains("serve_autonomous_model_with_view"),
        "ledger_core runtime must not manually compose view router variants"
    );
    let ledger_model = std::fs::read_to_string("examples/ledger_core/src/model.rs")
        .expect("read ledger_core model.rs");
    assert!(
        ledger_model.contains("prelude::dx::*"),
        "ledger_core model should use stable dx prelude path"
    );
    assert!(
        !ledger_model.contains("transaction_with_retry"),
        "ledger_core model must not orchestrate low-level retry/session flow"
    );

    let pulse_main = std::fs::read_to_string("examples/pulse_studio/src/main.rs")
        .expect("read pulse_studio main.rs");
    assert!(
        pulse_main.contains("serve_domain!(app, PulseWorkspace, \"workspaces\")")
            || pulse_main.contains("serve_autonomous_model::<PulseWorkspace>"),
        "pulse_studio runtime must mount generated autonomous router through high-level helper or direct API"
    );
    assert!(
        pulse_main.contains("prelude::dx::PersistApp"),
        "pulse_studio runtime should use stable dx prelude path"
    );
    assert!(
        !pulse_main.contains("register_view"),
        "pulse_studio runtime must not manually register view routes"
    );
    assert!(
        !pulse_main.contains("serve_autonomous_model_with_view"),
        "pulse_studio runtime must not manually compose view router variants"
    );
    let pulse_model = std::fs::read_to_string("examples/pulse_studio/src/model.rs")
        .expect("read pulse_studio model.rs");
    assert!(
        pulse_model.contains("prelude::dx::*"),
        "pulse_studio model should use stable dx prelude path"
    );
    assert!(
        !pulse_model.contains("snapshot_for_external_transaction"),
        "pulse_studio model must not orchestrate low-level persistence snapshots"
    );
    assert!(
        !pulse_model.contains("execute_intent_if_match_auto_audit"),
        "pulse_studio model must not use low-level audit orchestration helpers"
    );
    assert!(
        pulse_model.contains("#[api(views(") || pulse_model.contains("#[expose_rest(views("),
        "pulse_studio should demonstrate automatic typed view mounting from model declaration"
    );
}
