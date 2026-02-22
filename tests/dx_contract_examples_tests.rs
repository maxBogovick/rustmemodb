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

    let agile_main = std::fs::read_to_string("examples/agile_board/src/main.rs")
        .expect("read agile_board main.rs");
    assert!(
        agile_main.contains("serve_autonomous_model::<Board>"),
        "agile_board runtime must mount generated autonomous router"
    );
    assert!(
        agile_main.contains("prelude::dx::PersistApp"),
        "agile_board runtime should use stable dx prelude path"
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
        ledger_main.contains("serve_autonomous_model::<LedgerBook>"),
        "ledger_core runtime must mount generated autonomous router"
    );
    assert!(
        ledger_main.contains("prelude::dx::PersistApp"),
        "ledger_core runtime should use stable dx prelude path"
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
}
