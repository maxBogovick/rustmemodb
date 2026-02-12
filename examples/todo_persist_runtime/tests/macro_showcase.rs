use todo_persist_runtime::macro_showcase::run_macro_showcase;

#[tokio::test]
async fn macro_showcase_runs_and_restores_data() {
    let summary = run_macro_showcase()
        .await
        .expect("macro showcase should run");

    assert_eq!(summary.todo_count, 2);
    assert_eq!(summary.tag_count, 1);
    assert_eq!(summary.restored_todo_count, 2);
    assert_eq!(summary.invoked, 1);
    assert_eq!(summary.skipped, 1);
}
