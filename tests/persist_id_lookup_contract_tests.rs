use std::fs;

#[test]
fn managed_and_core_id_paths_avoid_linear_find_position_scans() {
    let files = [
        "src/persist/app/managed_vec/indexed_crud/validation_and_reads.rs",
        "src/persist/app/managed_vec/indexed_crud/update_paths.rs",
        "src/persist/app/managed_vec/indexed_crud/delete_paths.rs",
        "src/persist/app/managed_vec/command_model.rs",
        "src/persist/core/persist_vec_impl/basics_and_io.rs",
    ];

    let banned_patterns = [
        ".position(|item| item.persist_id()",
        ".find(|item| item.persist_id()",
    ];

    for path in files {
        let source = fs::read_to_string(path).unwrap_or_else(|err| {
            panic!("failed to read '{path}' for id-lookup contract check: {err}")
        });

        for pattern in banned_patterns {
            assert!(
                !source.contains(pattern),
                "file '{path}' contains banned linear id-scan pattern: {pattern}"
            );
        }
    }
}
