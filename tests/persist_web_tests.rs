use rustmemodb::{DbError, map_conflict_problem};
use rustmemodb::{
    IDEMPOTENCY_KEY_INVALID_MESSAGE, IDEMPOTENCY_KEY_MAX_LEN, IDEMPOTENCY_KEY_TOO_LONG_MESSAGE,
    IF_MATCH_INVALID_VERSION_MESSAGE, IF_MATCH_REQUIRED_MESSAGE, PersistWebProblem,
    normalize_idempotency_key, normalize_request_id, parse_if_match_header,
};

#[test]
fn parse_if_match_header_accepts_plain_and_quoted_numbers() {
    assert_eq!(parse_if_match_header(Some("5")).unwrap(), 5);
    assert_eq!(parse_if_match_header(Some("\"7\"")).unwrap(), 7);
    assert_eq!(parse_if_match_header(Some("  \"11\" ")).unwrap(), 11);
}

#[test]
fn parse_if_match_header_rejects_missing_or_invalid_versions() {
    let missing = parse_if_match_header(None).unwrap_err();
    assert_eq!(missing.message(), IF_MATCH_REQUIRED_MESSAGE);

    let invalid = parse_if_match_header(Some("abc")).unwrap_err();
    assert_eq!(invalid.message(), IF_MATCH_INVALID_VERSION_MESSAGE);

    let non_positive = parse_if_match_header(Some("0")).unwrap_err();
    assert_eq!(non_positive.message(), IF_MATCH_INVALID_VERSION_MESSAGE);
}

#[test]
fn normalize_idempotency_key_handles_empty_ascii_and_len_constraints() {
    assert_eq!(normalize_idempotency_key(None).unwrap(), None);
    assert_eq!(normalize_idempotency_key(Some("   ")).unwrap(), None);
    assert_eq!(
        normalize_idempotency_key(Some(" key-1 ")).unwrap(),
        Some("key-1".to_string())
    );

    let non_ascii = normalize_idempotency_key(Some("ключ")).unwrap_err();
    assert_eq!(non_ascii.message(), IDEMPOTENCY_KEY_INVALID_MESSAGE);

    let too_long = "a".repeat(IDEMPOTENCY_KEY_MAX_LEN + 1);
    let err = normalize_idempotency_key(Some(&too_long)).unwrap_err();
    assert_eq!(err.message(), IDEMPOTENCY_KEY_TOO_LONG_MESSAGE);
}

#[test]
fn normalize_request_id_trims_and_drops_empty_values() {
    assert_eq!(normalize_request_id(None), None);
    assert_eq!(normalize_request_id(Some("   ")), None);
    assert_eq!(
        normalize_request_id(Some(" req-42 ")),
        Some("req-42".to_string())
    );
}

#[test]
fn map_conflict_problem_maps_supported_conflicts() {
    let optimistic = DbError::ExecutionError("optimistic lock conflict for users:123".to_string());
    assert_eq!(
        map_conflict_problem(&optimistic),
        Some(PersistWebProblem {
            status: 409,
            title: "Optimistic lock conflict",
            code: "optimistic_lock_conflict",
        })
    );

    let unique = DbError::ConstraintViolation("unique constraint failed".to_string());
    assert_eq!(
        map_conflict_problem(&unique),
        Some(PersistWebProblem {
            status: 409,
            title: "Unique constraint conflict",
            code: "unique_key_conflict",
        })
    );

    let non_conflict = DbError::ExecutionError("something else".to_string());
    assert_eq!(map_conflict_problem(&non_conflict), None);
}
