use rustmemodb::core::Value;
use rustmemodb::core::omni_entity::{FieldMeta, OmniEntityPatch, OmniSchema, OmniValue, SqlEntity};
use rustmemodb_derive::{OmniEntity, OmniValue};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, OmniValue)]
pub struct UserId(String);

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, OmniValue)]
pub struct DepartmentId(String);

#[derive(Debug, Clone, PartialEq, OmniEntity)]
#[omni(table_name = "departments")]
pub struct Department {
    #[omni(primary_key)]
    pub id: DepartmentId,
    pub title: String,
    #[omni(readonly)]
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, OmniEntity)]
#[omni(table_name = "users")]
pub struct User {
    #[omni(primary_key)]
    pub id: UserId,
    pub username: String,
    #[omni(hidden)]
    pub password_hash: String,
    pub department_id: Option<DepartmentId>,
}

#[tokio::test]
async fn test_omni_schema_generation() {
    assert_eq!(Department::table_name(), "departments");
    assert_eq!(User::table_name(), "users");

    let dept_fields = Department::fields();
    assert_eq!(dept_fields.len(), 3);
    assert_eq!(dept_fields[0].name, "id");
    assert_eq!(dept_fields[0].is_primary_key, true);
    assert_eq!(dept_fields[1].name, "title");
    assert_eq!(dept_fields[2].name, "created_at");
    assert_eq!(dept_fields[2].rest_readonly, true);

    let user_fields = User::fields();
    assert_eq!(user_fields.len(), 4);
    assert_eq!(user_fields[0].name, "id");
    assert_eq!(user_fields[2].name, "password_hash");
    assert_eq!(user_fields[2].rest_hidden, true);
    // Is nullable test (fallback for Option)
    assert_eq!(user_fields[3].name, "department_id");
    assert_eq!(user_fields[3].is_nullable, true);
}

#[tokio::test]
async fn test_sql_projection() {
    let dept_proj = Department::sql_projection("d");
    assert_eq!(
        dept_proj,
        "\"d\".\"id\" AS \"d__id\", \"d\".\"title\" AS \"d__title\", \"d\".\"created_at\" AS \"d__created_at\""
    );
}

#[tokio::test]
async fn test_from_sql_row_and_to_params() {
    let row = vec![
        Value::Text("dept-1".to_string()),
        Value::Text("Engineering".to_string()),
        Value::Text("2024-01-01".to_string()),
    ];

    let dept = Department::from_sql_row(&row, 0).unwrap().unwrap();
    assert_eq!(dept.id.0, "dept-1");
    assert_eq!(dept.title, "Engineering");

    let params = dept.to_sql_params();
    assert_eq!(params.len(), 3);
    assert_eq!(params[0], Value::Text("dept-1".to_string()));
    assert_eq!(params[1], Value::Text("Engineering".to_string()));
}

#[tokio::test]
async fn test_omni_patch() {
    let mut user = User {
        id: UserId("u1".into()),
        username: "alice".into(),
        password_hash: "hash".into(),
        department_id: None,
    };

    let patch = UserPatch {
        username: Some("alice_new".into()),
        password_hash: None,
        department_id: Some(Some(DepartmentId("d1".into()))),
    };

    patch.apply_to(&mut user);
    assert_eq!(user.username, "alice_new");
    assert_eq!(user.department_id, Some(DepartmentId("d1".into())));

    let changes = patch.changed_fields();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].0, "username");
    assert_eq!(changes[1].0, "department_id");
}

#[tokio::test]
async fn test_left_join_null_pk_skips_parsing() {
    // A raw query might return a row where a joined entity is not present.
    // The macro generates: if row[offset].is_null() return None
    let row = vec![
        Value::Text("u1".into()),
        Value::Text("alice".into()),
        Value::Text("hash".into()),
        Value::Text("d1".into()), // User fields
        Value::Null,              // Department.id is NULL -> means LEFT JOIN didn't find match!
        Value::Null,
        Value::Null,
    ];

    let user = User::from_sql_row(&row, 0).unwrap().unwrap();
    assert_eq!(user.username, "alice");

    let dept_opt = Department::from_sql_row(&row, 4).unwrap();
    assert!(dept_opt.is_none());
}

#[tokio::test]
async fn test_crud_methods() {
    let mut db = rustmemodb::facade::InMemoryDB::new();

    // Create schema
    db.execute("CREATE TABLE departments (id TEXT PRIMARY KEY, title TEXT, created_at TEXT)")
        .await
        .unwrap();

    let dept = Department {
        id: DepartmentId("d1".into()),
        title: "HR".into(),
        created_at: "now".into(),
    };

    // Test save
    dept.save(&mut db).await.unwrap();

    // Test find_by_pk
    let fetched = Department::find_by_pk(&db, DepartmentId("d1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.title, "HR");

    // Test update
    let mut to_update = fetched;
    to_update.title = "Finance".into();
    to_update.update(&mut db).await.unwrap();

    let fetched_updated = Department::find_by_pk(&db, DepartmentId("d1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_updated.title, "Finance");

    // Test delete_by_pk
    Department::delete_by_pk(&mut db, DepartmentId("d1".into()))
        .await
        .unwrap();
    let fetched_empty = Department::find_by_pk(&db, DepartmentId("d1".into()))
        .await
        .unwrap();
    assert!(fetched_empty.is_none());
}

#[derive(Debug, Clone, PartialEq, OmniEntity)]
#[omni(table_name = "sync_test_table")]
pub struct SyncEntityV1 {
    #[omni(primary_key)]
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, OmniEntity)]
#[omni(table_name = "sync_test_table")]
pub struct SyncEntityV2 {
    #[omni(primary_key)]
    pub id: String,
    pub name: String,
    #[omni(sql_type = "TEXT DEFAULT 'N/A'")]
    pub new_field: String,
}

#[tokio::test]
async fn test_sync_schema_migrations() {
    let mut db = rustmemodb::facade::InMemoryDB::new();

    // 1. Initial sync (should CREATE TABLE)
    SyncEntityV1::sync_schema(&mut db).await.unwrap();
    assert!(db.table_exists("sync_test_table"));

    // 2. Insert data using V1
    let v1 = SyncEntityV1 {
        id: "rec1".into(),
        name: "First Record".into(),
    };
    v1.save(&mut db).await.unwrap();

    // 3. Sync V2 (should ALTER TABLE ADD COLUMN)
    SyncEntityV2::sync_schema(&mut db).await.unwrap();

    // 4. Fetch using V2 and check default value of new field
    let fetched_v2 = SyncEntityV2::find_by_pk(&db, "rec1".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched_v2.name, "First Record");
    assert_eq!(fetched_v2.new_field, "N/A"); // Default value propagated
}
