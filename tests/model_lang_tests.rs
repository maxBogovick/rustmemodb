use rustmemodb::core::{DbError, Result, Value};
use rustmemodb::{InMemoryDB, ModelProgram};

#[tokio::test]
async fn model_lang_materializes_tables_and_enforces_fk() -> Result<()> {
    let mut db = InMemoryDB::new();

    let source = r#"
        struct users {
          id: int pk
          name: text not_null
        }

        struct posts {
          id: int pk
          author: users not_null
          title: text
        }
    "#;

    let program = ModelProgram::parse(source)?;
    let statements = program.materialize(&mut db).await?;

    assert_eq!(statements.len(), 2);
    assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS users"));
    assert!(statements[1].contains("CREATE TABLE IF NOT EXISTS posts"));

    db.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
    db.execute("INSERT INTO posts VALUES (10, 1, 'hello')")
        .await?;

    let selected = db
        .execute("SELECT author_id, title FROM posts WHERE id = 10")
        .await?;
    assert_eq!(selected.row_count(), 1);
    assert_eq!(selected.rows()[0][0], Value::Integer(1));
    assert_eq!(selected.rows()[0][1], Value::Text("hello".to_string()));

    let err = db
        .execute("INSERT INTO posts VALUES (11, 999, 'ghost')")
        .await
        .unwrap_err();
    match err {
        DbError::ConstraintViolation(msg) => assert!(msg.contains("references non-existent key")),
        _ => panic!("Expected foreign key violation, got {:?}", err),
    }

    Ok(())
}

#[tokio::test]
async fn model_lang_rejects_bidirectional_composition() {
    let source = r#"
        struct parent {
          id: int pk
          child: child
        }

        struct child {
          id: int pk
          parent_ref: parent
        }
    "#;

    let program = ModelProgram::parse(source).unwrap();
    let err = program.to_create_table_sql().unwrap_err();

    match err {
        DbError::ExecutionError(msg) => assert!(msg.contains("Composition cycle detected")),
        _ => panic!("Expected cycle error, got {:?}", err),
    }
}
