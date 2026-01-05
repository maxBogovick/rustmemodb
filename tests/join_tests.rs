use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

async fn setup_db() -> InMemoryDB {
    let mut db = InMemoryDB::new();

    // Create 'users' table
    db.execute("CREATE TABLE users (id INTEGER, name TEXT, department_id INTEGER)").await.unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 1)").await.unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 1)").await.unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 2)").await.unwrap();
    db.execute("INSERT INTO users VALUES (4, 'David', NULL)").await.unwrap(); // No department

    // Create 'departments' table
    db.execute("CREATE TABLE departments (id INTEGER, name TEXT)").await.unwrap();
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')").await.unwrap();
    db.execute("INSERT INTO departments VALUES (2, 'Sales')").await.unwrap();
    db.execute("INSERT INTO departments VALUES (3, 'Marketing')").await.unwrap(); // No users

    db
}

#[tokio::test]
async fn test_inner_join() {
    let mut db = setup_db().await;

    // SELECT users.name, departments.name 
    // FROM users 
    // INNER JOIN departments ON users.department_id = departments.id
    let result = db.execute(
        "SELECT users.name, departments.name 
         FROM users 
         INNER JOIN departments ON users.department_id = departments.id
         ORDER BY users.name"
    ).await.unwrap();

    assert_eq!(result.row_count(), 3);
    
    let rows = result.rows();
    assert_eq!(rows[0][0], Value::Text("Alice".into()));
    assert_eq!(rows[0][1], Value::Text("Engineering".into()));
    
    assert_eq!(rows[1][0], Value::Text("Bob".into()));
    assert_eq!(rows[1][1], Value::Text("Engineering".into()));
    
    assert_eq!(rows[2][0], Value::Text("Charlie".into()));
    assert_eq!(rows[2][1], Value::Text("Sales".into()));
}

#[tokio::test]
async fn test_left_join() {
    let mut db = setup_db().await;

    // SELECT users.name, departments.name 
    // FROM users 
    // LEFT JOIN departments ON users.department_id = departments.id
    let result = db.execute(
        "SELECT u.name, d.name
         FROM users u
         LEFT JOIN departments d ON u.department_id = d.id
         ORDER BY u.name"
    ).await.unwrap();

    assert_eq!(result.row_count(), 4); // All users
    
    let rows = result.rows();
    // Alice -> Engineering
    assert_eq!(rows[0][0], Value::Text("Alice".into()));
    assert_eq!(rows[0][1], Value::Text("Engineering".into()));
    
    // Bob -> Engineering
    assert_eq!(rows[1][0], Value::Text("Bob".into()));
    
    // Charlie -> Sales
    assert_eq!(rows[2][0], Value::Text("Charlie".into()));
    
    // David -> NULL (no department)
    assert_eq!(rows[3][0], Value::Text("David".into()));
    assert_eq!(rows[3][1], Value::Null);
}

#[tokio::test]
async fn test_right_join() {
    let mut db = setup_db().await;

    // SELECT users.name, departments.name 
    // FROM users 
    // RIGHT JOIN departments ON users.department_id = departments.id
    let result = db.execute(
        "SELECT users.name, departments.name 
         FROM users 
         RIGHT JOIN departments ON users.department_id = departments.id
         ORDER BY departments.name"
    ).await.unwrap();

    assert_eq!(result.row_count(), 4);
    
    // Find Marketing row
    let marketing_row = result.rows().iter().find(|r| r[1] == Value::Text("Marketing".into()));
    assert!(marketing_row.is_some());
    assert_eq!(marketing_row.unwrap()[0], Value::Null);
}

#[tokio::test]
async fn test_cross_join() {
    let mut db = setup_db().await;

    // SELECT * FROM users CROSS JOIN departments
    let result = db.execute("SELECT * FROM users CROSS JOIN departments").await.unwrap();
    assert_eq!(result.row_count(), 12);
}

#[tokio::test]
async fn test_self_join() {
    let mut db = InMemoryDB::new();
    
    // Employees with managers
    db.execute("CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER)").await.unwrap();
    db.execute("INSERT INTO employees VALUES (1, 'Boss', NULL)").await.unwrap();
    db.execute("INSERT INTO employees VALUES (2, 'Manager', 1)").await.unwrap();
    db.execute("INSERT INTO employees VALUES (3, 'Worker', 2)").await.unwrap();

    // SELECT e.name, m.name 
    // FROM employees e 
    // JOIN employees m ON e.manager_id = m.id
    let result = db.execute(
        "SELECT e.name, m.name 
         FROM employees AS e 
         JOIN employees AS m ON e.manager_id = m.id
         ORDER BY e.name"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2);
    
    let rows = result.rows();
    // Manager -> Boss
    assert_eq!(rows[0][0], Value::Text("Manager".into()));
    assert_eq!(rows[0][1], Value::Text("Boss".into()));
    
    // Worker -> Manager
    assert_eq!(rows[1][0], Value::Text("Worker".into()));
    assert_eq!(rows[1][1], Value::Text("Manager".into()));
}

#[tokio::test]
async fn test_complex_join_with_where() {
    let mut db = setup_db().await;

    // Join with WHERE clause
    let result = db.execute(
        "SELECT users.name 
         FROM users 
         JOIN departments ON users.department_id = departments.id 
         WHERE departments.name = 'Engineering'"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2); // Alice and Bob
}