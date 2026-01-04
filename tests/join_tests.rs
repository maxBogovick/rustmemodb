use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

fn setup_db() -> InMemoryDB {
    let mut db = InMemoryDB::new();

    // Create 'users' table
    db.execute("CREATE TABLE users (id INTEGER, name TEXT, department_id INTEGER)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 1)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 1)").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 2)").unwrap();
    db.execute("INSERT INTO users VALUES (4, 'David', NULL)").unwrap(); // No department

    // Create 'departments' table
    db.execute("CREATE TABLE departments (id INTEGER, name TEXT)").unwrap();
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')").unwrap();
    db.execute("INSERT INTO departments VALUES (2, 'Sales')").unwrap();
    db.execute("INSERT INTO departments VALUES (3, 'Marketing')").unwrap(); // No users

    db
}

#[test]
fn test_inner_join() {
    let mut db = setup_db();

    // SELECT users.name, departments.name 
    // FROM users 
    // INNER JOIN departments ON users.department_id = departments.id
    let result = db.execute(
        "SELECT users.name, departments.name 
         FROM users 
         INNER JOIN departments ON users.department_id = departments.id
         ORDER BY users.name"
    ).unwrap();

    assert_eq!(result.row_count(), 3);
    
    let rows = result.rows();
    assert_eq!(rows[0][0], Value::Text("Alice".into()));
    assert_eq!(rows[0][1], Value::Text("Engineering".into()));
    
    assert_eq!(rows[1][0], Value::Text("Bob".into()));
    assert_eq!(rows[1][1], Value::Text("Engineering".into()));
    
    assert_eq!(rows[2][0], Value::Text("Charlie".into()));
    assert_eq!(rows[2][1], Value::Text("Sales".into()));
}

#[test]
fn test_left_join() {
    let mut db = setup_db();

    // SELECT users.name, departments.name 
    // FROM users 
    // LEFT JOIN departments ON users.department_id = departments.id
    let result = db.execute(
        "SELECT u.name, d.name
         FROM users u
         LEFT JOIN departments d ON u.department_id = d.id
         ORDER BY u.name"
    ).unwrap();

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

#[test]
fn test_right_join() {
    let mut db = setup_db();

    // SELECT users.name, departments.name 
    // FROM users 
    // RIGHT JOIN departments ON users.department_id = departments.id
    let result = db.execute(
        "SELECT users.name, departments.name 
         FROM users 
         RIGHT JOIN departments ON users.department_id = departments.id
         ORDER BY departments.name"
    ).unwrap();

    // Should include Marketing (no users) and exclude David (no department)
    // Rows:
    // Alice - Engineering
    // Bob - Engineering
    // Charlie - Sales
    // NULL - Marketing
    
    assert_eq!(result.row_count(), 4);
    
    // Find Marketing row
    let marketing_row = result.rows().iter().find(|r| r[1] == Value::Text("Marketing".into()));
    assert!(marketing_row.is_some());
    assert_eq!(marketing_row.unwrap()[0], Value::Null);
}

#[test]
fn test_cross_join() {
    let mut db = setup_db();

    // SELECT * FROM users CROSS JOIN departments
    // 4 users * 3 departments = 12 rows
    let result = db.execute("SELECT * FROM users CROSS JOIN departments").unwrap();
    assert_eq!(result.row_count(), 12);
}

#[test]
fn test_self_join() {
    let mut db = InMemoryDB::new();
    
    // Employees with managers
    db.execute("CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER)").unwrap();
    db.execute("INSERT INTO employees VALUES (1, 'Boss', NULL)").unwrap();
    db.execute("INSERT INTO employees VALUES (2, 'Manager', 1)").unwrap();
    db.execute("INSERT INTO employees VALUES (3, 'Worker', 2)").unwrap();

    // SELECT e.name, m.name 
    // FROM employees e 
    // JOIN employees m ON e.manager_id = m.id
    let result = db.execute(
        "SELECT e.name, m.name 
         FROM employees AS e 
         JOIN employees AS m ON e.manager_id = m.id
         ORDER BY e.name"
    ).unwrap();

    assert_eq!(result.row_count(), 2);
    
    let rows = result.rows();
    // Manager -> Boss
    assert_eq!(rows[0][0], Value::Text("Manager".into()));
    assert_eq!(rows[0][1], Value::Text("Boss".into()));
    
    // Worker -> Manager
    assert_eq!(rows[1][0], Value::Text("Worker".into()));
    assert_eq!(rows[1][1], Value::Text("Manager".into()));
}

#[test]
fn test_complex_join_with_where() {
    let mut db = setup_db();

    // Join with WHERE clause
    let result = db.execute(
        "SELECT users.name 
         FROM users 
         JOIN departments ON users.department_id = departments.id 
         WHERE departments.name = 'Engineering'"
    ).unwrap();

    assert_eq!(result.row_count(), 2); // Alice and Bob
}
