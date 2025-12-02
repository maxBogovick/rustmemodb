use rustmemodb::Client;

#[test]
fn test_count_star() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_count (id INTEGER, name TEXT)").unwrap();
    client.execute("INSERT INTO test_count VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();

    let result = client.query("SELECT COUNT(*) FROM test_count").unwrap();
    assert_eq!(result.row_count(), 1);

    // First row, first column should be 3
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0].len(), 1);
    assert_eq!(rows[0][0].to_string(), "3");
}

#[test]
fn test_count_column() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_count_col (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO test_count_col VALUES (1, 10), (2, NULL), (3, 30)").unwrap();

    // COUNT(value) should be 2 (excludes NULL)
    let result = client.query("SELECT COUNT(value) FROM test_count_col").unwrap();
    assert_eq!(result.row_count(), 1);

    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "2");
}

#[test]
fn test_count_with_where() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_count_where (id INTEGER, age INTEGER)").unwrap();
    client.execute("INSERT INTO test_count_where VALUES (1, 25), (2, 30), (3, 35), (4, 40)").unwrap();

    let result = client.query("SELECT COUNT(*) FROM test_count_where WHERE age > 30").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "2");
}

#[test]
fn test_sum_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_sum (value INTEGER)").unwrap();
    client.execute("INSERT INTO test_sum VALUES (10), (20), (30), (40)").unwrap();

    let result = client.query("SELECT SUM(value) FROM test_sum").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "100");
}

#[test]
fn test_sum_with_nulls() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_sum_null (value INTEGER)").unwrap();
    client.execute("INSERT INTO test_sum_null VALUES (10), (NULL), (30)").unwrap();

    let result = client.query("SELECT SUM(value) FROM test_sum_null").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "40");
}

#[test]
fn test_avg_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_avg (value INTEGER)").unwrap();
    client.execute("INSERT INTO test_avg VALUES (10), (20), (30), (40)").unwrap();

    let result = client.query("SELECT AVG(value) FROM test_avg").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "25");
}

#[test]
fn test_min_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_min (value INTEGER)").unwrap();
    client.execute("INSERT INTO test_min VALUES (50), (20), (80), (10)").unwrap();

    let result = client.query("SELECT MIN(value) FROM test_min").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "10");
}

#[test]
fn test_max_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_max (value INTEGER)").unwrap();
    client.execute("INSERT INTO test_max VALUES (50), (20), (80), (10)").unwrap();

    let result = client.query("SELECT MAX(value) FROM test_max").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "80");
}

#[test]
fn test_multiple_aggregates() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_multi_agg (value INTEGER)").unwrap();
    client.execute("INSERT INTO test_multi_agg VALUES (10), (20), (30), (40)").unwrap();

    let result = client.query("SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM test_multi_agg").unwrap();
    let rows: Vec<_> = result.iter().collect();

    assert_eq!(result.row_count(), 1);
    assert_eq!(rows[0].len(), 5);
    assert_eq!(rows[0][0].to_string(), "4");  // COUNT
    assert_eq!(rows[0][1].to_string(), "100"); // SUM
    assert_eq!(rows[0][2].to_string(), "25");  // AVG
    assert_eq!(rows[0][3].to_string(), "10");  // MIN
    assert_eq!(rows[0][4].to_string(), "40");  // MAX
}

#[test]
fn test_aggregate_with_float() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_agg_float (value FLOAT)").unwrap();
    client.execute("INSERT INTO test_agg_float VALUES (1.5), (2.5), (3.0)").unwrap();

    let result = client.query("SELECT SUM(value) FROM test_agg_float").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "7");
}

#[test]
fn test_aggregate_empty_table() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_agg_empty (value INTEGER)").unwrap();

    let result = client.query("SELECT COUNT(*) FROM test_agg_empty").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "0");

    let result = client.query("SELECT AVG(value) FROM test_agg_empty").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "NULL");
}

#[test]
fn test_count_with_expression() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_count_expr (id INTEGER, status TEXT)").unwrap();
    client.execute("INSERT INTO test_count_expr VALUES (1, 'active'), (2, 'inactive'), (3, 'active')").unwrap();

    let result = client.query("SELECT COUNT(*) FROM test_count_expr WHERE status = 'active'").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "2");
}

#[test]
fn test_aggregate_with_text() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_agg_text (name TEXT)").unwrap();
    client.execute("INSERT INTO test_agg_text VALUES ('Alice'), ('Bob'), ('Charlie')").unwrap();

    // MIN and MAX should work with text
    let result = client.query("SELECT MIN(name), MAX(name) FROM test_agg_text").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "Alice");
    assert_eq!(rows[0][1].to_string(), "Charlie");
}
