use std::path::PathBuf;

pub fn setup_sqlite_db() -> PathBuf {
    let tmpdir = std::env::temp_dir();
    let db_path = tmpdir.join(uuid::Uuid::new_v4().to_string());
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(include_str!("../../init_catalog.sql"))
        .unwrap();
    db_path
}
