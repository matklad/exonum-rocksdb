use rocksdb::{TransactionDB, WriteOptions, TransactionOptions};
use tempdir::TempDir;


#[test]
fn test_transactiondb_creation_and_destroy() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let _ = TransactionDB::open_default(path).unwrap();
}

#[test]
fn test_transactiondb_commit() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    assert!(txn.get(b"key1").unwrap().is_some());
    assert!(txn.commit().is_ok());
    assert_eq!(db.get(b"key1").unwrap().unwrap().to_utf8(), Some("value1"));
}

#[test]
fn test_transactiondb_rollback() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    assert!(txn.get(b"key1").unwrap().is_some());
    assert!(txn.rollback().is_ok());
    assert!(db.get(b"key1").unwrap().is_none());
}
