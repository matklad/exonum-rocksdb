extern crate rocksdb;

use rocksdb::{TransactionDB, Options, WriteOptions, TransactionOptions};

fn main() {
    let path = "/tmp/rookkkss";
    
    {
        let db = TransactionDB::open_default(path).unwrap();
        let _ = db.put(b"key1", b"value1");
        let _ = db.put(b"key2", b"value2");
        let txn = db.transaction_begin(&WriteOptions::default(), &TransactionOptions::default());
        let iter = txn.iterator();

        for (key, value) in iter {
            println!("key: {} value: {}", 
                String::from_utf8(key.to_vec()).unwrap(), 
                String::from_utf8(value.to_vec()).unwrap());
        }
    }

    if let Err(e) =  TransactionDB::destroy(&Options::default(), path) {
        println!("Error destroying db: {}", e);
    }
}
