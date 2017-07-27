use super::{WriteOptions, Error, DBVector, ReadOptions};
use super::TransactionDB;
use db::{DBIterator, IteratorMode};
use ffi;

use libc::{c_char, size_t};
use std::ptr::null_mut;

pub struct Transaction {
    pub inner: *mut ffi::rocksdb_transaction_t,
}


pub struct TransactionOptions {
    inner: *mut ffi::rocksdb_transaction_options_t,
}


impl Transaction {
    pub fn new(
        db: &TransactionDB,
        options: &WriteOptions,
        txn_options: &TransactionOptions,
    ) -> Self {
        unsafe {
            Transaction {
                inner: ffi::rocksdb_transaction_begin(
                    db.inner,
                    options.inner,
                    txn_options.inner,
                    null_mut(),
                ),
            }
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t
            ));
            Ok(())
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let opts = ReadOptions::default();
        self.get_opt(key, &opts)
    }

    pub fn get_opt(&self, key: &[u8], read_opts: &ReadOptions) -> Result<Option<DBVector>, Error> {
        if read_opts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                    .to_owned(),
            ));
        }

        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get(
                self.inner,
                read_opts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t
            ));
            Ok(())
        }
    }

    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(self.inner));
            Ok(())
        }
    }

    pub fn rollback(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_rollback(self.inner));
            Ok(())
        }
    }

    pub fn iterator(&self) -> DBIterator {
        let opts = ReadOptions::default();
        self.iterator_opt(&opts)
    }

    pub fn iterator_opt(&self, opts: &ReadOptions) -> DBIterator {
        DBIterator::new_txn(self, &opts, IteratorMode::Start)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

impl TransactionOptions {}

impl Default for TransactionOptions {
    fn default() -> Self {
        TransactionOptions { inner: unsafe { ffi::rocksdb_transaction_options_create() } }
    }
}
