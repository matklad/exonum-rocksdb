use ffi;
use std::ffi::CString;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::fs;

use libc::{self, c_char, c_int, c_uchar, c_void, size_t};
use super::{ColumnFamily, Options, Error, Transaction, ReadOptions, 
            WriteOptions, TransactionOptions, DBVector};

unsafe impl Send for TransactionDB {}
unsafe impl Sync for TransactionDB {}

pub struct TransactionDB {
    pub inner: *mut ffi::rocksdb_transactiondb_t,
    path: PathBuf,
}

pub struct TransactionDBOptions {
    inner: *mut ffi::rocksdb_transactiondb_options_t
}

pub struct Snapshot<'a> {
    db: &'a TransactionDB,
    inner: *const ffi::rocksdb_snapshot_t
}

impl TransactionDB {
    /// Open a transactional database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        let mut transaction_db_opts = TransactionDBOptions::default();
        opts.create_if_missing(true);
        Self::open(&opts, &transaction_db_opts, path)
    }

    /// Open the transactional database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, 
                                txn_db_opts: &TransactionDBOptions, 
                                path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new("Failed to convert path to CString \
                                       when opening DB."
                    .to_owned()))
            }
        };

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!("Failed to create RocksDB \
                                           directory: `{:?}`.",
                                          e)));
        }

        let db: *mut ffi::rocksdb_transactiondb_t = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open(opts.inner, 
                                                     txn_db_opts.inner,
                                                     cpath.as_ptr() as *const _))
        };

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(TransactionDB {
            inner: db,
            path: path.to_path_buf(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let opts = ReadOptions::default();
        self.get_opt(key, &opts)
    }

    pub fn get_opt(&self, key: &[u8], read_opts: &ReadOptions) -> Result<Option<DBVector>, Error> {
        if read_opts.inner.is_null() {
            return Err(Error::new("Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                .to_owned()));
        }

        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transactiondb_get(self.inner,
                                                              read_opts.inner,
                                                              key.as_ptr() as *const c_char,
                                                              key.len() as size_t,
                                                              &mut val_len)) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn transaction_begin(&self, w_opts: &WriteOptions, txn_opts: &TransactionOptions) -> Transaction {
        Transaction::new(self, w_opts, txn_opts)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self)
    }
}

impl Default for TransactionDBOptions {
    fn default() -> Self {
        unsafe {
            let transaction_db_options = ffi::rocksdb_transactiondb_options_create();
            if transaction_db_options.is_null() {
                panic!("Couldn't create Transaction RocksDB options");
            }
            Self {
                inner: transaction_db_options
            }
        }
    }
}

impl Drop for TransactionDB {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}

impl<'a> Snapshot<'a> {
    pub fn new(db: &TransactionDB) -> Snapshot {
        let snapshot = unsafe { ffi::rocksdb_transactiondb_create_snapshot(db.inner) };
        Snapshot {
            db: db,
            inner: snapshot,
        }
    }

    // pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
    //     let mut readopts = ReadOptions::default();
    //     readopts.set_snapshot(self);
    //     DBIterator::new(self.db, &readopts, mode)
    // }

    // pub fn raw_iterator(&self) -> DBRawIterator {
    //     let mut readopts = ReadOptions::default();
    //     readopts.set_snapshot(self);
    //     DBRawIterator::new(self.db, &readopts)
    // }

    // pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
    //     let mut readopts = ReadOptions::default();
    //     readopts.set_snapshot(self);
    //     self.db.get_opt(key, &readopts)
    // }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_release_snapshot(self.db.inner, self.inner);
        }
    }
}
