use crate::errors::*;
use crate::kvsengine::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::SeekFrom;
use std::path::PathBuf;
use tracing::{debug, info};
#[derive(Deserialize, Serialize)]
struct KvRecord {
    key: String,
    value: String,
}

impl KvRecord {
    pub fn new(key: String, value: String) -> KvRecord {
        KvRecord { key, value }
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct KvIndex {
    key: String,
    file_number: usize,
    record_offset: u64,
}

impl KvIndex {
    pub fn new(key: String, file_number: usize, record_offset: u64) -> KvIndex {
        KvIndex {
            key,
            file_number,
            record_offset,
        }
    }
}

pub struct KvStore {
    active_file_number: usize,
    base_directory: PathBuf,
    active_file_path: PathBuf,
    index_file_path: PathBuf,
    index_map: BTreeMap<String, KvIndex>,
    readers: HashMap<u64, BufReader<File>>,
}

fn search_bdd_files(directory: &PathBuf) -> Result<Vec<u64>> {
    let bdd_files = fs::read_dir(&directory)?
        .flat_map(|x| -> Result<_> { Ok(x?.path()) })
        .filter(|file| file.is_file() && file.extension() == Some("bdd".as_ref()))
        .flat_map(|file| {
            file.file_stem()
                .and_then(OsStr::to_str)
                .map(|name| name.trim_start_matches("file_")) // Yield an Option(String)
                .map(str::parse::<u64>) //Yield an Option(Option(u64) )
        }) //Yield an Option(String) -- One level of Option has been removed by the "flat"
        .flatten() //Extract the value
        .collect(); //Consume the iterator
    return Ok(bdd_files);
}

fn init_readers(directory: &PathBuf) -> Result<HashMap<u64, BufReader<File>>> {
    let mut my_readers: HashMap<u64, BufReader<File>> = HashMap::new();
    let bdd_files = search_bdd_files(directory);
    if let Ok(files) = bdd_files {
        for file in files {
            let mut file_path = directory.clone();
            file_path.push(format!("file_{}.bdd", file));
            //Create a buffered Reader and insert it in a Hashmap
            let bdd_reader = BufReader::new(File::open(&file_path)?);
            my_readers.insert(file, bdd_reader);
        }
    }
    return Ok(my_readers);
}

impl KvStore {
    fn new(directory: PathBuf) -> KvStore {
        let mut file: PathBuf = directory.clone();
        file.push("file_0.bdd");
        let mut idx_file: PathBuf = directory.clone();
        idx_file.push("kvindex.idx");
        if let Ok(readers) = init_readers(&directory) {
            KvStore {
                active_file_number: 0,
                base_directory: directory.clone(),
                active_file_path: file,
                index_map: BTreeMap::new(),
                index_file_path: idx_file,
                readers: readers,
            }
        } else {
            KvStore {
                active_file_number: 0,
                base_directory: directory.clone(),
                active_file_path: file,
                index_map: BTreeMap::new(),
                index_file_path: idx_file,
                readers: HashMap::new(),
            }
        }
    }

    pub fn sync_index(&mut self) -> Result<()> {
        debug!("Trying to create the index file");
        let mut idx_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.index_file_path)?;
        debug!("Now looping through index map");
        for (_, index) in &self.index_map {
            let serialis = serde_json::to_string(&index)?;
            let size_of = serialis.len().to_ne_bytes();
            std::io::Write::by_ref(&mut idx_file).write(&size_of)?;
            std::io::Write::by_ref(&mut idx_file).write(serialis.as_bytes())?;
        }
        Ok(())
    }

    /// Open a store directory - A store directory contains every files required to operate
    /// 0..N file_XX.bdd --> Containing datas as |Sizeofrecord(8bytes)|Record(N bytes)|...
    /// 0..1 kvindex.idx file -> Containing the index as described in the bitcask paper
    /// The function will count how many files there is in the directory and then load the index
    /// for each index it will quickly check that the file exists.
    /// And finaly we will check if each data file is indexed, if not it will be indexed.
    /// Even if the operation can be long at time it should be performed only one when running in
    /// server <-> client mode
    pub fn open(directory: PathBuf) -> KvStore {
        let mut pos: u64 = 0;
        let mut store: KvStore = KvStore::new(directory.clone());

        store.index_file_path = directory.clone();
        store.index_file_path.push("kvindex.idx");

        //        let mut record_bytes = vec![];
        if let Ok(mut idx_file) = File::open(&store.index_file_path) {
            let mut rl_bytes = [0u8; 8];
            idx_file.seek(SeekFrom::Start(pos));
            let mut bcontinue = true;
            while bcontinue {
                if let Ok(nb_bytes_read) = std::io::Read::by_ref(&mut idx_file)
                    .take(8)
                    .read(&mut rl_bytes)
                {
                    if nb_bytes_read == 8 {
                        let size_of_record = i64::from_ne_bytes(rl_bytes);
                        if size_of_record < 0 {
                            pos += size_of_record.abs() as u64;
                        } else {
                            let mut record_bytes = vec![];
                            std::io::Read::by_ref(&mut idx_file)
                                .take(size_of_record as u64)
                                .read_to_end(&mut record_bytes);
                            match serde_json::from_slice::<KvIndex>(record_bytes.as_slice()) {
                                Ok(index) => {
                                    store.index_map.insert(index.key.clone(), index);
                                }
                                Err(_) => {}
                            }
                        }
                    } else {
                        bcontinue = false;
                    }
                } else {
                    bcontinue = false;
                }
            }
        }
        return store;
    }
}

impl KvsEngine for KvStore {
    /// Write the serialized key/value structure to the current file.
    /// We still need to write the partitionning mechanism
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let kvrecord: KvRecord = KvRecord::new(key.clone(), value);
        let serial_kvrecord = serde_json::to_string(&kvrecord)?;
        let size_of_record = serial_kvrecord.as_bytes().len();
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.active_file_path)?;
        let pos = log_file.seek(SeekFrom::End(0))?;
        let buf_sizeof = size_of_record.to_ne_bytes();
        log_file.write(&buf_sizeof)?;
        log_file.write(serial_kvrecord.as_bytes())?;
        let index: KvIndex = KvIndex::new(key.clone(), self.active_file_number, pos);
        self.index_map.insert(key.clone(), index);
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if key == "".to_string() {
            debug!("get Function - No key was provided.");
            Ok(Some("Key not found".to_string()))
        } else {
            let index = self.index_map.get(&key);
            match index {
                Some(idx) => {
                    debug!(
                        "And index has been found. Record offset is {}",
                        idx.record_offset
                    );
                    let reader = self
                        .readers
                        .get_mut(&(idx.file_number as u64))
                        .expect("File not found");

                    let mut buf_size_of = [0u8; 8];
                    debug!("Seeking in the file");
                    reader.seek(SeekFrom::Start(idx.record_offset))?;
                    debug!("Reading bytes");
                    reader.take(8).read(&mut buf_size_of)?;

                    let record_size = i64::from_ne_bytes(buf_size_of);
                    if record_size > 0 {
                        debug!("Record size is {}", record_size);
                        let mut read_vector = vec![];
                        reader
                            .take(record_size as u64)
                            .read_to_end(&mut read_vector)?;
                        let record: KvRecord = serde_json::from_slice(read_vector.as_slice())?;
                        return Ok(Some(record.value));
                    } else {
                        debug!("Record size is < 0 ");
                        return Ok(Some("Key not found".to_string()));
                    }
                    Ok(Some("".to_string()))
                }
                None => {
                    debug!("No index record was found.");
                    debug!("Map is {:?}", self.index_map);
                    Ok(Some("Key not found".to_string()))
                }
            }
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if key == "".to_string() {
            return Ok(());
        } else {
            let index = self.index_map.get(&key);
            match index {
                Some(idx) => {
                    let mut log_file = self.base_directory.clone();
                    log_file.push(format!("file_{}", idx.file_number).as_str());
                    let mut log_file = OpenOptions::new().read(true).write(true).open(&log_file)?;
                    let mut buf_size_of = [0u8; 8];
                    std::io::Read::by_ref(&mut log_file)
                        .seek(SeekFrom::Start(idx.record_offset))?;
                    std::io::Read::by_ref(&mut log_file)
                        .take(8)
                        .read(&mut buf_size_of)?;
                    let mut record_size = i64::from_ne_bytes(buf_size_of);
                    if record_size > 0 {
                        record_size = record_size * -1;
                    }
                    buf_size_of = record_size.to_ne_bytes();
                    std::io::Write::by_ref(&mut log_file)
                        .seek(SeekFrom::Start(idx.record_offset))?;
                    std::io::Write::by_ref(&mut log_file).write(&buf_size_of)?;
                    return Ok(());
                }
                None => return Ok(()),
            }
        }
    }
}
