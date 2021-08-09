use crate::errors::*;
use crate::kvsengine::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use tracing::{debug, error};

//To store approx 10 records -- Goal is to see if partitionning
// is working properly. In real world we could go up to 3 or 4 gygabytes easily
const MAX_SIZE_THRESHOLD: u64 = 28 * 10;

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

#[derive(Deserialize, Serialize, Debug, Clone)]
struct KvIndex {
    key: String,
    file_number: u64,
    record_offset: u64,
    record_length: u64,
}

impl KvIndex {
    pub fn new(key: String, file_number: u64, record_offset: u64, record_length: u64) -> KvIndex {
        KvIndex {
            key,
            file_number,
            record_offset,
            record_length,
        }
    }
}

/// Main structure that hold our key/value store
/// Everything is based around a hashmap of bufreader for fast access
/// And 2 writers : One for the active log file and an other for the active index file
pub struct KvStore {
    active_file_number: u64,
    base_directory: PathBuf,
    active_file_writer: BufWriter<File>,
    index_file_writer: BufWriter<File>,
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

fn init_readers(directory: &PathBuf) -> Result<(HashMap<u64, BufReader<File>>, u64)> {
    let mut my_readers: HashMap<u64, BufReader<File>> = HashMap::new();
    let bdd_files = search_bdd_files(directory);
    let mut max_gen: u64 = 0;
    match bdd_files {
        Ok(files) => {
            for file in files {
                if file > max_gen {
                    max_gen = file;
                }
                let mut file_path = directory.clone();
                file_path.push(format!("file_{}.bdd", file));
                //Create a buffered Reader and insert it in a Hashmap
                let bdd_reader = BufReader::new(File::open(&file_path)?);
                my_readers.insert(file, bdd_reader);
            }
            return Ok((my_readers, max_gen));
        }
        Err(x) => Err(x),
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.index_file_writer.flush();
        self.active_file_writer.flush();
    }
}

impl KvStore {
    fn new(directory: PathBuf) -> KvStore {
        let mut file: PathBuf = directory.clone();

        let mut idx_file: PathBuf = directory.clone();
        idx_file.push("kvindex.idx");
        let index_writer = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .append(true)
            .open(idx_file);
        if let Ok(index_writer) = index_writer {
            if let Ok((readers, max_file)) = init_readers(&directory) {
                file.push(format!("file_{}.bdd", max_file));
                if let Ok(curr_file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .write(true)
                    .open(&file)
                {
                    KvStore {
                        active_file_number: max_file as u64,
                        base_directory: directory.clone(),
                        active_file_writer: BufWriter::new(curr_file),
                        index_map: BTreeMap::new(),
                        index_file_writer: BufWriter::new(index_writer),
                        readers: readers,
                    }
                } else {
                    panic!("Could not open current file...");
                }
            } else {
                panic!("Readers were not initialized...");
            }
        } else {
            panic!("Index_writer could be created");
        }
    }

    /// Synchronize the index map to a file.
    pub fn sync_index(&mut self) -> Result<()> {
        let mut index_file_path: PathBuf = self.base_directory.clone();
        index_file_path.push("kvindex.idx");
        self.index_file_writer = BufWriter::new(
            OpenOptions::new()
                .truncate(true)
                .write(true)
                .create(true)
                .append(true)
                .open(&index_file_path)?,
        );

        for (_, index) in &self.index_map {
            let serialis = serde_json::to_string(&index)?;
            let size_of = serialis.len().to_ne_bytes();
            std::io::Write::by_ref(&mut self.index_file_writer).write(&size_of)?;
            std::io::Write::by_ref(&mut self.index_file_writer).write(serialis.as_bytes())?;
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
    pub fn open<P: Into<PathBuf>>(directory: P) -> Result<KvStore> {
        let mut pos: u64 = 0;
        let mut mypath: PathBuf = directory.into();
        let mut store: KvStore = KvStore::new(mypath.clone());
        mypath.push("kvindex.idx");
        match File::open(&mypath) {
            Ok(mut idx_file) => {
                let mut rl_bytes = [0u8; 8];
                idx_file.seek(SeekFrom::Start(pos))?;
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
                                    .read_to_end(&mut record_bytes)?;
                                match serde_json::from_slice::<KvIndex>(record_bytes.as_slice()) {
                                    Ok(index) => {
                                        store.index_map.insert(index.key.clone(), index);
                                    }
                                    Err(x) => {
                                        error!("Error during deserialize : {:?}", x);
                                    }
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
            Err(z) => {
                error!("Error when opening indexfile {:?}", z);
            }
        };

        return Ok(store);
    }

    /// Go through the index_map and for each index found : Copy datas from file to a new one.
    /// In the end remaining file should be filled with active records.
    pub fn compaction(&mut self) -> Result<()> {
        // Files from 1 to Active-1 are compacted
        // This should not be confused with the new file function that simply create a new log file
        // when the max size is reached.

        let mut writers: HashMap<u64, BufWriter<File>> = HashMap::new();
        let mut new_index_map: BTreeMap<String, KvIndex> = BTreeMap::new();

        for (cle, index) in &self.index_map {
            if index.file_number == self.active_file_number {
                //We do not work on the current file
                let new_index = KvIndex::new(
                    cle.clone(),
                    index.file_number,
                    index.record_offset,
                    index.record_length,
                );
                new_index_map.insert(cle.clone(), new_index);
                continue;
            }
            if let None = writers.get_mut(&(index.file_number as u64)) {
                let mut new_file = self.base_directory.clone();
                new_file.push(format!("file_{}.new", index.file_number));
                let new_writer = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(&new_file)?;
                writers.insert(index.file_number as u64, BufWriter::new(new_writer));
            }
            let writer = writers
                .get_mut(&(index.file_number as u64))
                .expect("File not found and not created...");
            let reader = self
                .readers
                .get_mut(&(index.file_number as u64))
                .expect("File not found");
            //

            reader.seek(SeekFrom::Start(index.record_offset))?;
            reader.take(index.record_length + 8);
            let cur_pos = writer.seek(SeekFrom::Current(0))?;
            let copied_length = io::copy(reader, writer)?;
            let new_index =
                KvIndex::new(cle.clone(), index.file_number, cur_pos, index.record_length);
            new_index_map.insert(cle.clone(), new_index);
            debug!(
                "COMPACTION : {} bytes copied from reader to writer",
                copied_length
            );
        }

        // At this stage, we have now 1..N files named file_XX.new in our working directory$
        // We shall now rename old .bdd files and rename our new .bdd files
        let new_files: Vec<u64> = fs::read_dir(&self.base_directory)?
            .flat_map(|x| -> Result<_> { Ok(x?.path()) })
            .filter(|file| file.is_file() && file.extension() == Some("new".as_ref()))
            .flat_map(|file| {
                file.file_stem()
                    .and_then(OsStr::to_str)
                    .map(|name| name.trim_start_matches("file_")) // Yield an Option(String)
                    .map(str::parse::<u64>) //Yield an Option(Option(u64) )
            }) //Yield an Option(String) -- One level of Option has been removed by the "flat"
            .flatten() //Extract the value
            .collect(); //Consume the iterator

        for path in new_files {
            //Our old files are now renamed
            fs::rename(format!("file_{}.bdd", path), format!("file_{}.old", path))?;
            fs::rename(format!("file_{}.new", path), format!("file_{}.bdd", path))?;
        }
        // Replacement of the old index_map
        // On large systems this may not be a viable option if the index_map takes gygabytes of
        // memory - It may be wiser to just update the map
        self.index_map = new_index_map;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Write the serialized key/value structure to the current file.
    /// We still need to write the partitionning mechanism
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let kvrecord: KvRecord = KvRecord::new(key.clone(), value);
        let serial_kvrecord = serde_json::to_string(&kvrecord)?;
        let size_of_record = serial_kvrecord.as_bytes().len();
        //       let mut log_file = OpenOptions::new()
        //          .create(true)
        //         .append(true)
        //         .open(&self.active_file_path)?;
        let pos = self.active_file_writer.seek(SeekFrom::End(0))?;
        let buf_sizeof = size_of_record.to_ne_bytes();
        self.active_file_writer.write(&buf_sizeof)?;
        self.active_file_writer.write(serial_kvrecord.as_bytes())?;
        let index: KvIndex = KvIndex::new(
            key.clone(),
            self.active_file_number,
            pos,
            size_of_record as u64,
        );
        self.index_map.insert(key.clone(), index.clone());

        // Insertion in the index_file is performed as soon as the record has been set
        // This ensure that no data is lost
        let serialis = serde_json::to_string(&index)?;
        let size_of = serialis.len().to_ne_bytes();
        self.index_file_writer.write(&size_of)?;
        self.index_file_writer.write(serialis.as_bytes())?;

        //We shoud check here if it is not time to create a new file
        if pos as u64 + size_of_record as u64 > MAX_SIZE_THRESHOLD {
            self.active_file_writer.flush()?;
            self.active_file_number += 1;
            let mut new_activefile = self.base_directory.clone();
            new_activefile.push(format!("file_{}.bdd", self.active_file_number));
            self.active_file_writer = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .write(true)
                    .open(&new_activefile)?,
            );
            new_activefile.pop();
            new_activefile.push(format!("file_{}.bdd", self.active_file_number - 1));
            self.readers.insert(
                (self.active_file_number - 1) as u64,
                BufReader::new(File::open(&new_activefile)?),
            );
        }
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
                        .expect("File not found!");

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
