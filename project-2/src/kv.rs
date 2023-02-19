use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsError, Result};
use std::ffi::OsStr;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The 'KvStore' stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a 'log' extension name.
/// A 'BTreeMap' in memory stores the keys and the value locations for fast query.
///
/// '''rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// '''

pub struct KvStore {
	// directory for the log and other data:
	path: PathBuf,
	// map generation number to the file reader:
	readers: HashMap<u64, BufReaderWithPos<File>>,
	// writer of the current log:
	writer: BufWriterWithPos<File>,
	current_gen: u64,
	index: BTreeMap<String, CommandPos>,
	// the number of bytes representing "stale" commands that could be
	// deleted during a compaction:
	uncompacted: u64,
}


impl KvStore {
	
	/// Opens a 'KvStore' with the given path.
	///
	/// This will create a new directory if the given one does not exist.
	///
	/// # Errors
	///
	/// It propagates I/O or deserialization errors during the log replay.
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let path = path.into();
		fs::create_dir_all(&path)?;

		let mut readers = HashMap::new();
		let mut index = BTreeMap::new();

		let gen_list = sorted_gen_list(&path)?;
		let mut uncompacted = 0;

		for &gen in &gen_list {
			let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
			uncompacted += load(gen, &mut reader, &mut index)?;
			readers.insert(gen, reader);
		}

		let current_gen = gen_list.last().unwrap_or(&0) + 1;
		let writer = new_log_file(&path, current_gen, &mut readers)?;

		Ok(KvStore {
			path,
			readers,
			writer,
			current_gen,
			index,
			uncompacted,
		})
	}



	/// Sets the value of a string key to a string.
	///
	/// If the key already exists, the previous value will be overwritten.
	///
	/// # Errors
	///
	/// It propagates I/O or serialization errors during writing the log:
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		let cmd = Command::set(key, value);
		let pos = self.writer.pos;
		serde_json::to_writer(&mut self.writer, &cmp)?;
		self.writer.flush()?;
		if let Command::Set { key, .. } = cmd {
			if let Some(old_cmd) = self
				.index
				.insert(key, (self.current_gen, pos..self.writer.pos).into())
			{
				self.uncompacted += old_cmd.len;
			}
		}

		if self.uncompacted > COMPACTION_THRESHOLD {
			self.compact()?;
		}

		Ok(())
	}



	/// Gets the string value of a given string key.
	///
	/// Returns 'None' if the given key does not exist.
	///
	/// # Errors
	///
	/// It returns 'KvsError::UnexpectedCommandType' if the given command type unexpected:
	pub fn get(&mut self, key: String) -> Result<Option<String>> {
		if let Some(cmd_pos) = self.index.get(&key) {
			let reader = self
				.readers
				.get_mut(&cmd_pos.gen)
				.expect("Cannot find log reader");

			reader.seek(SeekFrom::Start(cmd_pos.pos))?;
			let cmd_reader = reader.take(cmd_pos.len);
			if let Command::Set {value, .. } = serde_json::from_reader(cmd_reader)? {
				Ok(Some(value))
			} else {
				Err(KvsError::UnexpectedCommandType)
			}
		} else {
			Ok(None)
		}
	}




	/// Removes a given key.
	///
	/// # Errors
	///
	/// It returns 'KvsError::KeyNotFound' if the given key is not found.
	///
	/// It propagates I/O or serialization errors during writing to the log:
	pub fn remove(&mut self, key: String) -> Result<()> {
		if self.index.contains_key(&key) {
			let cmd = Command::remove(key);
			serde_json::to_writer(&mut self.writer, &cmd)?;
			self.writer.flush()?;
			if let Command::Remove { key } = cmd {
				let old_cmd = self.index.remove(&key).expect("key not found");
				self.uncompacted += old_cmd.len;
			}

			Ok(())
		} else {
			Err(KvsError::KeyNotFound)
		}
	}




	/// Clears stale entries in the log:
	pub fn compact(&mut self) -> Result<()> {
		// increase current gen by 2. current_gen + 1 is for the compaction file.
		let compaction_gen = self.current_gen + 1;
		self.current_gen += 2;
		self.writer = self.new_log_files(self.current_gen)?;

		let mut compaction_writer = self.new_log_file(compaction_gen)?;

		let mut new_pos = 0; // pos in the new log file
		for cmd_pos in &mut self.index.values_mut() {
			let reader = self.readers
				.get_mut(&cmd_pos.gen)
				.expect("Cannot find log reader");
			if reader.pos != cmd_pos.pos {
				reader.seek(SeekFrom::Start(cmd_pos.pos))?;
			}

			let mut entry_reader = reader.take(cmd_pos.len);
			let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
			*cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
			new_pos += len;
		}

		compaction_writer.flush()?;

		// Remove stale log files:
		let stale_gens: Vec<_> = self
			.readers
			.keys()
			.filter(|&&gen| gen < compaction_gen)
			.cloned()
			.collect();

		for stale_gen in stale_gens {
			self.readers.remove(&stale_gen);
			fs::remove_file(log_path(&self.path, stale_gen))?;
		}

		self.uncompacted = 0;

		Ok(())
	}


	/// Create a new log file with given generation number and add the reader to the reader's map:
	///
	/// Returns the writer to the log:
	fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
		new_log_file(&self.path, gen, &mut self.readers)
	}

	/// Create a new log file with given generation number and add the reader to the reader's map
	///
	/// Returns the writer to the log:
	fn new_log_file(path: &Path, gen: u64, readers: &mut HashMap<u64, BufReaderWithPos<File>>) -> Result<BufWriterWithPos<File>> {
		let path = log_path(&path, gen);
		let writer = BufWriterWithPos::new(
			OpenOptions::new()
				.create(true)
				.write(true)
				.append(true)
				.open(&path)?,
			)?;
		readers.insert(gen, BufreaderWithPos::new(File::open(&path)?)?);
		Ok(writer)
	}



	/// Returns sorted generation numbers in the given directory:
	fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
		let mut gen_list: Vec<u64> = fs::read_dir(&path)?
			.flat_map(|res| -> Result<_> { Ok(res?.path()) })
			.filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
			.flat_map(|path| {
				path.file_name()
					.and_then(OsStr::to_str)
					.map(|s| s.trim_end_matches(".log"))
					.map(str::parse::<u64>)
			})
			.flatten()
			.collect();

		gen_list.sort_unstable();
		Ok(gen_list)
	}
			

		
