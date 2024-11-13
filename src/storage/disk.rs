use std::collections::{btree_map, BTreeMap};
use std::fs::{rename, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::PathBuf;
use fs4::FileExt;
use crate::storage::engine::{Engine, EngineIter};
use crate::error::Result;

// 先定义一下内存的数据结构
pub type KeyDir = BTreeMap<Vec<u8>, (u64,u32)>;  // key | (offset, value-len)

// 再定义一下磁盘数据的前缀
const LOG_HEADER_SIZE: u32 = 8; // size(key_len) + size(value_len) = 8

// 磁盘存储引擎的定义
pub struct DiskEngine{
    key_dir: KeyDir,    // 内存索引
    log: Log,           // 磁盘日志
}

struct Log{
    file: File,  // 日志存储文件
    file_path: PathBuf,  // 日志存储路径
}

impl Log{
    // 实现读日志和写日志的方法
    fn write_log(&mut self, key: &Vec<u8>, value:Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // 传引用是为了避免数据拷贝，这个函数直接返回 (offset, size) 即可

        // 1. 追加写入，首先要找到文件的末尾，即从End开始的第0个字节
        let start = self.file.seek(SeekFrom::End(0))?;  // 从start处开始写文件

        // 2. 使用BufferWriter进行写操作
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);  // value可能为空，需要操作一下
        let total_len = LOG_HEADER_SIZE + key_len + value_len;
        let mut writer =                                    // 得到了一个写缓冲器
            BufWriter::with_capacity(total_len as usize, &self.file);  // (缓冲区大小，文件)
        writer.write_all(&key_len.to_be_bytes())?;                    // write_all 保证必须将内容全部写入，否则会报错
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;  // value为None则value_size = -1
        writer.write_all(&key)?;
        if let Some(v) = value{
            writer.write_all(&v)?;
        }
        writer.flush()?;  // 将缓冲区的文件刷新为持久化
        Ok((start, total_len))
    }

    fn read_value(&mut self, offset: u64, value_len: u32) -> Result<Vec<u8>>{
        // 读取value的数据
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buffer= vec![0; value_len as usize];   // 大小为 value_len，其中每个元素初始化为 0
        self.file.read_exact(&mut buffer)?;     // 和write_all() 一样，read_exact()保证必须将内容全部读完，否则会报错
        Ok(buffer)  // buffer是大小为value长度的01字符流
    }

    // 实现启动方法
    fn new(file_path: PathBuf) -> Result<Self>{
        // 如果传入的路径不存在，则需要自动创建
        if let Some(parent) = file_path.parent(){  // abc/sql.log，如果目录abc不存在则需要创建
            if !parent.exists(){
                std::fs::create_dir_all(parent)?;
            }
        }

        // log文件存在或被创建成功，则打开文件
        let file = OpenOptions::new().write(true).read(true).create(true).open(&file_path)?;  // 可写可读可创建

        // 加锁，本文件不能并发地被其他数据库客户端使用
        file.try_lock_exclusive()?;

        Ok(Self{ file,file_path })
    }

    // 构建内存索引
    fn build_key_dir(&mut self) -> Result<KeyDir> {
        let mut key_dir = KeyDir::new();
        let mut reader = BufReader::new(&self.file);

        let mut offset = 0;  // 从文件开始读
        loop{
            if offset >= self.file.metadata()?.len(){
                break;   // 读完跳出循环
            }

            let (key, val_len) = Self::read_log(&mut reader, offset)?;
            let key_len = key.len() as u32;
            if val_len == -1{
                key_dir.remove(&key);
                offset += LOG_HEADER_SIZE as u64 + key_len as u64;
            }else {
                key_dir.insert(key,(
                    offset + LOG_HEADER_SIZE as u64 + key_len as u64, val_len as u32
                    ));
                offset += LOG_HEADER_SIZE as u64 + key_len as u64 + val_len as u64;
            }
        }
        Ok(key_dir)
    }

    // 构建内存索引辅助方法
    fn read_log(reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut buffer = [0;4];  // 大小为4的定长临时数组，用于存放读取到的key_len和value_len
        reader.read_exact(&mut buffer)?;
        let key_len = u32::from_be_bytes(buffer);

        reader.read_exact(&mut buffer)?;
        let value_len = i32::from_be_bytes(buffer);   // value_len 可能是 -1，所以是i32

        let mut key_buffer = vec![0; key_len as usize];   // 大小为 key_len 的变长临时数组，用于存放读到的 key
        reader.read_exact(&mut key_buffer)?;

        Ok((key_buffer, value_len))  // 返回key的字符码以及value的长度，这里不返回value是因为我们有单独的read_value函数
    }
}

// 实现一下通用的engine接口：
impl Engine for DiskEngine{
    type EngineIter<'a>= DiskEngineIter<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 1. 先写日志
        let (offset, size) = self.log.write_log(&key, Some(&value))?;
        // 2. 再更新内存索引
        let value_len = value.len() as u32;
        self.key_dir.insert(key, (
            offset + size as u64 - value_len as u64, value_len
            ));
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.key_dir.get(&key) {
            Some((offset, size)) => {
                let value = self.log.read_value(*offset, *size)?;
                Ok(Some(value))
            },
            None => Ok(None)
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_log(&key, None)?;  // 直接删除value即可
        self.key_dir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIter<'_> {
        DiskEngineIter{
            index: self.key_dir.range(range),
            log: &mut self.log
        }
    }
}


// 磁盘存储引擎的迭代器
pub struct DiskEngineIter<'a>{
    index:btree_map::Range<'a, Vec<u8>, (u64, u32)>,  // 范围迭代器, key | (offset, value-len)
    log: &'a mut Log,   // 需要从文件读取数据
}

impl<'a> DiskEngineIter<'a>{
    // self.index.next() 返回 Option<(&Vec<u8>, &(u64, u32))>
    fn iter_read_from_log(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (offset, value_len)) = item;
        let value = self.log.read_value(*offset, *value_len)?;
        Ok((key.clone(), value))
    }
}

impl<'a> EngineIter for DiskEngineIter<'a> {}

impl<'a> Iterator for DiskEngineIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.index.next().map(|item| self.iter_read_from_log(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.index.next_back().map(|item| self.iter_read_from_log(item))
    }
}

impl DiskEngine {
    // 启动流程
    pub fn new(file_path: PathBuf) -> Result<Self>{  // 传入日志文件路径
        // 1. 启动磁盘日志
        let mut log = Log::new(file_path)?;
        // 2. 从log中拿到数据，构建内存索引
        let  key_dir = log.build_key_dir()?;
        Ok(DiskEngine{ key_dir,log })
    }

    // 启动时清理
    pub fn new_compact(file_path: PathBuf) -> Result<Self>{
        // 启动存储引擎
        let mut engine = DiskEngine::new(file_path)?;  // 启动好的存储引擎已经包含了完整的log和key_dir，所以我们只要重写即可
        engine.compact()?;
        Ok(engine)
    }

    // 重写重复文件
    pub fn compact(&mut self) -> Result<()> {
        // 1. 在log相同目录打开一个新的临时文件
        let mut compact_path = self.log.file_path.clone();
        compact_path.set_extension("compact");   // 后缀名
        let mut compact_log = Log::new(compact_path)?;

        // 2. 在临时文件中重写
        let mut compact_key_dir = KeyDir::new();
        for(key, (offset, value_len)) in self.key_dir.iter() {
            let value = self.log.read_value(*offset, *value_len)?;
            let (compact_offset, compact_size) = compact_log.write_log(&key, Some(&value))?;
            compact_key_dir.insert(key.clone(), (
                compact_offset + compact_size as u64 - *value_len as u64, *value_len as u32
                ));
        }

        // 3. 将临时文件变为正式文件，删除原正式文件
        rename(&compact_log.file_path, &self.log.file_path)?;  // compact_log.file_path 变成 self.log.file_path
        compact_log.file_path = self.log.file_path.clone();
        self.key_dir = compact_key_dir;
        self.log = compact_log;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        storage::{disk::DiskEngine, engine::Engine},
    };
    use std::path::PathBuf;

    #[test]
    fn test_disk_engine_start() -> Result<()> {
        let eng = DiskEngine::new(PathBuf::from("./tmp/sqldb-log"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact_1() -> Result<()> {
        let eng = DiskEngine::new_compact(PathBuf::from("./tmp/sqldb-log"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact_2() -> Result<()> {
        let mut eng = DiskEngine::new(PathBuf::from("./tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;

        // 重写
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;

        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);  // 结束eng的生命周期，释放排他锁

        let mut eng2 = DiskEngine::new_compact(PathBuf::from("./tmp/sqldb/sqldb-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);

        std::fs::remove_dir_all("./tmp/sqldb")?;

        Ok(())
    }
}