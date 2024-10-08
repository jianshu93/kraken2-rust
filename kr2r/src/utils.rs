use std::collections::{BTreeMap as Map, HashMap};
use std::fs::{self, create_dir_all, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Result};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// 读取 seqid2taxid.map 文件。为了裁剪 ncbi 的 taxonomy 树
pub fn read_id_to_taxon_map<P: AsRef<Path>>(filename: P) -> Result<HashMap<String, u64>> {
    let file = open_file(filename)?;
    let reader = BufReader::new(file);
    let mut id_map = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        let seq_id = parts[0].to_string();
        if let Ok(taxid) = parts[1].parse::<u64>() {
            id_map.insert(seq_id, taxid);
        }
    }

    Ok(id_map)
}

/// Expands a spaced seed mask based on the given bit expansion factor.
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// # use kr2r::utils::expand_spaced_seed_mask; // Replace with the appropriate crate name
/// // Expanding 0b1010 (binary for 10) with a factor of 2
/// assert_eq!(expand_spaced_seed_mask(0b1010, 2), 204);
///
/// // Expanding 0b0101 (binary for 5) with a factor of 1
/// assert_eq!(expand_spaced_seed_mask(0b0101, 1), 5);
/// ```
///
/// When the bit expansion factor is zero or greater than 64:
///
/// ```
/// # use kr2r::utils::expand_spaced_seed_mask;
/// // No expansion, factor is 0
/// assert_eq!(expand_spaced_seed_mask(0b1010, 0), 0b1010);
///
/// // No expansion, factor is greater than 64
/// assert_eq!(expand_spaced_seed_mask(0b1010, 65), 0b1010);
/// ```
pub fn expand_spaced_seed_mask(spaced_seed_mask: u64, bit_expansion_factor: u64) -> u64 {
    // 检查 bit_expansion_factor 是否在有效范围内
    if bit_expansion_factor == 0 || bit_expansion_factor > 64 {
        return spaced_seed_mask;
    }

    let mut new_mask = 0;
    let bits = (1 << bit_expansion_factor) - 1;

    for i in (0..64 / bit_expansion_factor).rev() {
        new_mask <<= bit_expansion_factor;
        if (spaced_seed_mask >> i) & 1 == 1 {
            new_mask |= bits;
        }
    }

    new_mask
}

pub fn find_files<P: AsRef<Path>>(path: P, prefix: &str, suffix: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with(prefix) && name.ends_with(suffix))
                .unwrap_or(false)
        })
        .map(|e| e.path().to_path_buf())
        .collect();
    files.sort_unstable();
    files
}

pub fn format_bytes(size: f64) -> String {
    let suffixes = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut size = size;
    let mut current_suffix = &suffixes[0];

    for suffix in &suffixes[1..] {
        if size >= 1024.0 {
            current_suffix = suffix;
            size /= 1024.0;
        } else {
            break;
        }
    }

    format!("{:.2}{}", size, current_suffix)
}

#[cfg(unix)]
extern crate libc;

#[cfg(unix)]
use libc::{getrlimit, rlimit, setrlimit, RLIMIT_NOFILE};

#[cfg(unix)]
pub fn get_file_limit() -> usize {
    let mut limits = rlimit {
        rlim_cur: 0, // 当前（软）限制
        rlim_max: 0, // 最大（硬）限制
    };

    // 使用unsafe块调用getrlimit，因为这是一个外部C函数
    let result = unsafe { getrlimit(RLIMIT_NOFILE, &mut limits) };

    if result == 0 {
        // 如果成功，返回当前软限制转换为usize
        limits.rlim_cur as usize
    } else {
        // 如果失败，输出错误并可能返回一个默认值或panic
        eprintln!("Failed to get file limit");
        0
    }
}

#[cfg(unix)]
pub fn set_fd_limit(new_limit: u64) -> io::Result<()> {
    let rlim = rlimit {
        rlim_cur: new_limit,
        rlim_max: new_limit,
    };

    let ret = unsafe { setrlimit(RLIMIT_NOFILE, &rlim) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
pub fn get_file_limit() -> usize {
    8192
}

#[cfg(windows)]
pub fn set_fd_limit(new_limit: u64) -> io::Result<()> {
    Ok(())
}

pub fn create_partition_files(partition: usize, base_path: &PathBuf, prefix: &str) -> Vec<PathBuf> {
    create_dir_all(&base_path).expect(&format!("create dir error {:?}", base_path));
    let file_path = base_path.clone();
    (1..=partition)
        .into_iter()
        .map(|item| file_path.join(format!("{}_{}.k2", prefix, item)))
        .collect()
}

pub fn create_partition_writers(partition_files: &Vec<PathBuf>) -> Vec<BufWriter<File>> {
    partition_files
        .into_iter()
        .map(|item| {
            // 尝试创建文件，如果失败则直接返回错误
            let file = OpenOptions::new()
                .write(true)
                .append(true) // 确保以追加模式打开文件
                .create(true) // 如果文件不存在，则创建
                .open(item)
                .unwrap();
            BufWriter::new(file)
        })
        .collect()
}

pub fn create_sample_file<P: AsRef<Path>>(filename: P) -> BufWriter<File> {
    let file = OpenOptions::new()
        .write(true)
        .append(true) // 确保以追加模式打开文件
        .create(true) // 如果文件不存在，则创建
        .open(filename)
        .unwrap();
    BufWriter::new(file)
}

use regex::Regex;

pub fn find_and_trans_bin_files(
    directory: &Path,
    prefix: &str,
    suffix: &str,
    check: bool,
) -> io::Result<Map<usize, Vec<PathBuf>>> {
    // 改为聚合相同数字的文件路径
    // 构建正则表达式以匹配文件名中的第一个数字
    let pattern = format!(r"{}_(\d+)_\d+{}", prefix, suffix);
    let re = Regex::new(&pattern).expect("Invalid regex pattern");

    // 读取指定目录下的所有条目
    let mut map_entries = Map::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();

        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|name| name.to_str()) {
                // 使用正则表达式匹配文件名，并提取第一个数字部分
                if let Some(cap) = re.captures(file_name) {
                    if let Some(m) = cap.get(1) {
                        if let Ok(num) = m.as_str().parse::<usize>() {
                            map_entries.entry(num).or_insert_with(Vec::new).push(path);
                        }
                    }
                }
            }
        }
    }

    if check {
        // 检查数字是否从1开始连续
        let mut keys: Vec<_> = map_entries.keys().cloned().collect();
        keys.sort_unstable();
        for (i, &key) in keys.iter().enumerate() {
            if i + 1 != key {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "File numbers are not continuous starting from 1.",
                ));
            }
        }
    }

    // 返回聚合后的文件路径
    Ok(map_entries)
}

pub fn find_and_trans_files(
    directory: &Path,
    prefix: &str,
    suffix: &str,
    check: bool,
) -> io::Result<Map<usize, PathBuf>> {
    // 构建正则表达式以匹配文件名中的数字
    let pattern = format!(r"{}_(\d+){}", prefix, suffix);
    let re = Regex::new(&pattern).unwrap();

    // 读取指定目录下的所有条目
    let entries = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .map_or(false, |s| s.starts_with(prefix) && s.ends_with(suffix))
        })
        .collect::<Vec<PathBuf>>();

    // 使用正则表达式提取数字，并将它们存入BTreeMap
    let mut map_entries = Map::new();
    for path in entries {
        if let Some(fname) = path.file_name().and_then(|name| name.to_str()) {
            if let Some(cap) = re.captures(fname) {
                if let Some(m) = cap.get(1) {
                    if let Ok(num) = m.as_str().parse::<usize>() {
                        map_entries.insert(num, path);
                    }
                }
            }
        }
    }

    if check {
        // 检查数字是否从0开始连续
        let mut keys: Vec<_> = map_entries.keys().cloned().collect();
        keys.sort_unstable();
        for (i, key) in keys.iter().enumerate() {
            if i + 1 != *key {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "File numbers are not continuous starting from 1.",
                ));
            }
        }
    }

    // 返回排序后的文件路径
    Ok(map_entries)
}

// 函数定义
pub fn find_and_sort_files(
    directory: &Path,
    prefix: &str,
    suffix: &str,
    check: bool,
) -> io::Result<Vec<PathBuf>> {
    // 构建正则表达式以匹配文件名中的数字
    let pattern = format!(r"{}_(\d+){}", prefix, suffix);
    let re = Regex::new(&pattern).unwrap();

    // 读取指定目录下的所有条目
    let entries = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .map_or(false, |s| s.starts_with(prefix) && s.ends_with(suffix))
        })
        .collect::<Vec<PathBuf>>();

    // 使用正则表达式提取数字并排序
    let mut sorted_entries = entries
        .into_iter()
        .filter_map(|path| {
            re.captures(path.file_name()?.to_str()?)
                .and_then(|caps| caps.get(1).map(|m| m.as_str().parse::<usize>().ok()))
                .flatten()
                .map(|num| (path, num))
        })
        .collect::<Vec<(PathBuf, usize)>>();

    sorted_entries.sort_by_key(|k| k.1);

    if check {
        // 检查数字是否从0开始连续
        for (i, (_, num)) in sorted_entries.iter().enumerate() {
            let a_idx = i + 1;
            if a_idx != *num {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "File numbers are not continuous starting from 1.",
                ));
            }
        }
    }

    // 返回排序后的文件路径
    Ok(sorted_entries
        .iter()
        .map(|(path, _)| path.clone())
        .collect())
}

pub fn open_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
    File::open(&path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            io::Error::new(e.kind(), format!("File not found: {:?}", path.as_ref()))
        } else {
            e
        }
    })
}

/// 获取最新的文件序号
pub fn get_lastest_file_index(file_path: &PathBuf) -> Result<usize> {
    let file_content = fs::read_to_string(&file_path)?;
    // 如果文件内容为空，则默认最大值为0
    let index = if file_content.is_empty() {
        0
    } else {
        file_content
            .lines() // 将内容按行分割
            .filter_map(|line| line.split('\t').next()) // 获取每行的第一列
            .filter_map(|num_str| num_str.parse::<usize>().ok()) // 尝试将第一列的字符串转换为整型
            .max() // 找到最大值
            .unwrap_or(1)
    };
    Ok(index)
}
