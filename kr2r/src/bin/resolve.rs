use clap::Parser;
use kraken2_rs::classify::process_hitgroup;
use kraken2_rs::compact_hash::{HashConfig, Row};
use kraken2_rs::readcounts::{TaxonCounters, TaxonCountersDash};
use kraken2_rs::report::report_kraken_style;
use kraken2_rs::taxonomy::Taxonomy;
use kraken2_rs::utils::{find_and_trans_bin_files, find_and_trans_files, open_file};
use kraken2_rs::HitGroup;
// use rayon::prelude::*;
use seqkmer::{buffer_map_parallel, trim_pair_info, OptionPair};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Result, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub fn read_id_to_seq_map<P: AsRef<Path>>(
    filename: P,
) -> Result<HashMap<u32, (String, String, usize, Option<usize>)>> {
    let file = open_file(filename)?;
    let reader = BufReader::new(file);
    let mut id_map = HashMap::new();

    reader.lines().for_each(|line| {
        let line = line.expect("Could not read line");
        let parts: Vec<&str> = line.trim().splitn(4, '\t').collect();
        if parts.len() >= 4 {
            // 解析序号为u32类型的键
            if let Ok(id) = parts[0].parse::<u32>() {
                // 第二列是序列标识符，直接作为字符串
                let seq_id = parts[1].to_string();
                let seq_size = parts[2].to_string();
                let count_parts: Vec<&str> = parts[3].split('|').collect();
                let kmer_count1 = count_parts[0].parse::<usize>().unwrap();
                let kmer_count2 = if count_parts.len() > 1 {
                    count_parts[1].parse::<usize>().map_or(None, |i| Some(i))
                } else {
                    None
                };
                id_map.insert(id, (seq_id, seq_size, kmer_count1, kmer_count2));
            }
        }
    });

    Ok(id_map)
}

#[derive(Parser, Debug, Clone)]
#[clap(
    version,
    about = "resolve taxonomy tree",
    long_about = "resolve taxonomy tree"
)]
pub struct Args {
    /// database hash chunk directory and other files
    #[arg(long = "db", required = true)]
    pub database: PathBuf,

    /// chunk directory
    #[clap(long, value_parser, required = true)]
    pub chunk_dir: PathBuf,

    /// File path for outputting normal Kraken output.
    #[clap(long = "output-dir", value_parser)]
    pub output_dir: Option<PathBuf>,

    /// The number of threads to use.
    #[clap(short = 'p', long = "num-threads", value_parser, default_value_t = num_cpus::get())]
    pub num_threads: usize,

    // /// output file contains all unclassified sequence
    // #[clap(long, value_parser, default_value_t = false)]
    // pub full_output: bool,
    /// Confidence score threshold, default is 0.0.
    #[clap(
        short = 'T',
        long = "confidence-threshold",
        value_parser,
        default_value_t = 0.0
    )]
    pub confidence_threshold: f64,

    /// In comb. w/ -R, provide minimizer information in report
    #[clap(short = 'K', long, value_parser, default_value_t = false)]
    pub report_kmer_data: bool,

    /// In comb. w/ -R, report taxa w/ 0 count
    #[clap(short = 'z', long, value_parser, default_value_t = false)]
    pub report_zero_counts: bool,

    /// The minimum number of hit groups needed for a call.
    #[clap(
        short = 'g',
        long = "minimum-hit-groups",
        value_parser,
        default_value_t = 2
    )]
    pub minimum_hit_groups: usize,
}

fn read_rows_from_file<P: AsRef<Path>>(file_path: P) -> io::Result<HashMap<u32, Vec<Row>>> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = [0u8; std::mem::size_of::<Row>()]; // 确保buffer的大小与Row结构体的大小一致
    let mut map: HashMap<u32, Vec<Row>> = HashMap::new();

    while reader.read_exact(&mut buffer).is_ok() {
        let row: Row = unsafe { std::mem::transmute(buffer) }; // 将读取的字节直接转换为Row结构体
        map.entry(row.seq_id).or_default().push(row); // 插入到HashMap中
    }

    Ok(map)
}

fn process_batch<P: AsRef<Path>>(
    sample_files: &Vec<P>,
    args: &Args,
    taxonomy: &Taxonomy,
    id_map: &HashMap<u32, (String, String, usize, Option<usize>)>,
    writer: &mut Box<dyn Write + Send>,
    value_mask: usize,
) -> Result<(TaxonCountersDash, usize)> {
    let confidence_threshold = args.confidence_threshold;
    let minimum_hit_groups = args.minimum_hit_groups;

    let classify_counter = AtomicUsize::new(0);
    let cur_taxon_counts = TaxonCountersDash::new();

    for sample_file in sample_files {
        let hit_counts: HashMap<u32, Vec<Row>> = read_rows_from_file(sample_file)?;

        buffer_map_parallel(
            &hit_counts,
            args.num_threads,
            |(k, rows)| {
                if let Some(item) = id_map.get(&k) {
                    let mut rows = rows.to_owned();
                    rows.sort_unstable();

                    let dna_id = trim_pair_info(&item.0);
                    let range =
                        OptionPair::from(((0, item.2), item.3.map(|size| (item.2, size + item.2))));
                    let hits = HitGroup::new(rows, range);

                    let hit_data = process_hitgroup(
                        &hits,
                        taxonomy,
                        &classify_counter,
                        hits.required_score(confidence_threshold),
                        minimum_hit_groups,
                        value_mask,
                    );

                    hit_data.3.iter().for_each(|(key, value)| {
                        cur_taxon_counts
                            .entry(*key)
                            .or_default()
                            .merge(value)
                            .unwrap();
                    });

                    // 使用锁来同步写入
                    let output_line = format!(
                        "{}\t{}\t{}\t{}\t{}\n",
                        hit_data.0, dna_id, hit_data.1, item.1, hit_data.2
                    );
                    Some(output_line)
                } else {
                    eprintln!("can't find {} in sample_id map file", k);
                    None
                }
            },
            |result| {
                while let Some(output) = result.next() {
                    if let Some(res) = output.unwrap() {
                        writer
                            .write_all(res.as_bytes())
                            .expect("write output content error");
                    }
                }
            },
        )
        .expect("failed");
    }

    Ok((cur_taxon_counts, classify_counter.load(Ordering::SeqCst)))
}

pub fn run(args: Args) -> Result<()> {
    let k2d_dir = &args.database;
    let taxonomy_filename = k2d_dir.join("taxo.k2d");
    let taxo = Taxonomy::from_file(taxonomy_filename)?;

    let sample_files = find_and_trans_bin_files(&args.chunk_dir, "sample_file", ".bin", false)?;
    let sample_id_files = find_and_trans_files(&args.chunk_dir, "sample_id", ".map", false)?;

    // let partition = sample_files.len();
    let hash_config = HashConfig::from_hash_header(&args.database.join("hash_config.k2d"))?;
    let value_mask = hash_config.value_mask;

    let mut total_taxon_counts = TaxonCounters::new();
    let mut total_seqs = 0;
    let mut total_unclassified = 0;

    if let Some(output) = &args.output_dir {
        create_dir_all(output)?;
    }

    // 开始计时
    let start = Instant::now();
    println!("resolve start...");

    for (i, sam_files) in &sample_files {
        let sample_id_map = read_id_to_seq_map(&sample_id_files[i])?;

        let thread_sequences = sample_id_map.len();
        let mut writer: Box<dyn Write + Send> = match &args.output_dir {
            Some(ref file_path) => {
                let filename = file_path.join(format!("output_{}.txt", i));
                let file = File::create(filename)?;
                Box::new(BufWriter::new(file)) as Box<dyn Write + Send>
            }
            None => Box::new(BufWriter::new(io::stdout())) as Box<dyn Write + Send>,
        };
        let (thread_taxon_counts, thread_classified) = process_batch::<PathBuf>(
            sam_files,
            &args,
            &taxo,
            &sample_id_map,
            &mut writer,
            value_mask,
        )?;

        let mut sample_taxon_counts: HashMap<
            u64,
            kraken2_rs::readcounts::ReadCounts<
                hyperloglogplus::HyperLogLogPlus<u64, kraken2_rs::KBuildHasher>,
            >,
        > = HashMap::new();
        thread_taxon_counts.iter().for_each(|entry| {
            total_taxon_counts
                .entry(*entry.key())
                .or_default()
                .merge(&entry.value())
                .unwrap();
            sample_taxon_counts
                .entry(*entry.key())
                .or_default()
                .merge(&entry.value())
                .unwrap();
        });
        if let Some(output) = &args.output_dir {
            let filename = output.join(format!("output_{}.kreport2", i));
            report_kraken_style(
                filename,
                args.report_zero_counts,
                args.report_kmer_data,
                &taxo,
                &sample_taxon_counts,
                thread_sequences as u64,
                (thread_sequences - thread_classified) as u64,
            )?;
        }

        total_seqs += thread_sequences;
        total_unclassified += thread_sequences - thread_classified;
    }

    if let Some(output) = &args.output_dir {
        if !sample_files.is_empty() {
            let min = &sample_files.keys().min().cloned().unwrap();
            let max = &sample_files.keys().max().cloned().unwrap();

            if max > min {
                let filename = output.join(format!("output_{}-{}.kreport2", min, max));
                report_kraken_style(
                    filename,
                    args.report_zero_counts,
                    args.report_kmer_data,
                    &taxo,
                    &total_taxon_counts,
                    total_seqs as u64,
                    total_unclassified as u64,
                )?;
            }

            let source_sample_file = args.chunk_dir.join("sample_file.map");
            let to_sample_file = output.join("sample_file.txt");
            std::fs::copy(source_sample_file, to_sample_file)?;
        };
    }

    // 计算持续时间
    let duration = start.elapsed();
    // 打印运行时间
    println!("resolve took: {:?}", duration);

    for (_, sam_files) in &sample_files {
        for sample_file in sam_files {
            let _ = std::fs::remove_file(sample_file);
        }
    }

    for (_, sample_file) in sample_id_files {
        let _ = std::fs::remove_file(sample_file);
    }
    // let source_sample_file = args.chunk_dir.join("sample_file.map");
    // let _ = std::fs::remove_file(source_sample_file);
    Ok(())
}

#[allow(dead_code)]
fn main() {
    let args = Args::parse();
    if let Err(e) = run(args) {
        eprintln!("Application error: {}", e);
    }
}
