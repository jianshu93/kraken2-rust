// 使用时需要引用模块路径
use clap::Parser;
use kraken2_rs::compact_hash::HashConfig;
use kraken2_rs::db::process_k2file;
use kraken2_rs::taxonomy::Taxonomy;
use kraken2_rs::utils::find_and_trans_files;
use std::fs::remove_file;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about="build database", long_about = None)]
pub struct Args {
    /// database hash chunk directory and other files
    #[arg(long = "db", required = true)]
    pub database: PathBuf,
}

pub fn run(database: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let k2d_dir = database;
    let taxonomy_filename = k2d_dir.join("taxo.k2d");
    let taxonomy = Taxonomy::from_file(taxonomy_filename)?;
    let hash_filename = k2d_dir.join("hash_config.k2d");

    let mut hash_config = HashConfig::from_hash_header(&hash_filename)?;

    let start = Instant::now();

    let chunk_files = find_and_trans_files(&k2d_dir, "chunk", ".k2", true)?;

    let mut size: usize = 0;

    println!("start process k2 files...");
    for (i, chunk_file) in &chunk_files {
        let count = process_k2file(
            hash_config,
            &k2d_dir,
            &chunk_file,
            &taxonomy,
            hash_config.hash_capacity,
            *i,
        )?;
        size += count;
        let duration = start.elapsed();
        println!(
            "process chunk file {:?}/{:}: duration: {:?}",
            i, hash_config.partition, duration
        );
    }

    hash_config.size = size;
    hash_config.write_to_file(&hash_filename)?;


    let duration = start.elapsed();

    println!("build k2 db took: {:?}", duration);

    for (_, chunk_file) in &chunk_files {
        remove_file(chunk_file)?;
    }

    Ok(())
}

#[allow(dead_code)]
fn main() {
    let args = Args::parse();
    if let Err(e) = run(&args.database) {
        eprintln!("Application error: {}", e);
    }
}
