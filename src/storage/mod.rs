//! Storage layer: SQLite-backed commit graph + manifest store.
//!
//! Schema managed by Refinery migrations (see `migrations/`).

pub mod remote;

use prost::Message;
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};

use std::path::{Path, PathBuf};

use crate::engine::stats::{StatisticalManifest, scan_dataset};
use crate::error::{PgitError, PgitResult};

const PGIT_DIR: &str = ".pgit";
const DB_FILE: &str = "pgit.db";

// Embed SQL migration files at compile time.
mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

// ── Path helpers ─────────────────────────────────────────────────────────────

pub fn pgit_dir() -> PathBuf {
    Path::new(PGIT_DIR).to_path_buf()
}

pub fn db_path() -> PathBuf {
    pgit_dir().join(DB_FILE)
}

// ── Init ─────────────────────────────────────────────────────────────────────

/// Create `.pgit/` directory and run all pending Refinery migrations.
pub fn init_repo() -> PgitResult<()> {
    let dir = pgit_dir();
    if dir.exists() {
        eprintln!(".pgit directory already exists");
        return Ok(());
    }

    std::fs::create_dir_all(&dir)?;

    let mut conn = Connection::open(db_path())?;
    embedded::migrations::runner()
        .run(&mut conn)
        .map_err(|_e| PgitError::Database(rusqlite::Error::InvalidQuery))?;

    println!("Initialized empty pgit repository in {}", dir.display());
    Ok(())
}

/// Open (and migrate) an existing database.  Returns an error if `.pgit` is
/// missing (user forgot to run `pgit init`).
pub fn open_db() -> PgitResult<Connection> {
    let path = db_path();
    if !path.exists() {
        return Err(PgitError::DatabaseNotFound);
    }
    let conn = Connection::open(&path)?;
    Ok(conn)
}

/// Open and run pending migrations — used by push/pull to ensure remote-pulled
/// databases are up-to-date.
pub fn open_and_migrate() -> PgitResult<Connection> {
    let path = db_path();
    std::fs::create_dir_all(pgit_dir())?;
    let mut conn = Connection::open(&path)?;
    embedded::migrations::runner()
        .run(&mut conn)
        .map_err(|_| PgitError::Database(rusqlite::Error::InvalidQuery))?;
    Ok(conn)
}

// ── Content-addressable hash (Phase 2) ───────────────────────────────────────

/// Hash the serialized Protobuf bytes with SHA-256.
/// Content-addressable: same stats → same hash, always, across Rust versions.
#[allow(dead_code)] // Used for integrity verification
pub fn hash_manifest(manifest: &StatisticalManifest) -> PgitResult<String> {
    let mut buf = Vec::new();
    manifest.encode(&mut buf)?;
    let digest = Sha256::digest(&buf);
    Ok(hex::encode(digest))
}

// ── Commit ────────────────────────────────────────────────────────────────────

/// Scan each (file, dataset_name) pair, build manifests, and commit them.
/// Returns the new commit hash.
pub fn commit_manifests(
    files: Vec<(String, String)>,
    message: &str,
) -> PgitResult<String> {
    let conn = open_db()?;

    let parent_hash: Option<String> = conn
        .query_row(
            "SELECT current_commit_hash FROM head WHERE id = 1",
            [],
            |row| row.get(0),
        )?;

    let timestamp = chrono::Utc::now().to_rfc3339();

    // Build manifests first so we can hash their content.
    let mut manifests: Vec<(String, String, StatisticalManifest)> = Vec::new();
    for (file_path, dataset_name) in &files {
        let manifest = scan_dataset(file_path, dataset_name)?;
        manifests.push((file_path.clone(), dataset_name.clone(), manifest));
    }

    // Commit hash = SHA-256 of all manifest bytes concatenated.
    let mut combined = Vec::new();
    for (_, _, m) in &manifests {
        m.encode(&mut combined)?;
    }
    combined.extend_from_slice(message.as_bytes());
    let new_hash = hex::encode(Sha256::digest(&combined));

    conn.execute(
        "INSERT INTO commits (hash, message, created_at, parent_hash)
         VALUES (?1, ?2, ?3, ?4)",
        params![new_hash, message, timestamp, parent_hash],
    )?;

    for (file_path, dataset_name, manifest) in &manifests {
        let mut blob = Vec::new();
        manifest.encode(&mut blob)?;

        conn.execute(
            "INSERT INTO manifests
             (commit_hash, file_path, dataset_name, manifest_blob, total_rows, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![new_hash, file_path, dataset_name, blob, manifest.total_rows, timestamp],
        )?;

        let manifest_id = conn.last_insert_rowid();

        for feature in &manifest.features {
            conn.execute(
                "INSERT INTO features
                 (manifest_id, feature_name, count, mean, variance, std_dev, min, max)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    manifest_id,
                    feature.feature_name,
                    feature.count,
                    feature.mean,
                    feature.variance,
                    feature.std_dev,
                    feature.min,
                    feature.max,
                ],
            )?;

            let feature_id = conn.last_insert_rowid();
            for q in &feature.quantiles {
                conn.execute(
                    "INSERT INTO quantiles (feature_id, percentile, value)
                     VALUES (?1, ?2, ?3)",
                    params![feature_id, q.percentile, q.value],
                )?;
            }
        }

        println!("  Added: {} ({})", file_path, dataset_name);
    }

    conn.execute(
        "UPDATE head SET current_commit_hash = ?1 WHERE id = 1",
        params![new_hash],
    )?;

    println!("\nCommitted: {} ({})", &new_hash[..12], message);
    Ok(new_hash)
}

// ── Baseline ──────────────────────────────────────────────────────────────────

/// Load the most recently committed manifest for `dataset_name`.
pub fn load_baseline(dataset_name: &str) -> PgitResult<StatisticalManifest> {
    let conn = open_db()?;

    let blob: Vec<u8> = conn
        .query_row(
            "SELECT manifest_blob FROM manifests
             WHERE dataset_name = ?1
             ORDER BY created_at DESC LIMIT 1",
            params![dataset_name],
            |row| row.get(0),
        )
        .map_err(|_| PgitError::NoBaseline(dataset_name.to_string()))?;

    let manifest = StatisticalManifest::decode(&mut &blob[..])?;
    Ok(manifest)
}

// ── Log / Status ──────────────────────────────────────────────────────────────

pub fn show_log() -> PgitResult<()> {
    let conn = open_db()?;

    let mut stmt = conn.prepare(
        "SELECT hash, message, created_at, parent_hash
         FROM commits ORDER BY created_at DESC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;

    println!("\n=== Commit History ===\n");
    for row in rows {
        let (hash, message, created_at, parent_hash) = row?;
        println!("commit {}", &hash[..12]);
        if let Some(p) = parent_hash {
            println!("Parent: {}", &p[..12]);
        }
        println!("Date:   {}", created_at);
        println!("\n    {}\n", message);
    }
    Ok(())
}

pub fn show_status() -> PgitResult<()> {
    let conn = open_db()?;

    let current_hash: Option<String> = conn
        .query_row(
            "SELECT current_commit_hash FROM head WHERE id = 1",
            [],
            |row| row.get(0),
        )?;

    println!("\n=== PGit Status ===\n");

    match current_hash {
        Some(hash) => {
            println!("HEAD: {}", &hash[..12]);

            let commit_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM commits", [], |r| r.get(0),
            )?;
            println!("Total commits:    {}", commit_count);

            let dataset_count: i64 = conn.query_row(
                "SELECT COUNT(DISTINCT dataset_name) FROM manifests", [], |r| r.get(0),
            )?;
            println!("Tracked datasets: {}", dataset_count);
        }
        None => println!("No commits yet"),
    }

    if let Ok(cfg) = remote::load_remote_config() {
        println!(
            "\nRemote: {}://{}/{}",
            cfg.provider,
            cfg.bucket,
            cfg.prefix.as_deref().unwrap_or("")
        );
    }

    println!();
    Ok(())
}
