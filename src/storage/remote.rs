//! Remote storage: push/pull manifests to/from S3.
//!
//! S3 key scheme: `{prefix}manifests/{commit_hash}.pb`
//! No timestamp-based filenames — the hash IS the identity.

use aws_sdk_s3::primitives::ByteStream;

use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::error::{PgitError, PgitResult};
use crate::storage::{db_path, open_and_migrate};

const REMOTE_CONFIG_FILE: &str = ".pgit-remote";

// ── Remote config ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    pub provider: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub region: Option<String>,
}

pub fn load_remote_config() -> PgitResult<RemoteConfig> {
    let p = Path::new(REMOTE_CONFIG_FILE);
    if !p.exists() {
        return Err(PgitError::Config(
            "No remote configured. Run 'pgit remote add s3 <bucket>' first.".into(),
        ));
    }
    let content = fs::read_to_string(p)?;
    let cfg: RemoteConfig = serde_json::from_str(&content)?;
    Ok(cfg)
}

pub fn save_remote_config(cfg: &RemoteConfig) -> PgitResult<()> {
    let content = serde_json::to_string_pretty(cfg)?;
    fs::write(REMOTE_CONFIG_FILE, content)?;
    Ok(())
}

pub fn add_remote(
    provider: &str,
    bucket: &str,
    prefix: Option<&str>,
    region: Option<&str>,
) -> PgitResult<()> {
    if provider != "s3" {
        return Err(PgitError::Remote(format!(
            "Unsupported provider '{}'. Only 's3' is supported.",
            provider
        )));
    }
    let cfg = RemoteConfig {
        provider: provider.to_string(),
        bucket: bucket.to_string(),
        prefix: prefix.map(String::from),
        region: region.map(String::from),
    };
    save_remote_config(&cfg)?;
    println!(
        "✅ Remote configured: {}://{}/{}",
        provider,
        bucket,
        prefix.unwrap_or("")
    );
    Ok(())
}

// ── S3 helpers ────────────────────────────────────────────────────────────────

fn manifest_key(prefix: &str, commit_hash: &str) -> String {
    format!("{}manifests/{}.pb", prefix, commit_hash)
}

fn db_key(prefix: &str) -> String {
    format!("{}pgit.db", prefix)
}

async fn build_s3_client(_cfg: &RemoteConfig) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::load_from_env().await;
    aws_sdk_s3::Client::new(&sdk_config)
}

// ── Push ──────────────────────────────────────────────────────────────────────

pub async fn push_to_s3(cfg: &RemoteConfig) -> PgitResult<()> {
    let conn = super::open_db()?;
    let s3 = build_s3_client(cfg).await;
    let prefix = cfg.prefix.as_deref().unwrap_or("");

    let mut stmt = conn.prepare(
        "SELECT c.hash, m.manifest_blob
         FROM manifests m
         JOIN commits c ON c.hash = m.commit_hash",
    )?;

    let rows: Vec<(String, Vec<u8>)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;

    let mut pushed = 0usize;
    for (hash, blob) in rows {
        let key = manifest_key(prefix, &hash);
        s3.put_object()
            .bucket(&cfg.bucket)
            .key(&key)
            .body(ByteStream::from(blob))
            .send()
            .await
            .map_err(|e| PgitError::Remote(e.to_string()))?;
        println!("  Pushed: {}", key);
        pushed += 1;
    }

    // Also push the SQLite database.
    let db_bytes = fs::read(db_path())?;
    s3.put_object()
        .bucket(&cfg.bucket)
        .key(&db_key(prefix))
        .body(ByteStream::from(db_bytes))
        .send()
        .await
        .map_err(|e| PgitError::Remote(e.to_string()))?;
    println!("  Pushed: {}", db_key(prefix));

    println!(
        "\n✅ Pushed {} manifest(s) to s3://{}/{}",
        pushed,
        cfg.bucket,
        prefix
    );
    Ok(())
}

// ── Pull ──────────────────────────────────────────────────────────────────────

pub async fn pull_from_s3(cfg: &RemoteConfig) -> PgitResult<()> {
    let s3 = build_s3_client(cfg).await;
    let prefix = cfg.prefix.as_deref().unwrap_or("");
    let manifests_prefix = format!("{}manifests/", prefix);

    let resp = s3
        .list_objects_v2()
        .bucket(&cfg.bucket)
        .prefix(&manifests_prefix)
        .send()
        .await
        .map_err(|e| PgitError::Remote(e.to_string()))?;

    let conn = open_and_migrate()?;
    let mut pulled = 0usize;

    if let Some(contents) = resp.contents {
        for obj in contents {
            let key = match obj.key {
                Some(k) => k,
                None => continue,
            };

            // Extract commit hash from key: `{prefix}manifests/{hash}.pb`
            let hash = key
                .rsplit('/')
                .next()
                .map(|s| s.trim_end_matches(".pb"))
                .unwrap_or("")
                .to_string();

            if hash.is_empty() {
                continue;
            }

            // Skip if we already have this commit.
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM commits WHERE hash = ?1)",
                    params![hash],
                    |row| row.get(0),
                )?;

            if exists {
                continue;
            }

            let resp = s3
                .get_object()
                .bucket(&cfg.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| PgitError::Remote(e.to_string()))?;

            let blob = resp
                .body
                .collect()
                .await
                .map_err(|e| PgitError::Remote(e.to_string()))?
                .into_bytes()
                .to_vec();

            let timestamp = chrono::Utc::now().to_rfc3339();

            // Insert a synthetic commit record for the pulled manifest.
            conn.execute(
                "INSERT OR IGNORE INTO commits (hash, message, created_at, parent_hash)
                 VALUES (?1, ?2, ?3, NULL)",
                params![hash, "pulled from remote", timestamp],
            )?;

            conn.execute(
                "INSERT INTO manifests
                 (commit_hash, file_path, dataset_name, manifest_blob, total_rows, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![hash, "remote", "remote", blob, 0_i64, timestamp],
            )?;

            println!("  Pulled: {}", key);
            pulled += 1;
        }
    }

    // Try to pull the full database (overrides local if found).
    match s3
        .get_object()
        .bucket(&cfg.bucket)
        .key(&db_key(prefix))
        .send()
        .await
    {
        Ok(resp) => {
            let bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| PgitError::Remote(e.to_string()))?
                .into_bytes()
                .to_vec();
            fs::write(db_path(), bytes)?;
            println!("  Pulled: {}", db_key(prefix));
        }
        Err(_) => {} // No database in remote — fine.
    }

    println!(
        "\n✅ Pulled {} new manifest(s) from s3://{}/{}",
        pulled,
        cfg.bucket,
        prefix
    );
    Ok(())
}
