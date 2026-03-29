//! CLI definitions powered by Clap.
//!
//! Run `pgit --help` or `pgit <command> --help` for auto-generated usage.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "pgit",
    version,
    about = "Statistical data version control with drift detection",
    long_about = None,
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new .pgit repository in the current directory.
    Init,

    /// Scan a dataset and commit its statistical manifest.
    Commit {
        /// Path to the CSV or Parquet file.
        file: String,

        /// Logical name for this dataset (used for drift comparisons).
        name: String,

        /// Commit message describing this snapshot.
        #[arg(short, long)]
        message: String,
    },

    /// Compare the current dataset against its committed baseline.
    Check {
        /// Path to the CSV or Parquet file.
        file: String,

        /// Logical dataset name to look up the baseline.
        name: String,

        /// P-value significance threshold (default: 0.05).
        #[arg(long, default_value_t = 0.05)]
        threshold: f64,
    },

    /// Show commit history.
    Log,

    /// Show repository status.
    Status,

    /// Manage remote storage.
    Remote {
        #[command(subcommand)]
        action: RemoteAction,
    },

    /// Push manifests to remote storage.
    Push,

    /// Pull manifests from remote storage.
    Pull,
}

#[derive(Subcommand)]
pub enum RemoteAction {
    /// Configure a new remote.
    Add {
        /// Storage provider (currently only 's3').
        provider: String,

        /// Bucket name.
        bucket: String,

        /// Optional key prefix inside the bucket.
        #[arg(long)]
        prefix: Option<String>,

        /// AWS region (e.g. us-east-1).
        #[arg(long)]
        region: Option<String>,
    },
}
