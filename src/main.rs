//! pgit — Statistical Data Version Control
//!
//! Entry point: parse the CLI and dispatch to the engine and storage layers.

mod cli;
mod engine;
mod error;
mod storage;

use clap::Parser;
use cli::{Cli, Commands, RemoteAction};
use engine::stats::{compute_drift, scan_dataset};
use error::{PgitError, PgitResult};
use storage::remote;

fn main() -> PgitResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => {
            storage::init_repo()?;
        }

        Commands::Commit { file, name, message } => {
            storage::commit_manifests(vec![(file, name)], &message)?;
        }

        Commands::Check { file, name, threshold } => {
            let baseline = storage::load_baseline(&name)?;
            let current  = scan_dataset(&file, &name)?;

            println!("\n=== Statistical Drift Check ===");
            println!("Comparing: {} vs baseline", file);
            println!("Threshold: p < {:.4}\n", threshold);

            let drifts = compute_drift(&baseline, &current, threshold);
            let mut any_drift = false;

            for d in &drifts {
                let marker = if d.is_significant { "⚠️  DRIFT" } else { "✓ OK" };
                println!(
                    "{} {} [{}→{}] (p={:.6})",
                    marker, d.feature_name,
                    d.baseline_summary, d.current_summary,
                    d.p_value
                );
                if d.is_significant { any_drift = true; }
            }

            println!();
            if any_drift {
                println!("❌ DRIFT DETECTED (p < {:.4})", threshold);
                std::process::exit(1);
            } else {
                println!("✅ NO DRIFT DETECTED");
            }
        }

        Commands::Log => {
            storage::show_log()?;
        }

        Commands::Status => {
            storage::show_status()?;
        }

        Commands::Remote { action: RemoteAction::Add { provider, bucket, prefix, region } } => {
            remote::add_remote(
                &provider,
                &bucket,
                prefix.as_deref(),
                region.as_deref(),
            )?;
        }

        Commands::Push => {
            let cfg = remote::load_remote_config()?;
            println!(
                "Pushing to {}://{}/{}...",
                cfg.provider, cfg.bucket,
                cfg.prefix.as_deref().unwrap_or("")
            );
            tokio::runtime::Runtime::new()
                .map_err(|e| PgitError::Remote(e.to_string()))?
                .block_on(remote::push_to_s3(&cfg))?;
        }

        Commands::Pull => {
            let cfg = remote::load_remote_config()?;
            println!(
                "Pulling from {}://{}/{}...",
                cfg.provider, cfg.bucket,
                cfg.prefix.as_deref().unwrap_or("")
            );
            tokio::runtime::Runtime::new()
                .map_err(|e| PgitError::Remote(e.to_string()))?
                .block_on(remote::pull_from_s3(&cfg))?;
        }
    }

    Ok(())
}
