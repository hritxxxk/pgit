# pgit: Statistical Data Version Control

**pgit** is a high-performance version control system for tracking the *statistical integrity* of datasets. Built in Rust and powered by **Polars**, it enables engineers to snapshot, audit, and detect drift in massive CSV and Parquet files without storing raw data.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

---

## 🔬 The Core Problem
Traditional Git tracks **line-by-line byte changes**. This is insufficient for data engineering:
- **Noise:** A shuffle or re-encoding of a 1GB CSV is a "major change" to Git, but statistically identical.
- **Silent Failures:** A subtle shift in a feature's mean (drift) might break an ML model but pass a byte-level diff.

**pgit** solves this by tracking **Statistical Manifests**—Protobuf-serialized distributions that provide a mathematical "fingerprint" of your data.

## 🏗 Architecture & Performance
- **Polars Engine:** Uses vectorized SIMD operations to scan multi-million row datasets in seconds.
- **Content-Addressable Manifests:** Summary statistics are hashed (SHA-256) to create deterministic commit IDs.
- **Local State:** An embedded **SQLite** database manages the commit graph and dataset baselines.
- **Zero-Trust Storage:** Only statistical metadata is synced to remote storage (S3); your raw data never leaves your infrastructure.

## 🚀 Quickstart

### 1. Initialize the Workspace
```bash
pgit init
```

### 2. Establish a Statistical Baseline
Scan a dataset and commit its distribution to the local history.
```bash
pgit commit train_v1.parquet training_data -m "Initial training baseline"
```

### 3. Detect Drift (CI/CD)
Compare new data against the committed baseline. Fails with `exit 1` if the $p$-value falls below the significance threshold.
```bash
pgit check batch_2024_03.csv training_data --threshold 0.05
```

## 📊 Statistical Methodology

| Data Type | Test | Metric |
| :--- | :--- | :--- |
| **Numeric** | **Welch's t-test** | Robust comparison of means under unequal variance. |
| **Categorical** | **$\chi^2$ test** | Goodness-of-fit for frequency distribution shifts. |
| **Text** | **Semantic Centroid** | (Optional) BERT-based embedding similarity check. |

## 🛠 Command Reference

| Command | Action |
| :--- | :--- |
| `init` | Bootstraps `.pgit/` and the SQLite internal schema. |
| `commit` | Scans `[file]`, computes stats, and appends to the commit graph. |
| `check` | Validates current data against the last known baseline for a dataset. |
| `log` | Displays the statistical audit trail. |
| `push`/`pull` | Synchronizes manifests with S3-compatible remote storage. |

## 🤖 CI/CD Integration
pgit is designed to block pipelines when data quality degrades.

### GitHub Action
```yaml
- uses: pgit-team/pgit-action@v1
  with:
    data_file: 'data/current.csv'
    dataset_name: 'production_main'
    threshold: 0.01  # Stricter 99% confidence interval
```

### Git Hooks (Pre-commit)
Ensure local data hasn't drifted before pushing code changes:
```bash
pgit check local_dev.csv prod_dataset || exit 1
```

---
**pgit** is open-source under the [MIT License](./LICENSE). Built with 🦀 in Rust.
