# PGit GitHub Action

[![GitHub Marketplace](https://img.shields.io/badge/marketplace-pgit-blue?logo=github)](https://github.com/marketplace/actions/pgit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Statistical data drift detection for your CI/CD pipelines.** Catch data quality issues before they reach production using Welch's t-test for statistical significance.

## Features

- 🔍 **Statistical Drift Detection** - Uses Welch's t-test to detect significant changes in data distributions
- 📊 **Multi-Feature Analysis** - Automatically checks all numeric columns in your datasets
- ☁️ **Cloud Storage Sync** - Push/pull manifests to AWS S3 or Google Cloud Storage
- 🎯 **Configurable Thresholds** - Set your own p-value threshold (default: 0.05)
- 🚦 **Flexible Failure Modes** - Choose whether to fail workflows on drift detection
- 📈 **Rich Outputs** - Get drift status, p-values, and feature counts as workflow outputs

## Quick Start

### Basic Usage

```yaml
name: Data Drift Check

on: [push]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Check for data drift
        uses: pgit-team/pgit-action@v1
        with:
          data_file: 'data/production.csv'
          dataset_name: 'production_data'
```

### With Remote Storage

```yaml
jobs:
  check:
    runs-on: ubuntu-latest
    env:
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Initialize and sync
        run: |
          pgit init
          pgit remote add s3 my-bucket --prefix pgit-data/
          pgit pull
      
      - name: Check drift
        uses: pgit-team/pgit-action@v1
        with:
          data_file: 'data/new_data.csv'
          dataset_name: 'production_data'
          threshold: '0.05'
```

## Inputs

| Input | Description | Default | Required |
|-------|-------------|---------|----------|
| `data_file` | Path to CSV/Parquet file to check | - | ✅ |
| `dataset_name` | Name for tracking this dataset | - | ✅ |
| `threshold` | P-value threshold for drift (0.05 = 95% confidence) | `0.05` | ❌ |
| `fail_on_drift` | Fail workflow if drift detected | `true` | ❌ |
| `remote_provider` | Storage provider: `s3` or `gcs` | - | ❌ |
| `remote_bucket` | Bucket name for remote storage | - | ❌ |
| `remote_prefix` | Path prefix within bucket | - | ❌ |
| `aws_region` | AWS region for S3 | `us-east-1` | ❌ |
| `action_type` | Action: `check`, `push`, `pull`, or `all` | `check` | ❌ |

## Outputs

| Output | Description |
|--------|-------------|
| `drift_detected` | `true` if statistical drift was detected |
| `min_p_value` | Minimum p-value across all features |
| `features_checked` | Number of features analyzed |

## Example Workflows

### 1. Simple Drift Check

```yaml
name: Data Validation
on: [push]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pgit-team/pgit-action@v1
        with:
          data_file: 'data/latest.csv'
          dataset_name: 'main_dataset'
```

### 2. Scheduled Monitoring with Alerts

```yaml
name: Daily Data Quality
on:
  schedule:
    - cron: '0 6 * * *'

jobs:
  monitor:
    runs-on: ubuntu-latest
    env:
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Check drift
        id: check
        uses: pgit-team/pgit-action@v1
        with:
          data_file: 'data/daily.csv'
          dataset_name: 'daily_data'
          fail_on_drift: 'false'
      
      - name: Alert on drift
        if: steps.check.outputs.drift_detected == 'true'
        run: |
          echo "Drift detected! p=${{ steps.check.outputs.min_p_value }}"
          # Send to Slack, PagerDuty, etc.
```

### 3. ML Pipeline Integration

```yaml
name: ML Training
on:
  pull_request:
    paths: ['data/**', 'src/**']

jobs:
  train:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Validate training data
        uses: pgit-team/pgit-action@v1
        with:
          data_file: 'data/train.csv'
          dataset_name: 'training'
          threshold: '0.01'  # Stricter for ML
      
      - name: Train model
        run: python train.py
```

## How It Works

PGit uses **Welch's t-test** to compare the means of numeric features between your current data and a baseline:

1. **Baseline**: First commit creates a statistical baseline (mean, variance, quantiles)
2. **Comparison**: Subsequent checks compute p-values for each feature
3. **Detection**: If p < threshold, the change is statistically significant
4. **Alert**: Workflow fails (optional) to prevent bad data from progressing

### Statistical Method

- **Test**: Welch's two-sample t-test (unequal variance)
- **Null Hypothesis**: Means are equal (no drift)
- **Alternative**: Means differ (drift detected)
- **Confidence**: 1 - threshold (default 95%)

## Remote Storage

### AWS S3

```bash
# Configure
pgit remote add s3 my-bucket --prefix pgit-data/ --region us-east-1

# Sync
pgit push
pgit pull
```

Required environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (optional)

### Google Cloud Storage

```bash
# Configure
pgit remote add gcs my-bucket --prefix pgit-data/

# Sync
pgit push
pgit pull
```

Required: Application Default Credentials or `GOOGLE_APPLICATION_CREDENTIALS`

## CLI Usage

```bash
# Initialize
pgit init

# Commit baseline
pgit commit data.csv my_dataset -m "Initial baseline"

# Check for drift
pgit check new_data.csv my_dataset --threshold 0.05

# View history
pgit log
pgit status

# Remote sync
pgit remote add s3 bucket --prefix path/
pgit push
pgit pull
```

## License

MIT License - see [LICENSE](../LICENSE) for details.

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](../CONTRIBUTING.md) first.
