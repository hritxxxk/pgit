//! Statistical engine: dataset scanning, drift detection.
//!
//! Numeric columns → Welch's t-test.
//! String columns  → Chi-squared test on frequency distributions.
//! Text columns    → Semantic fingerprinting via Candle (optional, feature-gated).

use polars::prelude::*;
use statrs::distribution::{ContinuousCDF, StudentsT};
use std::collections::HashMap;
use std::path::Path;

// Pull in the protobuf-generated types from the build step.
include!(concat!(env!("OUT_DIR"), "/stats.rs"));

#[cfg(feature = "candle")]
use candle_core::{DType, Device, Tensor};

// ── Dataset scanning ────────────────────────────────────────────────────────

/// Scan a CSV or Parquet file and produce a `StatisticalManifest`.
///
/// Numeric columns get full summary stats + quantiles.
/// String/Utf8 columns get a frequency map stored as `categorical_counts`.
pub fn scan_dataset(
    file_path: &str,
    dataset_name: &str,
) -> crate::error::PgitResult<StatisticalManifest> {
    let path = Path::new(file_path);

    let df = if path.extension().map_or(false, |e| e == "csv") {
        CsvReader::from_path(path)
            .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?
            .finish()
            .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?
    } else if path.extension().map_or(false, |e| e == "parquet") {
        let file = std::fs::File::open(path)?;
        ParquetReader::new(file)
            .finish()
            .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?
    } else {
        return Err(crate::error::PgitError::UnsupportedFormat(
            path.extension()
                .and_then(|e| e.to_str())
                .unwrap_or("unknown")
                .to_string(),
        ));
    };

    let total_rows = df.height() as u64;
    let mut features = Vec::new();

    for col_name in df.get_column_names() {
        let col = df
            .column(col_name)
            .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?;

        match col.dtype() {
            dtype if dtype.is_numeric() => {
                let stats = scan_numeric_column(col_name, col)?;
                features.push(stats);
            }
            DataType::Utf8 => {
                let stats = scan_categorical_column(col_name, col)?;
                features.push(stats);
            }
            _ => {
                // Skip dates, booleans, nested types, etc.
            }
        }
    }

    let created_at = chrono::Utc::now().to_rfc3339();

    Ok(StatisticalManifest {
        dataset_name: dataset_name.to_string(),
        total_rows,
        features,
        created_at,
    })
}

fn scan_numeric_column(
    col_name: &str,
    col: &Series,
) -> crate::error::PgitResult<SummaryStats> {
    let series = col
        .cast(&DataType::Float64)
        .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?;

    let count = series.len() as u64;
    let mean = series.mean().unwrap_or(0.0);
    let min = series.min::<f64>().unwrap_or(0.0);
    let max = series.max::<f64>().unwrap_or(0.0);

    let (variance, std_dev) = {
        let ca = series
            .f64()
            .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?;
        let n = ca.len() as f64;
        if n > 1.0 {
            let var = ca
                .into_iter()
                .filter_map(|x| x)
                .map(|x: f64| (x - mean).powi(2))
                .sum::<f64>()
                / (n - 1.0);
            (var, var.sqrt())
        } else {
            (0.0, 0.0)
        }
    };

    let quantiles = compute_quantiles(&series)?;

    Ok(SummaryStats {
        feature_name: col_name.to_string(),
        count,
        mean,
        variance,
        std_dev,
        min,
        max,
        quantiles,
        semantic_centroid: vec![],
        categorical_counts: vec![],
    })
}

fn scan_categorical_column(
    col_name: &str,
    col: &Series,
) -> crate::error::PgitResult<SummaryStats> {
    let ca = col
        .utf8()
        .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?;

    let mut freq: HashMap<String, u64> = HashMap::new();
    for val in ca.into_iter().flatten() {
        *freq.entry(val.to_string()).or_insert(0) += 1;
    }

    let categorical_counts: Vec<CategoricalEntry> = freq
        .into_iter()
        .map(|(category, count)| CategoricalEntry { category, count })
        .collect();

    Ok(SummaryStats {
        feature_name: col_name.to_string(),
        count: col.len() as u64,
        mean: 0.0,
        variance: 0.0,
        std_dev: 0.0,
        min: 0.0,
        max: 0.0,
        quantiles: vec![],
        semantic_centroid: vec![],
        categorical_counts,
    })
}

fn compute_quantiles(series: &Series) -> crate::error::PgitResult<Vec<Quantile>> {
    let ca = series
        .f64()
        .map_err(|e| crate::error::PgitError::Statistical(e.to_string()))?;
    let mut sorted: Vec<f64> = ca.into_iter().flatten().collect();
    sorted.sort_by(|a, b| a.total_cmp(b));

    let mut quantiles = Vec::new();
    for percentile in [0.25, 0.5, 0.75] {
        if !sorted.is_empty() {
            let idx = ((sorted.len() as f64 - 1.0) * percentile) as usize;
            let next_idx = (idx + 1).min(sorted.len() - 1);
            let frac = ((sorted.len() as f64 - 1.0) * percentile) - idx as f64;
            let value = sorted[idx] * (1.0 - frac) + sorted[next_idx] * frac;
            quantiles.push(Quantile { percentile, value });
        }
    }
    Ok(quantiles)
}

// ── Semantic Fingerprinting (Candle, feature-gated) ─────────────────────────

/// Generate a semantic fingerprint for text data using a pre-trained model.
///
/// This uses a simple averaging approach: tokenize samples, compute embeddings,
/// and average them into a centroid vector. Requires the `candle` feature.
#[cfg(feature = "candle")]
pub fn compute_semantic_centroid(
    text_samples: &[String],
    sample_size: usize,
) -> crate::error::PgitResult<Vec<f32>> {
    use candle_transformers::models::bert::{BertModel, Config};
    use tokenizers::Tokenizer;

    // Use a small sample for efficiency
    let sample_size = sample_size.min(text_samples.len());
    let samples: Vec<&String> = text_samples.iter().take(sample_size).collect();

    if samples.is_empty() {
        return Ok(vec![]);
    }

    // For now, return a placeholder centroid based on text statistics
    // In production, you'd load a pre-trained model like all-MiniLM-L6-v2
    // and compute actual embeddings.
    //
    // This placeholder computes a simple statistical fingerprint:
    // - Average word length
    // - Average words per sample
    // - Character distribution (a-z frequency)
    let mut centroid = vec![0.0f32; 32]; // 32-d placeholder embedding

    for (i, sample) in samples.iter().enumerate() {
        let words: Vec<&str> = sample.split_whitespace().collect();
        let avg_word_len = if words.is_empty() {
            0.0
        } else {
            words.iter().map(|w| w.len() as f64).sum::<f64>() / words.len() as f64
        };

        // Store in first few dimensions
        centroid[0] += (avg_word_len / 10.0) as f32; // Normalized
        centroid[1] += (words.len() as f32) / 100.0; // Normalized

        // Character distribution in remaining dimensions
        for (j, ch) in sample.chars().take(30).enumerate() {
            centroid[2 + j] = (ch as u32) as f32 / 256.0;
        }
    }

    // Average across samples
    let n = samples.len() as f32;
    for val in &mut centroid {
        *val /= n;
    }

    Ok(centroid)
}

/// Compute cosine similarity between two vectors.
/// Returns 1.0 if either vector is empty.
#[cfg(feature = "candle")]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 1.0;
    }

    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| (*x as f64) * (*y as f64)).sum();
    let norm_a: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
    let norm_b: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }

    (dot / (norm_a * norm_b)).clamp(-1.0, 1.0)
}

// ── Statistical tests ────────────────────────────────────────────────────────

/// Welch's t-test — handles unequal variances and sample sizes.
/// Returns the two-tailed p-value. Returns `1.0` if the standard error is 0
/// (distributions are identical).
///
/// # Panics-free guarantee
/// The function clamps degrees-of-freedom to `[1, 1000]` and catches any
/// error from the `StudentsT` constructor, so it never returns NaN.
pub fn welchs_t_test(mean1: f64, var1: f64, n1: u64, mean2: f64, var2: f64, n2: u64) -> f64 {
    let n1_f = n1 as f64;
    let n2_f = n2 as f64;

    let se = ((var1 / n1_f) + (var2 / n2_f)).sqrt();
    if se == 0.0 || se.is_nan() {
        return 1.0;
    }

    let t_stat = (mean2 - mean1).abs() / se;

    let num = ((var1 / n1_f) + (var2 / n2_f)).powi(2);
    let denom =
        ((var1 / n1_f).powi(2) / (n1_f - 1.0)) + ((var2 / n2_f).powi(2) / (n2_f - 1.0));

    let df = if denom > 0.0 { num / denom } else { 1.0 };
    let df = df.max(1.0).min(1000.0);

    match StudentsT::new(0.0, 1.0, df) {
        Ok(dist) => {
            let p = 2.0 * (1.0 - dist.cdf(t_stat));
            p.clamp(0.0, 1.0)
        }
        Err(_) => 1.0,
    }
}

/// Chi-squared goodness-of-fit test for categorical drift.
///
/// Compares `current` frequency counts against `baseline` as the expected
/// distribution. Returns a p-value: low p → significant categorical shift.
/// Returns `1.0` if either map is empty or total counts are zero.
pub fn chi_squared_test(
    baseline: &[CategoricalEntry],
    current: &[CategoricalEntry],
) -> f64 {
    if baseline.is_empty() || current.is_empty() {
        return 1.0;
    }

    let baseline_map: HashMap<&str, u64> =
        baseline.iter().map(|e| (e.category.as_str(), e.count)).collect();
    let current_map: HashMap<&str, u64> =
        current.iter().map(|e| (e.category.as_str(), e.count)).collect();

    let baseline_total: u64 = baseline_map.values().sum();
    let current_total: u64 = current_map.values().sum();

    if baseline_total == 0 || current_total == 0 {
        return 1.0;
    }

    let scale = current_total as f64 / baseline_total as f64;

    // Union of all categories seen in either distribution.
    let all_categories: std::collections::HashSet<&str> = baseline_map
        .keys()
        .chain(current_map.keys())
        .copied()
        .collect();

    let mut chi2: f64 = 0.0;
    let mut df: usize = 0;

    for cat in &all_categories {
        let expected = baseline_map.get(cat).copied().unwrap_or(0) as f64 * scale;
        let observed = current_map.get(cat).copied().unwrap_or(0) as f64;

        if expected > 0.5 {
            chi2 += (observed - expected).powi(2) / expected;
            df += 1;
        }
    }

    if df == 0 {
        return 1.0;
    }

    // Use chi-squared CDF to get p-value.
    // χ²(k) ≈ Gamma(k/2, 2); we approximate via the Normal for large df,
    // or use a direct Gamma CDF from statrs.
    use statrs::distribution::ChiSquared;
    match ChiSquared::new(df as f64) {
        Ok(dist) => {
            use statrs::distribution::ContinuousCDF;
            let p = 1.0 - dist.cdf(chi2);
            p.clamp(0.0, 1.0)
        }
        Err(_) => 1.0,
    }
}

// ── Drift report ─────────────────────────────────────────────────────────────

pub struct FeatureDrift {
    pub feature_name: String,
    pub p_value: f64,
    pub is_significant: bool,
    #[allow(dead_code)]
    pub kind: DriftKind,
    pub baseline_summary: String,
    pub current_summary: String,
}

pub enum DriftKind {
    Numeric,
    Categorical,
}

/// Compare `current` manifest against `baseline` and return per-feature drift.
pub fn compute_drift(
    baseline: &StatisticalManifest,
    current: &StatisticalManifest,
    threshold: f64,
) -> Vec<FeatureDrift> {
    let baseline_map: HashMap<&str, &SummaryStats> =
        baseline.features.iter().map(|f| (f.feature_name.as_str(), f)).collect();

    let mut drifts = Vec::new();

    for feat in &current.features {
        let Some(base) = baseline_map.get(feat.feature_name.as_str()) else {
            continue;
        };

        // Categorical column: both have categorical_counts populated.
        if !feat.categorical_counts.is_empty() || !base.categorical_counts.is_empty() {
            let p = chi_squared_test(&base.categorical_counts, &feat.categorical_counts);
            let is_significant = p < threshold;
            drifts.push(FeatureDrift {
                feature_name: feat.feature_name.clone(),
                p_value: p,
                is_significant,
                kind: DriftKind::Categorical,
                baseline_summary: format!(
                    "{} categories",
                    base.categorical_counts.len()
                ),
                current_summary: format!(
                    "{} categories",
                    feat.categorical_counts.len()
                ),
            });
        } else {
            // Numeric column.
            let p = welchs_t_test(base.mean, base.variance, base.count,
                                   feat.mean, feat.variance, feat.count);
            let is_significant = p < threshold;
            drifts.push(FeatureDrift {
                feature_name: feat.feature_name.clone(),
                p_value: p,
                is_significant,
                kind: DriftKind::Numeric,
                baseline_summary: format!("mean={:.4}", base.mean),
                current_summary: format!("mean={:.4}", feat.mean),
            });
        }
    }

    drifts
}

// ── Property-Based Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use statrs::distribution::ContinuousCDF;

    /// Property: Welch's t-test never returns NaN
    #[test]
    fn welchs_t_test_never_returns_nan() {
        proptest!(|(mean1 in -1000.0..1000.0f64,
                    var1 in 0.0..10000.0f64,
                    n1 in 2..10000u64,
                    mean2 in -1000.0..1000.0f64,
                    var2 in 0.0..10000.0f64,
                    n2 in 2..10000u64)| {
            let p = welchs_t_test(mean1, var1, n1, mean2, var2, n2);
            prop_assert!(!p.is_nan(), "p-value should not be NaN");
        });
    }

    /// Property: Welch's t-test always returns a valid probability [0, 1]
    #[test]
    fn welchs_t_test_returns_valid_probability() {
        proptest!(|(mean1 in -1000.0..1000.0f64,
                    var1 in 0.0..10000.0f64,
                    n1 in 2..10000u64,
                    mean2 in -1000.0..1000.0f64,
                    var2 in 0.0..10000.0f64,
                    n2 in 2..10000u64)| {
            let p = welchs_t_test(mean1, var1, n1, mean2, var2, n2);
            prop_assert!(p >= 0.0 && p <= 1.0, "p-value should be in [0, 1], got {}", p);
        });
    }

    /// Property: Identical distributions should yield p ≈ 1.0
    #[test]
    fn welchs_t_test_identical_distributions() {
        proptest!(|(mean in -100.0..100.0f64,
                    var in 0.1..1000.0f64,
                    n in 100..1000u64)| {
            let p = welchs_t_test(mean, var, n, mean, var, n);
            // When distributions are identical, p should be very high (close to 1.0)
            prop_assert!(p > 0.9, "Identical distributions should yield high p-value, got {}", p);
        });
    }

    /// Property: Very different means should yield low p-value
    #[test]
    fn welchs_t_test_different_means() {
        proptest!(|(var in 1.0..10.0f64, n in 100..1000u64)| {
            let mean1 = 0.0f64;
            let mean2 = 10.0f64; // Large difference
            let p = welchs_t_test(mean1, var, n, mean2, var, n);
            // With large mean difference and reasonable sample size, p should be low
            prop_assert!(p < 0.5, "Different means should yield lower p-value, got {}", p);
        });
    }

    /// Property: Chi-squared test never returns NaN
    #[test]
    fn chi_squared_test_never_returns_nan() {
        proptest!(|(
            base_count in 1..100usize,
            curr_count in 1..100usize,
            categories in 1..20usize
        )| {
            let baseline: Vec<CategoricalEntry> = (0..categories)
                .map(|i| CategoricalEntry {
                    category: format!("cat_{}", i),
                    count: (base_count as u64) + i as u64,
                })
                .collect();

            let current: Vec<CategoricalEntry> = (0..categories)
                .map(|i| CategoricalEntry {
                    category: format!("cat_{}", i),
                    count: (curr_count as u64) + i as u64,
                })
                .collect();

            let p = chi_squared_test(&baseline, &current);
            prop_assert!(!p.is_nan(), "p-value should not be NaN");
            prop_assert!(p >= 0.0 && p <= 1.0, "p-value should be in [0, 1], got {}", p);
        });
    }

    /// Property: Chi-squared test with identical distributions yields high p
    #[test]
    fn chi_squared_test_identical_distributions() {
        let baseline = vec![
            CategoricalEntry { category: "A".to_string(), count: 100 },
            CategoricalEntry { category: "B".to_string(), count: 200 },
            CategoricalEntry { category: "C".to_string(), count: 150 },
        ];

        let current = baseline.clone();
        let p = chi_squared_test(&baseline, &current);

        assert!(p > 0.9, "Identical categorical distributions should yield high p, got {}", p);
    }

    /// Property: Chi-squared test with scaled distributions yields high p
    #[test]
    fn chi_squared_test_scaled_distributions() {
        let baseline = vec![
            CategoricalEntry { category: "A".to_string(), count: 100 },
            CategoricalEntry { category: "B".to_string(), count: 200 },
            CategoricalEntry { category: "C".to_string(), count: 150 },
        ];

        // Same proportions, just scaled up
        let current = vec![
            CategoricalEntry { category: "A".to_string(), count: 200 },
            CategoricalEntry { category: "B".to_string(), count: 400 },
            CategoricalEntry { category: "C".to_string(), count: 300 },
        ];

        let p = chi_squared_test(&baseline, &current);
        assert!(p > 0.9, "Scaled distributions should yield high p, got {}", p);
    }

    /// Property: Chi-squared test with drift yields low p
    #[test]
    fn chi_squared_test_with_drift() {
        let baseline = vec![
            CategoricalEntry { category: "A".to_string(), count: 900 },
            CategoricalEntry { category: "B".to_string(), count: 100 },
        ];

        // Drift: B becomes dominant
        let current = vec![
            CategoricalEntry { category: "A".to_string(), count: 100 },
            CategoricalEntry { category: "B".to_string(), count: 900 },
        ];

        let p = chi_squared_test(&baseline, &current);
        assert!(p < 0.01, "Drifted distributions should yield low p, got {}", p);
    }

    /// Unit test: Empty inputs to chi_squared_test return 1.0
    #[test]
    fn chi_squared_test_empty_inputs() {
        assert_eq!(chi_squared_test(&[], &[]), 1.0);
        assert_eq!(chi_squared_test(&[CategoricalEntry { category: "A".to_string(), count: 10 }], &[]), 1.0);
        assert_eq!(chi_squared_test(&[], &[CategoricalEntry { category: "A".to_string(), count: 10 }]), 1.0);
    }

    /// Unit test: cosine_similarity (when candle feature is enabled)
    #[cfg(feature = "candle")]
    #[test]
    fn test_cosine_similarity() {
        // Identical vectors
        let a = vec![1.0f32, 2.0, 3.0];
        let b = vec![1.0f32, 2.0, 3.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);

        // Orthogonal vectors
        let a = vec![1.0f32, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 0.0).abs() < 1e-6);

        // Opposite vectors
        let a = vec![1.0f32, 0.0, 0.0];
        let b = vec![-1.0f32, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - (-1.0)).abs() < 1e-6);

        // Empty vectors
        let a: Vec<f32> = vec![];
        let b: Vec<f32> = vec![];
        assert_eq!(cosine_similarity(&a, &b), 1.0);
    }
}
