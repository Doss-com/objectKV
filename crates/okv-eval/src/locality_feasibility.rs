//! Closed-form feasibility bounds for provider-backed local placement.

use serde::Serialize;
use sha2::{Digest, Sha256};

const PARTS_PER_MILLION: u64 = 1_000_000;
const PARTS_PER_MILLION_USIZE: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalityDistribution {
    Uniform,
    Zipfian {
        theta_milli: u32,
    },
    MovingHotset {
        hotset_fraction_ppm: u32,
        hot_read_fraction_ppm: u32,
    },
}

impl LocalityDistribution {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Zipfian { .. } => "zipfian",
            Self::MovingHotset { .. } => "moving_hotset",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalityFeasibilityMode {
    #[default]
    Correct,
    InflateCapacity,
    SkipProbabilityNormalization,
    IgnoreBackgroundReads,
}

impl LocalityFeasibilityMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::InflateCapacity => "inflate_capacity",
            Self::SkipProbabilityNormalization => "skip_probability_normalization",
            Self::IgnoreBackgroundReads => "ignore_background_reads",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LocalityFeasibilityInput {
    pub key_count: u64,
    pub logical_bytes: u64,
    pub point_bytes: u64,
    pub cache_fraction_ppm: u32,
    pub distribution: LocalityDistribution,
    pub provider_get_cost_per_million_usd: f64,
    pub target_request_cost_per_million_reads_usd: f64,
    pub expected_provider_miss_ratio: f64,
    pub probability_tolerance: f64,
    pub mode: LocalityFeasibilityMode,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct LocalityFeasibilityReceipt {
    pub contract_version: u32,
    pub model: String,
    pub mode: String,
    pub distribution: String,
    pub key_count: u64,
    pub logical_bytes: u64,
    pub point_bytes: u64,
    pub cache_fraction_ppm: u32,
    pub declared_capacity_keys: u64,
    pub declared_capacity_bytes: u64,
    pub modeled_resident_keys: u64,
    pub modeled_resident_bytes: u64,
    pub probability_mass: f64,
    pub closed_form_hit_ratio: f64,
    pub enumerated_hit_ratio: f64,
    pub ideal_hit_ratio: f64,
    pub irreducible_miss_ratio: f64,
    pub target_provider_miss_ratio: f64,
    pub locality_target_gap: f64,
    pub provider_request_cost_floor_per_million_reads_usd: f64,
    pub probability_mass_normalized: bool,
    pub closed_form_matches_enumeration: bool,
    pub capacity_bound_held: bool,
    pub hit_plus_miss_equals_one: bool,
    pub request_cost_target_reproduced: bool,
    pub receipt_sha256: String,
}

/// Calculate the strongest local-hit bound allowed by one workload and
/// capacity contract.
///
/// # Errors
///
/// Returns an error for invalid dimensions, probabilities, costs, or controls
/// that do not apply to the selected distribution.
pub fn evaluate_locality_feasibility(
    input: LocalityFeasibilityInput,
) -> Result<LocalityFeasibilityReceipt, String> {
    validate_input(input)?;
    let key_count = usize::try_from(input.key_count)
        .map_err(|_| "locality key count does not fit usize".to_owned())?;
    let declared_capacity_bytes = input
        .logical_bytes
        .saturating_mul(u64::from(input.cache_fraction_ppm))
        / PARTS_PER_MILLION;
    let declared_capacity_keys = declared_capacity_bytes / input.point_bytes;
    if declared_capacity_keys == 0 {
        return Err("locality capacity must hold at least one complete point".to_owned());
    }
    let modeled_resident_keys = if input.mode == LocalityFeasibilityMode::InflateCapacity {
        input.key_count
    } else {
        declared_capacity_keys
    };
    let modeled_resident_bytes = modeled_resident_keys.saturating_mul(input.point_bytes);
    let modeled_capacity = usize::try_from(modeled_resident_keys)
        .map_err(|_| "modeled locality capacity does not fit usize".to_owned())?
        .min(key_count);
    let key_count_f64 = f64::from(
        u32::try_from(input.key_count)
            .map_err(|_| "locality model supports at most u32::MAX keys".to_owned())?,
    );
    let capacity_fraction = f64::from(
        u32::try_from(modeled_capacity)
            .map_err(|_| "locality capacity does not fit u32".to_owned())?,
    ) / key_count_f64;

    let (probabilities, closed_form_hit_ratio) = distribution_model(
        input.distribution,
        input.mode,
        key_count,
        modeled_capacity,
        capacity_fraction,
    )?;
    let probability_mass = probabilities.iter().sum::<f64>();
    let mut ranked_probabilities = probabilities;
    ranked_probabilities.sort_by(|left, right| right.total_cmp(left));
    let enumerated_hit_ratio = ranked_probabilities
        .iter()
        .take(modeled_capacity)
        .sum::<f64>();
    let ideal_hit_ratio = closed_form_hit_ratio;
    let irreducible_miss_ratio = 1.0 - ideal_hit_ratio;
    let target_provider_miss_ratio =
        input.target_request_cost_per_million_reads_usd / input.provider_get_cost_per_million_usd;
    let locality_target_gap = irreducible_miss_ratio - target_provider_miss_ratio;
    let provider_request_cost_floor_per_million_reads_usd =
        irreducible_miss_ratio * input.provider_get_cost_per_million_usd;
    let probability_mass_normalized = (probability_mass - 1.0).abs() <= input.probability_tolerance;
    let closed_form_matches_enumeration =
        (closed_form_hit_ratio - enumerated_hit_ratio).abs() <= input.probability_tolerance;
    let capacity_bound_held = modeled_resident_keys <= declared_capacity_keys
        && modeled_resident_bytes <= declared_capacity_bytes;
    let hit_plus_miss_equals_one =
        (ideal_hit_ratio + irreducible_miss_ratio - 1.0).abs() <= input.probability_tolerance;
    let request_cost_target_reproduced =
        (target_provider_miss_ratio - input.expected_provider_miss_ratio).abs()
            <= input.probability_tolerance;
    let receipt_sha256 = receipt_digest(
        input,
        declared_capacity_keys,
        declared_capacity_bytes,
        modeled_resident_keys,
        modeled_resident_bytes,
        probability_mass,
        closed_form_hit_ratio,
        enumerated_hit_ratio,
        target_provider_miss_ratio,
    );

    Ok(LocalityFeasibilityReceipt {
        contract_version: 1,
        model: "provider-bound-locality-feasibility-v0".to_owned(),
        mode: input.mode.id().to_owned(),
        distribution: input.distribution.id().to_owned(),
        key_count: input.key_count,
        logical_bytes: input.logical_bytes,
        point_bytes: input.point_bytes,
        cache_fraction_ppm: input.cache_fraction_ppm,
        declared_capacity_keys,
        declared_capacity_bytes,
        modeled_resident_keys,
        modeled_resident_bytes,
        probability_mass,
        closed_form_hit_ratio,
        enumerated_hit_ratio,
        ideal_hit_ratio,
        irreducible_miss_ratio,
        target_provider_miss_ratio,
        locality_target_gap,
        provider_request_cost_floor_per_million_reads_usd,
        probability_mass_normalized,
        closed_form_matches_enumeration,
        capacity_bound_held,
        hit_plus_miss_equals_one,
        request_cost_target_reproduced,
        receipt_sha256,
    })
}

fn validate_input(input: LocalityFeasibilityInput) -> Result<(), String> {
    if input.key_count == 0 || input.point_bytes == 0 || input.logical_bytes == 0 {
        return Err("locality dimensions must be positive".to_owned());
    }
    if input.logical_bytes != input.key_count.saturating_mul(input.point_bytes) {
        return Err("locality logical bytes must equal key count times point bytes".to_owned());
    }
    if !(1..=1_000_000).contains(&input.cache_fraction_ppm) {
        return Err("locality cache fraction must be in 1..=1000000 ppm".to_owned());
    }
    if !input.provider_get_cost_per_million_usd.is_finite()
        || input.provider_get_cost_per_million_usd <= 0.0
        || !input.target_request_cost_per_million_reads_usd.is_finite()
        || input.target_request_cost_per_million_reads_usd < 0.0
        || !input.expected_provider_miss_ratio.is_finite()
        || !(0.0..=1.0).contains(&input.expected_provider_miss_ratio)
        || !input.probability_tolerance.is_finite()
        || input.probability_tolerance <= 0.0
    {
        return Err("locality cost, target, and tolerance values must be valid".to_owned());
    }
    match input.distribution {
        LocalityDistribution::Uniform => {
            if input.mode == LocalityFeasibilityMode::IgnoreBackgroundReads {
                return Err("ignore-background control requires moving hotset".to_owned());
            }
        }
        LocalityDistribution::Zipfian { theta_milli } => {
            if theta_milli == 0 {
                return Err("locality Zipfian theta must be positive".to_owned());
            }
            if input.mode == LocalityFeasibilityMode::IgnoreBackgroundReads {
                return Err("ignore-background control requires moving hotset".to_owned());
            }
        }
        LocalityDistribution::MovingHotset {
            hotset_fraction_ppm,
            hot_read_fraction_ppm,
        } => {
            if !(1..=1_000_000).contains(&hotset_fraction_ppm) || hot_read_fraction_ppm > 1_000_000
            {
                return Err("locality moving-hotset probabilities are invalid".to_owned());
            }
            if input.mode == LocalityFeasibilityMode::SkipProbabilityNormalization {
                return Err("skip-normalization control requires Zipfian".to_owned());
            }
        }
    }
    Ok(())
}

fn distribution_model(
    distribution: LocalityDistribution,
    mode: LocalityFeasibilityMode,
    key_count: usize,
    capacity_keys: usize,
    capacity_fraction: f64,
) -> Result<(Vec<f64>, f64), String> {
    let key_count_f64 = f64::from(
        u32::try_from(key_count)
            .map_err(|_| "locality distribution key count does not fit u32".to_owned())?,
    );
    match distribution {
        LocalityDistribution::Uniform => {
            Ok((vec![1.0 / key_count_f64; key_count], capacity_fraction))
        }
        LocalityDistribution::Zipfian { theta_milli } => {
            let theta = f64::from(theta_milli) / 1_000.0;
            let raw = (1..=key_count)
                .map(|rank| f64::from(u32::try_from(rank).unwrap_or(u32::MAX)).powf(-theta))
                .collect::<Vec<_>>();
            let normalizer = raw.iter().sum::<f64>();
            let probabilities = if mode == LocalityFeasibilityMode::SkipProbabilityNormalization {
                raw.clone()
            } else {
                raw.iter().map(|weight| weight / normalizer).collect()
            };
            let closed_form_hit_ratio = raw.iter().take(capacity_keys).sum::<f64>() / normalizer;
            Ok((probabilities, closed_form_hit_ratio))
        }
        LocalityDistribution::MovingHotset {
            hotset_fraction_ppm,
            hot_read_fraction_ppm,
        } => {
            let hotset_keys = key_count
                .saturating_mul(hotset_fraction_ppm as usize)
                .div_ceil(PARTS_PER_MILLION_USIZE)
                .clamp(1, key_count);
            let hotset_keys_f64 = f64::from(
                u32::try_from(hotset_keys)
                    .map_err(|_| "locality hotset size does not fit u32".to_owned())?,
            );
            let hot_read_fraction = f64::from(hot_read_fraction_ppm) / 1_000_000.0;
            let background_fraction = 1.0 - hot_read_fraction;
            let mut probabilities = vec![background_fraction / key_count_f64; key_count];
            for probability in probabilities.iter_mut().take(hotset_keys) {
                *probability += hot_read_fraction / hotset_keys_f64;
            }
            let hot_coverage = (f64::from(u32::try_from(capacity_keys).unwrap_or(u32::MAX))
                / hotset_keys_f64)
                .min(1.0);
            let closed_form_hit_ratio = if mode == LocalityFeasibilityMode::IgnoreBackgroundReads {
                hot_coverage
            } else {
                hot_read_fraction * hot_coverage + background_fraction * capacity_fraction
            };
            Ok((probabilities, closed_form_hit_ratio))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn receipt_digest(
    input: LocalityFeasibilityInput,
    declared_capacity_keys: u64,
    declared_capacity_bytes: u64,
    modeled_resident_keys: u64,
    modeled_resident_bytes: u64,
    probability_mass: f64,
    closed_form_hit_ratio: f64,
    enumerated_hit_ratio: f64,
    target_provider_miss_ratio: f64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-provider-locality-feasibility-v0");
    hasher.update(input.key_count.to_be_bytes());
    hasher.update(input.logical_bytes.to_be_bytes());
    hasher.update(input.point_bytes.to_be_bytes());
    hasher.update(input.cache_fraction_ppm.to_be_bytes());
    hasher.update(input.distribution.id().as_bytes());
    match input.distribution {
        LocalityDistribution::Uniform => {}
        LocalityDistribution::Zipfian { theta_milli } => {
            hasher.update(theta_milli.to_be_bytes());
        }
        LocalityDistribution::MovingHotset {
            hotset_fraction_ppm,
            hot_read_fraction_ppm,
        } => {
            hasher.update(hotset_fraction_ppm.to_be_bytes());
            hasher.update(hot_read_fraction_ppm.to_be_bytes());
        }
    }
    hasher.update(input.mode.id().as_bytes());
    hasher.update(declared_capacity_keys.to_be_bytes());
    hasher.update(declared_capacity_bytes.to_be_bytes());
    hasher.update(modeled_resident_keys.to_be_bytes());
    hasher.update(modeled_resident_bytes.to_be_bytes());
    hasher.update(probability_mass.to_bits().to_be_bytes());
    hasher.update(closed_form_hit_ratio.to_bits().to_be_bytes());
    hasher.update(enumerated_hit_ratio.to_bits().to_be_bytes());
    hasher.update(target_provider_miss_ratio.to_bits().to_be_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_locality_feasibility, LocalityDistribution, LocalityFeasibilityInput,
        LocalityFeasibilityMode,
    };

    fn input(distribution: LocalityDistribution) -> LocalityFeasibilityInput {
        LocalityFeasibilityInput {
            key_count: 4_096,
            logical_bytes: 33_554_432,
            point_bytes: 8_192,
            cache_fraction_ppm: 250_000,
            distribution,
            provider_get_cost_per_million_usd: 0.40,
            target_request_cost_per_million_reads_usd: 0.01,
            expected_provider_miss_ratio: 0.025,
            probability_tolerance: 1.0e-12,
            mode: LocalityFeasibilityMode::Correct,
        }
    }

    #[test]
    fn proves_zipfian_target_is_infeasible_at_twenty_five_percent() {
        let receipt = evaluate_locality_feasibility(input(LocalityDistribution::Zipfian {
            theta_milli: 990,
        }))
        .expect("evaluate Zipfian locality");
        assert!((receipt.ideal_hit_ratio - 0.838_299_212_912).abs() < 1.0e-12);
        assert!((receipt.irreducible_miss_ratio - 0.161_700_787_088).abs() < 1.0e-12);
        assert!(receipt.locality_target_gap > 0.13);
        assert!(receipt.probability_mass_normalized);
        assert!(receipt.closed_form_matches_enumeration);
        assert!(receipt.capacity_bound_held);
    }

    #[test]
    fn proves_moving_hotset_background_sets_a_seven_point_five_percent_floor() {
        let receipt = evaluate_locality_feasibility(input(LocalityDistribution::MovingHotset {
            hotset_fraction_ppm: 100_000,
            hot_read_fraction_ppm: 900_000,
        }))
        .expect("evaluate moving-hotset locality");
        assert!((receipt.ideal_hit_ratio - 0.925).abs() < 1.0e-12);
        assert!((receipt.irreducible_miss_ratio - 0.075).abs() < 1.0e-12);
        assert!(receipt.locality_target_gap > 0.049);
        assert!(receipt.closed_form_matches_enumeration);
    }

    #[test]
    fn unsafe_controls_fail_their_independent_gates() {
        let mut inflated = input(LocalityDistribution::Zipfian { theta_milli: 990 });
        inflated.mode = LocalityFeasibilityMode::InflateCapacity;
        assert!(
            !evaluate_locality_feasibility(inflated)
                .expect("evaluate inflated capacity")
                .capacity_bound_held
        );

        let mut unnormalized = input(LocalityDistribution::Zipfian { theta_milli: 990 });
        unnormalized.mode = LocalityFeasibilityMode::SkipProbabilityNormalization;
        let unnormalized =
            evaluate_locality_feasibility(unnormalized).expect("evaluate unnormalized Zipfian");
        assert!(!unnormalized.probability_mass_normalized);
        assert!(!unnormalized.closed_form_matches_enumeration);

        let mut ignored_background = input(LocalityDistribution::MovingHotset {
            hotset_fraction_ppm: 100_000,
            hot_read_fraction_ppm: 900_000,
        });
        ignored_background.mode = LocalityFeasibilityMode::IgnoreBackgroundReads;
        assert!(
            !evaluate_locality_feasibility(ignored_background)
                .expect("evaluate ignored background")
                .closed_form_matches_enumeration
        );
    }

    #[test]
    fn replay_receipt_is_deterministic() {
        let input = input(LocalityDistribution::Uniform);
        let first = evaluate_locality_feasibility(input).expect("first locality receipt");
        let second = evaluate_locality_feasibility(input).expect("second locality receipt");
        assert_eq!(first.receipt_sha256, second.receipt_sha256);
        assert_eq!(first.receipt_sha256.len(), 64);
    }
}
