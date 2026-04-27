//! Server release version parsing and capability checks.
//!
//! Exasol reports its release version as a string in the auth response.
//! These helpers parse that string into a `(major, minor, patch)` tuple
//! and compare it against feature-availability thresholds.

/// Parse an Exasol release version string into a `(major, minor, patch)` tuple.
///
/// Accepts `"MAJOR.MINOR"` (patch defaults to 0) or `"MAJOR.MINOR.PATCH"`.
/// Trailing suffixes on the patch component (e.g. `"-rc1"`, `"+build.5"`,
/// `"_dev"`) are tolerated by taking only the leading digit run.
///
/// Returns `None` if fewer than two numeric components are present, or if
/// any of the major/minor components fail to parse.
pub fn parse_release_version(s: &str) -> Option<(u32, u32, u32)> {
    let mut parts = s.split('.');

    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;

    let patch = match parts.next() {
        Some(raw) => {
            let digits: String = raw.chars().take_while(|c| c.is_ascii_digit()).collect();
            if digits.is_empty() {
                return None;
            }
            digits.parse::<u32>().ok()?
        }
        None => 0,
    };

    Some((major, minor, patch))
}

/// Check whether a parsed `(major, minor, patch)` is at least `(major, minor)`.
///
/// Comparison is lexicographic on the `(major, minor)` pair only; the patch
/// component is intentionally ignored so that callers gate features on
/// minor-version boundaries.
pub fn supports_at_least(parsed: (u32, u32, u32), major: u32, minor: u32) -> bool {
    (parsed.0, parsed.1) >= (major, minor)
}

/// Check whether a parsed `(major, minor, patch)` version meets the threshold
/// for native Parquet IMPORT support, which was introduced in Exasol 2025.1.11.
///
/// Comparison is lexicographic across all three components, matching Rust's
/// default tuple ordering. So `(2025, 1, 10)` is rejected, while
/// `(2025, 1, 11)`, `(2025, 2, 0)`, and `(2026, 0, 0)` are accepted.
pub fn supports_native_parquet_import(v: (u32, u32, u32)) -> bool {
    v >= (2025, 1, 11)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_two_and_three_component_versions() {
        assert_eq!(parse_release_version("7.1"), Some((7, 1, 0)));
        assert_eq!(parse_release_version("2025.1.11"), Some((2025, 1, 11)));
        assert_eq!(parse_release_version("8.31.0"), Some((8, 31, 0)));
    }

    #[test]
    fn test_parse_rejects_garbage_strings() {
        assert_eq!(parse_release_version(""), None);
        assert_eq!(parse_release_version("foo.bar"), None);
        assert_eq!(parse_release_version("2025"), None);
    }

    #[test]
    fn test_parse_trailing_suffix_tolerated() {
        assert_eq!(parse_release_version("2025.1.11-rc1"), Some((2025, 1, 11)));
        assert_eq!(parse_release_version("8.31.0+build"), Some((8, 31, 0)));
    }

    #[test]
    fn test_supports_native_parquet_import_threshold_boundaries() {
        assert!(!supports_native_parquet_import((2025, 1, 10)));
        assert!(supports_native_parquet_import((2025, 1, 11)));
        assert!(supports_native_parquet_import((2025, 2, 0)));
        assert!(supports_native_parquet_import((2026, 0, 0)));
        assert!(!supports_native_parquet_import((7, 1, 0)));
        assert!(!supports_native_parquet_import((8, 31, 0)));
    }
}
