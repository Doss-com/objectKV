use super::bounded_count;

// Mirror the eight frozen values in docs/reviews/count-conversion-characterization-20261010/oracle.json.
// These tests call the function compiled into the actual okv-eval binary crate.
macro_rules! count_fixture {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let expected: f64 = $expected;
            assert_eq!(
                bounded_count($input).to_bits(),
                expected.to_bits(),
                "input={}; reporting should preserve this exactly representable integer",
                $input
            );
        }
    };
}

count_fixture!(zero, 0_u64, 0.0);
count_fixture!(one, 1_u64, 1.0);
count_fixture!(u32_max, 4_294_967_295_u64, 4_294_967_295.0);
count_fixture!(four_gib, 4_294_967_296_u64, 4_294_967_296.0);
count_fixture!(four_gib_plus_one, 4_294_967_297_u64, 4_294_967_297.0);
count_fixture!(ten_gib, 10_737_418_240_u64, 10_737_418_240.0);
count_fixture!(
    f64_exact_integer_limit_minus_one,
    9_007_199_254_740_991_u64,
    9_007_199_254_740_991.0
);
count_fixture!(
    f64_exact_integer_limit,
    9_007_199_254_740_992_u64,
    9_007_199_254_740_992.0
);
