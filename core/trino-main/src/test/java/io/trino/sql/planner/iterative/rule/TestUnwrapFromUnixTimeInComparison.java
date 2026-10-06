/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.type.DateTimes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.iterative.rule.UnwrapFromUnixTimeInComparison.adjustDoubleThresholdGE;
import static io.trino.sql.planner.iterative.rule.UnwrapFromUnixTimeInComparison.adjustIntThresholdGE;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.iterative.rule.UnwrapFromUnixTimeInComparison.unwrapFromUnixTime;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnwrapFromUnixTimeInComparison
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction FROM_UNIXTIME = FUNCTIONS.resolveFunction("from_unixtime", fromTypes(DOUBLE));
    private static final ResolvedFunction FROM_UNIXTIME_ZONE = FUNCTIONS.resolveFunction("from_unixtime", fromTypes(DOUBLE, createVarcharType(13)));
    private static final ResolvedFunction FROM_UNIXTIME_OFFSET = FUNCTIONS.resolveFunction("from_unixtime", fromTypes(DOUBLE, BIGINT, BIGINT));
    private static final ResolvedFunction FROM_UNIXTIME_NANOS = FUNCTIONS.resolveFunction("from_unixtime_nanos", fromTypes(BIGINT));
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction DIVIDE_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction IS_NAN = FUNCTIONS.resolveFunction("is_nan", fromTypes(DOUBLE));

    private static final Reference N = new Reference(BIGINT, "n");
    private static final Reference X = new Reference(DOUBLE, "x");

    // epoch second of 2026-09-01 00:00:00 in Asia/Shanghai
    private static final long METABASE_BOUNDARY_K = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond();

    @Test
    public void testRangeOperatorsOnIntegerDivisionChain()
    {
        // k = 1: from_unixtime(n / 1000) vs T(1s)
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, chain(), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
        assertThat(unwrap(comparison(LESS_THAN, chain(), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, 1000L)));
        // <= T(1s): q <= 1 → n < L(2) = 2000
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, chain(), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, 2000L)));
        // > T(1s): q >= 2 → n >= 2000
        assertThat(unwrap(comparison(GREATER_THAN, chain(), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 2000L)));

        // k = 0 and k = -1: truncation-toward-zero boundaries
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, chain(), tzConstant(3, 0, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, -999L)));
        assertThat(unwrap(comparison(LESS_THAN, chain(), tzConstant(3, -1000, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, -1999L)));
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, chain(), tzConstant(3, -1000, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, -999L)));

        // realistic Metabase boundary
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, chain(), tzConstant(3, METABASE_BOUNDARY_K * 1000, "Asia/Shanghai"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, METABASE_BOUNDARY_K * 1000)));

        // chain on the right: T <= chain, T > chain
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, tzConstant(3, 1000, "UTC"), chain())))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
        assertThat(unwrap(comparison(GREATER_THAN, tzConstant(3, 1000, "UTC"), chain())))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, 1000L)));
    }

    @Test
    public void testEqualityOnIntegerDivisionChain()
    {
        // = T(1s): n in [1000, 2000)
        assertThat(unwrap(comparison(EQUAL, chain(), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)),
                        comparison(LESS_THAN, N, new Constant(BIGINT, 2000L)))));
        // <>: canonical form is $not($equal(chain, T)) — the inner equality is rewritten, the NOT stays
        assertThat(unwrap(comparison(NOT_EQUAL, chain(), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(not(FUNCTIONS.getMetadata(), getCharVarcharCoercion(TEST_SESSION), new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)),
                        comparison(LESS_THAN, N, new Constant(BIGINT, 2000L))))));
        // constant on the left: T = chain flips to the same equality inversion
        assertThat(unwrap(comparison(EQUAL, tzConstant(3, 1000, "UTC"), chain())))
                .isEqualTo(new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)),
                        comparison(LESS_THAN, N, new Constant(BIGINT, 2000L)))));
        // = with a fractional constant can never hold: NULL-safe empty range
        assertThat(unwrap(comparison(EQUAL, chain(), tzConstant(3, 1500, "UTC"))))
                .isEqualTo(new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 0L)),
                        comparison(LESS_THAN, N, new Constant(BIGINT, 0L)))));
    }

    @Test
    public void testFractionalConstantOnIntegerDivisionChain()
    {
        // >= T(1.5s): q >= ceil(1.5) = 2 → n >= 2000
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, chain(), tzConstant(3, 1500, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 2000L)));
        // < T(1.5s): q < 2 → n < 2000
        assertThat(unwrap(comparison(LESS_THAN, chain(), tzConstant(3, 1500, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, 2000L)));
        // <= T(1.5s): q <= 1 → n < 2000
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, chain(), tzConstant(3, 1500, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, 2000L)));
        // > T(1.5s): q >= 2 → n >= 2000
        assertThat(unwrap(comparison(GREATER_THAN, chain(), tzConstant(3, 1500, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 2000L)));
    }

    @Test
    public void testOtherDivisorsAndNoDivision()
    {
        // epoch seconds column, no division: from_unixtime(CAST(n AS double)) >= T(1s) → n >= 1
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(N, DOUBLE))),
                tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1L)));
        // other integer divisor: n / 60 >= T(1s) → q >= 1 → n >= 60
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(division(N, 60), DOUBLE))),
                tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 60L)));
        // negative k with other divisor: n / 60 >= T(-1s) → q >= -1 → n >= -60 - 60 + 1 = -119
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(division(N, 60), DOUBLE))),
                tzConstant(3, -1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, -119L)));
    }

    @Test
    public void testDoubleChains()
    {
        // double division by 1000.0 over a bigint column: behavioral equivalence across the boundary
        Expression doubleDiv = new Call(FROM_UNIXTIME, ImmutableList.of(
                new Call(DIVIDE_DOUBLE, ImmutableList.of(new Cast(N, DOUBLE), new Constant(DOUBLE, 1000.0)))));
        Expression original = comparison(GREATER_THAN_OR_EQUAL, doubleDiv, tzConstant(3, 1000, "UTC"));
        Expression rewritten = unwrap(original);
        assertThat(rewritten).isNotEqualTo(original);
        for (long n : new long[] {998, 999, 1000, 1001}) {
            assertThat(evaluate(rewritten, n)).as("n=%s", n).isEqualTo(evaluate(original, n));
        }

        // double column: sweep the half-millis boundary including adjacent doubles
        Expression doubleCol = widenedDoubleChain();
        Constant t62 = new Constant(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(62, 0, getTimeZoneKey("UTC")));
        for (ComparisonOperator op : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN)) {
            Expression orig = comparison(op, doubleCol, t62);
            Expression rw = unwrap(orig);
            assertThat(rw).isNotEqualTo(orig);
            double pivot = 0.0615;
            double[] xs = {Math.nextDown(Math.nextDown(pivot)), Math.nextDown(pivot), pivot, Math.nextUp(pivot), Math.nextUp(Math.nextUp(pivot))};
            for (double x : xs) {
                assertThat(evaluateDouble(rw, x)).as("op=%s x=%s", op, x).isEqualTo(evaluateDouble(orig, x));
            }
        }
    }

    @Test
    public void testZoneVariants()
    {
        // zone argument does not affect instant comparison
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, chain("Asia/Shanghai"), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
        assertThat(unwrap(comparison(LESS_THAN, chain("+08:00"), tzConstant(3, 0, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, -999L)));
        // three-argument form with constant offset
        Expression offsetChain = new Call(FROM_UNIXTIME_OFFSET, ImmutableList.of(
                new Cast(division(N, 1000), DOUBLE), new Constant(BIGINT, 8L), new Constant(BIGINT, 0L)));
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, offsetChain, tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
    }

    @Test
    public void testFromUnixtimeNanos()
    {
        // identity in epoch nanos: T = 1s + 0.5ms → 1_000_500_000 nanos
        Constant t = new Constant(TIMESTAMP_TZ_NANOS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000, 500_000_000, getTimeZoneKey("UTC")));
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, nanosChain(), t)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1_000_500_000L)));
        assertThat(unwrap(comparison(EQUAL, nanosChain(), t)))
                .isEqualTo(comparison(EQUAL, N, new Constant(BIGINT, 1_000_500_000L)));

        // sub-nanosecond constant: a legal value needs p12 (1ps is below p9 resolution), and the p9 chain
        // is widened to p12 by the comparison's type coercion
        Constant t12 = new Constant(createTimestampWithTimeZoneType(12),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000, 500_000_001, getTimeZoneKey("UTC")));
        Expression widenedNanos = new Cast(nanosChain(), createTimestampWithTimeZoneType(12));
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, widenedNanos, t12)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1_000_500_001L)));
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, widenedNanos, t12)))
                .isEqualTo(comparison(LESS_THAN_OR_EQUAL, N, new Constant(BIGINT, 1_000_500_000L)));
    }

    @Test
    public void testInPredicate()
    {
        Expression in = new In(chain(), ImmutableList.of(tzConstant(3, 1000, "UTC"), tzConstant(3, 3000, "UTC")));
        assertThat(unwrap(in))
                .isEqualTo(new Logical(OR, ImmutableList.of(
                        new Logical(AND, ImmutableList.of(
                                comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)),
                                comparison(LESS_THAN, N, new Constant(BIGINT, 2000L)))),
                        new Logical(AND, ImmutableList.of(
                                comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 3000L)),
                                comparison(LESS_THAN, N, new Constant(BIGINT, 4000L)))))));

        // one non-constant element blocks the whole rewrite
        Expression mixed = new In(chain(), ImmutableList.of(tzConstant(3, 1000, "UTC"), new Reference(TIMESTAMP_TZ_MILLIS, "other")));
        assertThat(unwrap(mixed)).isEqualTo(mixed);

        // behavioral equivalence of the rewritten IN
        for (long n : new long[] {999, 1000, 1500, 1999, 2000, 2999, 3000, 3500, 3999, 4000}) {
            assertThat(evaluate(unwrap(in), n)).as("n=%s", n).isEqualTo(evaluate(in, n));
        }
    }

    @Test
    public void testLetWithNonInvertibleConjunctStaysUnchanged()
    {
        // BETWEEN over a non-trivial chain desugars to Let(symbol, chain, min <= symbol AND symbol <= max);
        // a conjunct that does not compare the bound symbol against a constant must block the whole rewrite
        Symbol bound = new Symbol(TIMESTAMP_TZ_MILLIS, "between");
        Reference boundRef = new Reference(TIMESTAMP_TZ_MILLIS, "between");
        Expression let = new Let(bound, chain(), new Logical(AND, ImmutableList.of(
                comparison(LESS_THAN_OR_EQUAL, tzConstant(3, 1000, "UTC"), boundRef),
                comparison(LESS_THAN_OR_EQUAL, boundRef, new Reference(TIMESTAMP_TZ_MILLIS, "other")))));
        assertUnchanged(let);
    }

    @Test
    public void testSessionZoneInterpretationOfConstant()
    {
        Expression constant = new Cast(new Constant(TIMESTAMP_MICROS, DateTimes.parseTimestamp(6, "2026-09-01 00:00:00.000000")), TIMESTAMP_TZ_MICROS);

        Session shanghai = testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Asia/Shanghai")).build();
        long expectedShanghai = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        assertThat(unwrap(shanghai, comparison(GREATER_THAN_OR_EQUAL, widenedChain(), constant)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, expectedShanghai)));

        Session utc = testSessionBuilder().setTimeZoneKey(getTimeZoneKey("UTC")).build();
        long expectedUtc = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("UTC")).toEpochSecond() * 1000;
        assertThat(unwrap(utc, comparison(GREATER_THAN_OR_EQUAL, widenedChain(), constant)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, expectedUtc)));

        assertThat(expectedShanghai).isNotEqualTo(expectedUtc);
    }

    @Test
    public void testPrecisionsAndWideningCasts()
    {
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, widenedChain(), tzConstant(6, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
        assertThat(unwrap(comparison(LESS_THAN, widenedTo(TIMESTAMP_TZ_NANOS), tzConstant(9, 0, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, -999L)));
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, widenedTo(createTimestampWithTimeZoneType(12)), tzConstant(12, -1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, -1999L)));
        // fractional constant in long representation with 1ps beyond the milli: legal only at p12
        Constant t12 = new Constant(createTimestampWithTimeZoneType(12),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1500, 1, getTimeZoneKey("UTC")));
        // m = 1500ms + 1ps → q >= ceil(m/1000) = 2 → n >= 2000
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, widenedTo(createTimestampWithTimeZoneType(12)), t12)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 2000L)));
    }

    @Test
    public void testFractionalMillisConstantSnappedToGrid()
    {
        // from_unixtime output is integer millis; T = 0.5ms must snap to 1ms before inversion
        // (correctness review §3 counterexample: x = 0.0002 must be excluded by >=)
        Constant t = new Constant(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 500_000_000, getTimeZoneKey("UTC")));
        Expression doubleChain = widenedDoubleChain();
        for (ComparisonOperator op : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, EQUAL)) {
            Expression original = comparison(op, doubleChain, t);
            Expression rewritten = unwrap(original);
            assertThat(rewritten).isNotEqualTo(original);
            for (double x : new double[] {0.0002, 0.0004, 0.0005, 0.0006, 0.0014, 0.0015}) {
                assertThat(evaluateDouble(rewritten, x))
                        .as("op=%s x=%s", op, x)
                        .isEqualTo(evaluateDouble(original, x));
            }
        }
    }

    @Test
    public void testSpecialDoubleInputs()
    {
        Expression doubleChain = widenedDoubleChain();
        Constant epoch0 = new Constant(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, getTimeZoneKey("UTC")));

        // NULL and ±0: equivalence holds
        Expression original = comparison(EQUAL, doubleChain, epoch0);
        Expression rewritten = unwrap(original);
        assertThat(evaluateNullDouble(rewritten)).isNull();
        assertThat(evaluateNullDouble(original)).isNull();
        for (double x : new double[] {0.0, -0.0}) {
            assertThat(evaluateDouble(rewritten, x)).as("x=%s", x).isEqualTo(evaluateDouble(original, x));
        }

        // NaN: exact — the original maps NaN to epoch 0 via Math.round, and the rewrite adds an
        // explicit is_nan disjunct when millis 0 satisfies the comparison (= epoch 0 is such a case)
        assertThat(evaluateDouble(original, Double.NaN)).isEqualTo(true);
        assertThat(evaluateDouble(rewritten, Double.NaN)).isEqualTo(true);

        // +Infinity: the original throws at evaluation or stays unevaluated until runtime (the engine
        // defers failing calls); the rewritten comparison keeps the row. Either original behavior is
        // the pre-patch semantics; the divergence is the documented accepted deviation
        Expression geqOriginal = comparison(GREATER_THAN_OR_EQUAL, doubleChain, epoch0);
        Expression geqRewritten = unwrap(geqOriginal);
        assertThat(evaluateDouble(geqRewritten, Double.POSITIVE_INFINITY)).isEqualTo(true);
        assertThat(evaluateDoublePreservingError(geqOriginal, Double.POSITIVE_INFINITY)).isIn("INVALID_FUNCTION_ARGUMENT", "DEFERRED_TO_RUNTIME");
    }

    @Test
    public void testNaNCoversAllComparisonOperatorsAndConstants()
    {
        // every nanSatisfies branch: epoch 0 satisfies a comparison iff (>=, k<=0), (<, k>0),
        // (=, k=0), (<>, k<>0); NaN must behave identically in the original and the rewrite
        Expression doubleChain = widenedDoubleChain();
        Constant[] constants = {
                new Constant(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1000, 0, getTimeZoneKey("UTC"))),
                new Constant(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, getTimeZoneKey("UTC"))),
                new Constant(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000, 0, getTimeZoneKey("UTC"))),
                // sub-millis constant: snaps to 1ms on the integer-millis grid, which still covers epoch 0 for <
                new Constant(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 500_000_000, getTimeZoneKey("UTC"))),
        };
        for (Constant constant : constants) {
            for (ComparisonOperator op : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, EQUAL, NOT_EQUAL)) {
                Expression original = comparison(op, doubleChain, constant);
                Expression rewritten = unwrap(original);
                assertThat(rewritten).isNotEqualTo(original);
                assertThat(evaluateDouble(rewritten, Double.NaN))
                        .as("op=%s constant=%s", op, constant)
                        .isEqualTo(evaluateDouble(original, Double.NaN));
            }
        }
    }

    @Test
    public void testDoubleChainThresholdMatrix()
    {
        // negative half-millis boundary on a double leaf, all six operators
        Constant tm62 = new Constant(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(-62, 0, getTimeZoneKey("UTC")));
        Expression doubleChain = widenedDoubleChain();
        for (ComparisonOperator op : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, EQUAL, NOT_EQUAL)) {
            Expression original = comparison(op, doubleChain, tm62);
            Expression rewritten = unwrap(original);
            assertThat(rewritten).isNotEqualTo(original);
            for (double x : new double[] {-0.0614, -0.0615, -0.0624, -0.0625, -0.0634, -0.0635}) {
                assertThat(evaluateDouble(rewritten, x)).as("op=%s x=%s", op, x).isEqualTo(evaluateDouble(original, x));
            }
        }

        // non-binary-exact double divisors over a double column, both threshold sides, all six operators
        Constant t62 = new Constant(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(62, 0, getTimeZoneKey("UTC")));
        for (double d : new double[] {3.0, 0.1}) {
            Expression divChain = new Cast(new Call(FROM_UNIXTIME, ImmutableList.of(
                    new Call(DIVIDE_DOUBLE, ImmutableList.of(X, new Constant(DOUBLE, d))))), TIMESTAMP_TZ_MICROS);
            for (ComparisonOperator op : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, EQUAL, NOT_EQUAL)) {
                Expression original = comparison(op, divChain, t62);
                Expression rewritten = unwrap(original);
                assertThat(rewritten).isNotEqualTo(original);
                for (double pivot : new double[] {d * 0.0615, d * 0.0625}) {
                    double[] xs = {Math.nextDown(Math.nextDown(pivot)), Math.nextDown(pivot), pivot, Math.nextUp(pivot), Math.nextUp(Math.nextUp(pivot))};
                    for (double x : xs) {
                        assertThat(evaluateDouble(rewritten, x)).as("d=%s op=%s x=%s", d, op, x).isEqualTo(evaluateDouble(original, x));
                    }
                }
            }
        }
    }

    @Test
    public void testThresholdAdjustmentNonConvergence()
    {
        // an eval function that never reaches k: the bounded adjustment must fall back to empty
        assertThat(adjustIntThresholdGE(n -> 0, new BigDecimal("1000"), 5))
                .isEmpty();
        assertThat(adjustDoubleThresholdGE(x -> 0, new BigDecimal("0.5"), 5))
                .isEmpty();
        // an eval function that always satisfies: walking down never converges either
        assertThat(adjustIntThresholdGE(n -> Long.MAX_VALUE, new BigDecimal("1000"), 5))
                .isEmpty();
        assertThat(adjustDoubleThresholdGE(x -> Long.MAX_VALUE, new BigDecimal("0.5"), 5))
                .isEmpty();
        // walking up from near Long.MAX_VALUE must stop at the overflow guard, not wrap around
        assertThat(adjustIntThresholdGE(n -> 0, new BigDecimal(Long.MAX_VALUE), 5))
                .isEmpty();
        assertThat(adjustIntThresholdGE(n -> n == Long.MAX_VALUE ? 5 : 0, new BigDecimal(Long.MAX_VALUE), 5))
                .hasValue(Long.MAX_VALUE);
    }

    @Test
    public void testThresholdAdjustmentFallback()
    {
        // year-9999 constant: with a huge divisor the analytic threshold overflows bigint/double,
        // and the rule must fall back to no rewrite
        Constant year9999 = tzConstant(3, 253402300799000L, "UTC");
        Expression hugeDivisorIntLeaf = new Call(FROM_UNIXTIME, ImmutableList.of(
                new Call(DIVIDE_DOUBLE, ImmutableList.of(new Cast(N, DOUBLE), new Constant(DOUBLE, 1e300)))));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, hugeDivisorIntLeaf, year9999));

        Expression hugeDivisorDoubleLeaf = new Call(FROM_UNIXTIME, ImmutableList.of(
                new Call(DIVIDE_DOUBLE, ImmutableList.of(X, new Constant(DOUBLE, 1e300)))));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, hugeDivisorDoubleLeaf, year9999));
    }

    @Test
    public void testDoubleLeafExclusiveUpperBound()
    {
        // x = 0.0625s is binary-exact: x*1000 = 62.5 rounds half-up to 63, so target 62ms must exclude x
        // (correctness review §4 counterexample)
        Constant t62 = new Constant(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(62, 0, getTimeZoneKey("UTC")));
        Expression doubleChain = widenedDoubleChain();
        for (ComparisonOperator op : List.of(EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL)) {
            Expression original = comparison(op, doubleChain, t62);
            Expression rewritten = unwrap(original);
            assertThat(rewritten).isNotEqualTo(original);
            for (double x : new double[] {0.0614, 0.0615, 0.0624, 0.0625, 0.0634, 0.0635}) {
                assertThat(evaluateDouble(rewritten, x))
                        .as("op=%s x=%s", op, x)
                        .isEqualTo(evaluateDouble(original, x));
            }
        }
    }

    @Test
    public void testDoubleDivisionIntLeafBoundarySweep()
    {
        // /1000.0 double division over a bigint column: sweep boundaries across positive/negative/realistic epochs
        Expression chain = new Call(FROM_UNIXTIME, ImmutableList.of(
                new Call(DIVIDE_DOUBLE, ImmutableList.of(new Cast(N, DOUBLE), new Constant(DOUBLE, 1000.0)))));
        for (long k : new long[] {-2, -1, 0, 1, 2, 1788192000L}) {
            for (ComparisonOperator op : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, EQUAL, NOT_EQUAL)) {
                Expression original = comparison(op, chain, tzConstant(3, k * 1000, "UTC"));
                Expression rewritten = unwrap(original);
                assertThat(rewritten).isNotEqualTo(original);
                for (long n : new long[] {k * 1000 - 1, k * 1000, k * 1000 + 1, k * 1000 + 999, k * 1000 + 1000}) {
                    assertThat(evaluate(rewritten, n))
                            .as("op=%s k=%s n=%s", op, k, n)
                            .isEqualTo(evaluate(original, n));
                }
            }
        }
    }

    @Test
    public void testNoRewriteForNonFiniteDivisor()
    {
        // Double.POSITIVE_INFINITY passes a naive value > 0 check but breaks new BigDecimal(value)
        Expression infiniteDivisor = new Call(FROM_UNIXTIME, ImmutableList.of(
                new Call(DIVIDE_DOUBLE, ImmutableList.of(new Cast(N, DOUBLE), new Constant(DOUBLE, Double.POSITIVE_INFINITY)))));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, infiniteDivisor, tzConstant(3, 1000, "UTC")));
        Expression nanDivisor = new Call(FROM_UNIXTIME, ImmutableList.of(
                new Call(DIVIDE_DOUBLE, ImmutableList.of(new Cast(N, DOUBLE), new Constant(DOUBLE, Double.NaN)))));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, nanDivisor, tzConstant(3, 1000, "UTC")));
    }

    @Test
    public void testNoRewrite()
    {
        // invalid zone: the original error must be preserved
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, chain("Invalid/Zone"), tzConstant(3, 1000, "UTC")));
        // NULL zone: the original NULL semantics must be preserved
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, chain(null), tzConstant(3, 1000, "UTC")));
        // non-constant zone
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME_ZONE, ImmutableList.of(new Cast(division(N, 1000), DOUBLE), new Reference(createVarcharType(13), "zone"))),
                tzConstant(3, 1000, "UTC")));
        // outer cast to timestamp without time zone
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, new Cast(chain(), TIMESTAMP_MICROS), new Constant(TIMESTAMP_MICROS, DateTimes.parseTimestamp(6, "1970-01-01 00:00:01.000000"))));
        // precision-lowering cast on the function side
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, new Cast(chain(), TIMESTAMP_TZ_SECONDS), tzConstant(0, 1000, "UTC")));
        // multiplication instead of division
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(new Call(MULTIPLY_BIGINT, ImmutableList.of(N, new Constant(BIGINT, 1000L))), DOUBLE))),
                tzConstant(3, 1000, "UTC")));
        // division whose dividend is not a plain column reference
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(division(new Call(MULTIPLY_BIGINT, ImmutableList.of(N, new Constant(BIGINT, 2L))), 1000), DOUBLE))),
                tzConstant(3, 1000, "UTC")));
        // non-constant or NULL constant side
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, chain(), new Reference(TIMESTAMP_TZ_MILLIS, "other")));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, chain(), new Constant(TIMESTAMP_TZ_MILLIS, null)));
        // negative or zero divisor
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(division(N, -1000), DOUBLE))),
                tzConstant(3, 1000, "UTC")));
        // non-constant divisor
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(new Call(DIVIDE_BIGINT, ImmutableList.of(N, new Reference(BIGINT, "d"))), DOUBLE))),
                tzConstant(3, 1000, "UTC")));
        // IS NOT DISTINCT FROM: out of scope
        assertUnchanged(comparison(IDENTICAL, chain(), tzConstant(3, 1000, "UTC")));
        // from_unixtime_nanos with a non-bigint argument
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME_NANOS, ImmutableList.of(new Cast(X, BIGINT))),
                tzConstant(9, 1000, "UTC")));
    }

    // Negative control group: for every out-of-scope shape the rule must not only stay structurally
    // unchanged, the runtime behavior (values, NULLs, errors) must stay identical to the original semantics.
    @Test
    public void testNegativeControlBehavior()
    {
        // invalid zone: untouched by the rule, and the original INVALID_FUNCTION_ARGUMENT defers to runtime
        Expression invalidZone = comparison(GREATER_THAN_OR_EQUAL, chain("Invalid/Zone"), tzConstant(3, 1000, "UTC"));
        assertUnchanged(invalidZone);
        assertThat(evaluatePreservingError(invalidZone, 1000L)).isIn("INVALID_FUNCTION_ARGUMENT", "DEFERRED_TO_RUNTIME");

        // division by zero: untouched by the rule, and the original DIVISION_BY_ZERO still fires
        Expression divZero = comparison(GREATER_THAN_OR_EQUAL,
                new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(division(N, 0), DOUBLE))),
                tzConstant(3, 1000, "UTC"));
        assertUnchanged(divZero);
        assertThat(evaluatePreservingError(divZero, 1000L)).isIn("DIVISION_BY_ZERO", "DEFERRED_TO_RUNTIME");

        // NULL zone: original evaluates to NULL, rule must not touch it
        Expression nullZone = comparison(GREATER_THAN_OR_EQUAL, chain(null), tzConstant(3, 1000, "UTC"));
        assertUnchanged(nullZone);
        assertThat(evaluate(nullZone, 1000L)).isNull();

        // NULL constant side: untouched, evaluates to NULL
        Expression nullConstant = comparison(GREATER_THAN_OR_EQUAL, chain(), new Constant(TIMESTAMP_TZ_MILLIS, null));
        assertUnchanged(nullConstant);
        assertThat(evaluate(nullConstant, 1000L)).isNull();

        // IN with a NULL element: untouched, NULL element makes a non-matching row evaluate to NULL
        Expression inWithNull = new In(chain(), ImmutableList.of(tzConstant(3, 2000, "UTC"), new Constant(TIMESTAMP_TZ_MILLIS, null)));
        assertThat(unwrap(inWithNull)).isEqualTo(inWithNull);
        assertThat(evaluate(inWithNull, 1000L)).isNull();
        assertThat(evaluate(inWithNull, 2000L)).isEqualTo(true);

        // equality against a fractional constant rewrites to an empty range: false for non-NULL, NULL for NULL
        Expression unsatisfiable = comparison(EQUAL, chain(), tzConstant(3, 1500, "UTC"));
        Expression rewrittenUnsatisfiable = unwrap(unsatisfiable);
        assertThat(rewrittenUnsatisfiable).isNotEqualTo(unsatisfiable);
        assertThat(evaluate(rewrittenUnsatisfiable, 1500L)).isEqualTo(false);
        assertThat(evaluate(rewrittenUnsatisfiable, null)).isNull();

        // its negation rewrites to a whole-domain disjunction: true for non-NULL, NULL for NULL
        Expression alwaysTrue = comparison(NOT_EQUAL, chain(), tzConstant(3, 1500, "UTC"));
        Expression rewrittenAlwaysTrue = unwrap(alwaysTrue);
        assertThat(rewrittenAlwaysTrue).isNotEqualTo(alwaysTrue);
        assertThat(evaluate(rewrittenAlwaysTrue, 1500L)).isEqualTo(true);
        assertThat(evaluate(rewrittenAlwaysTrue, null)).isNull();
    }

    // evaluates with the column bound to n; returns the Trino error code if planning-time evaluation
    // fails, or DEFERRED_TO_RUNTIME when the optimizer deliberately keeps the failing call unevaluated
    // (the original runtime error then still fires per row)
    private static String evaluatePreservingError(Expression expression, Long n)
    {
        try {
            Expression result = FUNCTIONS.getPlannerContext().getExpressionOptimizer()
                    .process(expression, TEST_SESSION, emptySymbolAllocator(), ImmutableMap.of(new Symbol(BIGINT, "n"), new Constant(BIGINT, n)))
                    .orElse(expression);
            if (result instanceof Constant constant) {
                throw new AssertionError("expected failure but evaluated to " + constant.value());
            }
            return "DEFERRED_TO_RUNTIME";
        }
        catch (io.trino.spi.TrinoException e) {
            return e.getErrorCode().getName();
        }
    }

    private static String evaluateDoublePreservingError(Expression expression, double x)
    {
        try {
            Expression result = FUNCTIONS.getPlannerContext().getExpressionOptimizer()
                    .process(expression, TEST_SESSION, emptySymbolAllocator(), ImmutableMap.of(new Symbol(DOUBLE, "x"), new Constant(DOUBLE, x)))
                    .orElse(expression);
            if (result instanceof Constant constant) {
                throw new AssertionError("expected failure but evaluated to " + constant.value());
            }
            return "DEFERRED_TO_RUNTIME";
        }
        catch (io.trino.spi.TrinoException e) {
            return e.getErrorCode().getName();
        }
    }

    @Test
    public void testResultEquivalence()
    {
        assertEquivalent(GREATER_THAN_OR_EQUAL, 1, new Long[] {999L, 1000L, 1001L, 500L, 1500L, -500L, null});
        assertEquivalent(GREATER_THAN_OR_EQUAL, 0, new Long[] {-1000L, -999L, -998L, -1L, 0L, 500L, 999L, null});
        assertEquivalent(GREATER_THAN_OR_EQUAL, -1, new Long[] {-2000L, -1999L, -1998L, -1500L, -1000L, 0L, null});
        assertEquivalent(LESS_THAN, 1, new Long[] {999L, 1000L, 1001L, 500L, 1500L, null});
        assertEquivalent(LESS_THAN, 0, new Long[] {-1000L, -999L, -998L, 0L, 999L, null});
        assertEquivalent(LESS_THAN, -1, new Long[] {-2000L, -1999L, -1998L, -1000L, null});
        assertEquivalent(LESS_THAN_OR_EQUAL, 1, new Long[] {999L, 1000L, 1001L, 1999L, 2000L, null});
        assertEquivalent(GREATER_THAN, 1, new Long[] {999L, 1000L, 1999L, 2000L, 2001L, null});
        assertEquivalent(EQUAL, 1, new Long[] {999L, 1000L, 1001L, 1999L, 2000L, null});
        assertEquivalent(NOT_EQUAL, 1, new Long[] {999L, 1000L, 1500L, 1999L, 2000L, null});
        assertEquivalent(EQUAL, 0, new Long[] {-1000L, -999L, -1L, 0L, 999L, null});
    }

    private static void assertEquivalent(ComparisonOperator operator, long k, Long[] values)
    {
        Expression original = comparison(operator, chain("Asia/Shanghai"), tzConstant(3, k * 1000, "UTC"));
        Expression rewritten = unwrap(original);
        assertThat(rewritten).isNotEqualTo(original);

        for (Long value : values) {
            Boolean originalResult = evaluate(original, value);
            Boolean rewrittenResult = evaluate(rewritten, value);
            assertThat(rewrittenResult)
                    .as("equivalence for operator %s, k=%s, n=%s", operator, k, value)
                    .isEqualTo(originalResult);
        }
    }

    private static Boolean evaluate(Expression expression, Long n)
    {
        Expression result = FUNCTIONS.getPlannerContext().getExpressionOptimizer()
                .process(expression, TEST_SESSION, emptySymbolAllocator(), ImmutableMap.of(new Symbol(BIGINT, "n"), new Constant(BIGINT, n)))
                .orElse(expression);
        assertThat(result).isInstanceOf(Constant.class);
        return (Boolean) ((Constant) result).value();
    }

    private static Boolean evaluateDouble(Expression expression, double x)
    {
        Expression result = FUNCTIONS.getPlannerContext().getExpressionOptimizer()
                .process(expression, TEST_SESSION, emptySymbolAllocator(), ImmutableMap.of(new Symbol(DOUBLE, "x"), new Constant(DOUBLE, x)))
                .orElse(expression);
        assertThat(result).isInstanceOf(Constant.class);
        return (Boolean) ((Constant) result).value();
    }

    private static Boolean evaluateNullDouble(Expression expression)
    {
        Expression result = FUNCTIONS.getPlannerContext().getExpressionOptimizer()
                .process(expression, TEST_SESSION, emptySymbolAllocator(), ImmutableMap.of(new Symbol(DOUBLE, "x"), new Constant(DOUBLE, null)))
                .orElse(expression);
        assertThat(result).isInstanceOf(Constant.class);
        return (Boolean) ((Constant) result).value();
    }

    // CAST(from_unixtime(x) AS timestamp(6) with time zone), x a double column
    private static Expression widenedDoubleChain()
    {
        return new Cast(new Call(FROM_UNIXTIME, ImmutableList.of(X)), TIMESTAMP_TZ_MICROS);
    }

    @Test
    public void testNanDisjunctOnlyWhenEpochZeroSatisfies()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setTimeZoneKey(getTimeZoneKey("UTC"))
                .build();
        // threshold for k = 2026-01-01T00:00Z (epoch millis 1767225600000): (k - 0.5) / 1000
        Constant bound = new Constant(DOUBLE, 1767225599.9995);

        // upper bound in the future: epoch 0 satisfies, so the double leaf keeps NaN rows explicitly.
        // the range part no longer forms a column domain for file pruning — the accepted exactness tradeoff
        assertPlan(
                """
                SELECT count(*) FROM orders
                WHERE from_unixtime(totalprice) < TIMESTAMP '2026-01-01 00:00:00.000000'
                """,
                session,
                output(anyTree(
                        filter(
                                new Logical(OR, ImmutableList.of(
                                        comparison(LESS_THAN, new Reference(DOUBLE, "TOTALPRICE"), bound),
                                        new Call(IS_NAN, ImmutableList.of(new Reference(DOUBLE, "TOTALPRICE"))))),
                                tableScan("orders", ImmutableMap.of("TOTALPRICE", "totalprice"))))));

        // lower bound after epoch 0: NaN excluded by both forms, no disjunct, clean range
        assertPlan(
                """
                SELECT count(*) FROM orders
                WHERE from_unixtime(totalprice) >= TIMESTAMP '2026-01-01 00:00:00.000000'
                """,
                session,
                output(anyTree(
                        filter(
                                comparison(GREATER_THAN_OR_EQUAL, new Reference(DOUBLE, "TOTALPRICE"), bound),
                                tableScan("orders", ImmutableMap.of("TOTALPRICE", "totalprice"))))));
    }

    // proves the filter is rewritten to a plain bigint range directly above the scan; actual connector
    // pushdown and file pruning are verified separately on a live cluster (see the deployment doc)
    @Test
    public void testMetabaseQueryFilterRewrittenToBigintRange()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setTimeZoneKey(getTimeZoneKey("Asia/Shanghai"))
                .build();
        long low = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        long high = ZonedDateTime.of(2026, 10, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;

        assertPlan(
                """
                SELECT count(*)
                FROM (
                    SELECT from_unixtime(orderkey / 1000, 'Asia/Shanghai') AS ts, custkey
                    FROM orders
                ) AS virtual_table
                WHERE ts >= TIMESTAMP '2026-09-01 00:00:00.000000'
                  AND ts < TIMESTAMP '2026-10-01 00:00:00.000000'
                """,
                session,
                output(anyTree(
                        filter(
                                new Logical(AND, ImmutableList.of(
                                        comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, low)),
                                        comparison(LESS_THAN, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, high)))),
                                tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))))));
    }

    // BETWEEN over the chain desugars to a Let that the rule eliminates; assert the exact rewritten
    // predicate structure, not just the absence of from_unixtime
    @Test
    public void testBetweenIsFullyUnwrapped()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setTimeZoneKey(getTimeZoneKey("Asia/Shanghai"))
                .build();
        long low = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        long high = ZonedDateTime.of(2026, 10, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;

        // <= T is inclusive of the whole final second, hence the exclusive high bound is high + 1000
        assertPlan(
                """
                SELECT count(*)
                FROM (
                    SELECT from_unixtime(orderkey / 1000, 'Asia/Shanghai') AS ts
                    FROM orders
                ) AS virtual_table
                WHERE ts BETWEEN TIMESTAMP '2026-09-01 00:00:00.000000' AND TIMESTAMP '2026-10-01 00:00:00.000000'
                """,
                session,
                output(anyTree(
                        filter(
                                new Logical(AND, ImmutableList.of(
                                        comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, low)),
                                        comparison(LESS_THAN, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, high + 1000)))),
                                tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))))));

        // endpoint equivalence of the desugared Let form: low-1, low, high+999, high+1000, NULL
        Symbol bound = new Symbol(TIMESTAMP_TZ_MICROS, "between");
        Reference boundRef = new Reference(TIMESTAMP_TZ_MICROS, "between");
        Constant t1 = new Constant(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000, 0, getTimeZoneKey("UTC")));
        Constant t3 = new Constant(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(3000, 0, getTimeZoneKey("UTC")));
        Expression letForm = new Let(bound, new Cast(chain(), TIMESTAMP_TZ_MICROS), new Logical(AND, ImmutableList.of(
                comparison(LESS_THAN_OR_EQUAL, t1, boundRef),
                comparison(LESS_THAN_OR_EQUAL, boundRef, t3))));
        Expression rewritten = unwrap(letForm);
        assertThat(rewritten).isNotEqualTo(letForm);
        for (Long n : new Long[] {999L, 1000L, 3999L, 4000L, null}) {
            assertThat(evaluate(rewritten, n)).as("n=%s", n).isEqualTo(evaluate(letForm, n));
        }
    }

    private static void assertUnchanged(Expression expression)
    {
        assertThat(unwrap(expression)).isEqualTo(expression);
    }

    private static Expression unwrap(Expression expression)
    {
        return unwrap(TEST_SESSION, expression);
    }

    private static Expression unwrap(Session session, Expression expression)
    {
        return unwrapFromUnixTime(session, FUNCTIONS.getPlannerContext(), emptySymbolAllocator(), expression);
    }

    // from_unixtime(CAST(n / 1000 AS double))
    private static Expression chain()
    {
        return new Call(FROM_UNIXTIME, ImmutableList.of(new Cast(division(N, 1000), DOUBLE)));
    }

    // from_unixtime(CAST(n / 1000 AS double), zone); a null zone builds a NULL varchar constant
    private static Expression chain(String zone)
    {
        return new Call(FROM_UNIXTIME_ZONE, ImmutableList.of(
                new Cast(division(N, 1000), DOUBLE),
                new Constant(createVarcharType(13), zone == null ? null : utf8Slice(zone))));
    }

    // from_unixtime_nanos(n)
    private static Expression nanosChain()
    {
        return new Call(FROM_UNIXTIME_NANOS, ImmutableList.of(N));
    }

    // CAST(chain() AS timestamp(6) with time zone), the precision-widening cast introduced when compared against a timestamp(6) constant
    private static Expression widenedChain()
    {
        return widenedTo(TIMESTAMP_TZ_MICROS);
    }

    // chain() widened to the given timestamp with time zone precision, possibly through multiple casts
    private static Expression widenedTo(TimestampWithTimeZoneType target)
    {
        Expression expression = chain();
        int[] steps = target.getPrecision() <= 6 ? new int[] {target.getPrecision()} : new int[] {6, target.getPrecision()};
        for (int precision : steps) {
            if (precision == 3) {
                continue;
            }
            expression = new Cast(expression, createTimestampWithTimeZoneType(precision));
        }
        return expression;
    }

    private static Call division(Expression dividend, long divisor)
    {
        return new Call(DIVIDE_BIGINT, ImmutableList.of(dividend, new Constant(BIGINT, divisor)));
    }

    private static Constant tzConstant(int precision, long epochMillis, String zone)
    {
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
        TimeZoneKey zoneKey = getTimeZoneKey(zone);
        if (type.isShort()) {
            return new Constant(type, packDateTimeWithZone(epochMillis, zoneKey));
        }
        return new Constant(type, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, zoneKey));
    }
}
