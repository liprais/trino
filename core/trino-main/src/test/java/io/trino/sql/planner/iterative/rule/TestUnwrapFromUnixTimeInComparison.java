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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
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
import static io.trino.spi.type.TimestampType.createTimestampType;
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
import static io.trino.sql.ir.TestingIr.between;
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
    public void testInstantPreservingAtTimeZoneIsPeeled()
    {
        // `value AT TIME ZONE <constant zone>` only relabels the zone and never changes the instant, so the
        // comparison can be inverted onto the chain behind it
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, atTimeZone(chain(), "UTC"), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
        assertThat(unwrap(comparison(LESS_THAN, atTimeZone(chain("Asia/Shanghai"), "+08:00"), tzConstant(3, 0, "UTC"))))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, -999L)));
        // constant on the left
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, tzConstant(3, 1000, "UTC"), atTimeZone(chain(), "Asia/Shanghai"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 1000L)));
        assertThat(unwrap(comparison(GREATER_THAN, atTimeZone(chain(), "UTC"), tzConstant(3, 1000, "UTC"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, 2000L)));
        // NULL, non-constant and invalid zones keep the original call so that NULL and error semantics survive
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, atTimeZone(chain(), (String) null), tzConstant(3, 1000, "UTC")));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, atTimeZone(chain(), "Not/AZone"), tzConstant(3, 1000, "UTC")));
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                atTimeZone(chain(), new Reference(createVarcharType(3), "zone")), tzConstant(3, 1000, "UTC")));
    }

    @Test
    public void testWallClockDomainRangeOperators()
    {
        // the wall clock 2026-09-01 00:00:00 in Asia/Shanghai is the instant 2026-08-31T16:00Z
        long instant = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");

        // from_unixtime(n / 1000) renders whole seconds, so the leaf grid is 1000 millis wide
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, wallClockChain("Asia/Shanghai"), boundary)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant)));
        assertThat(unwrap(comparison(LESS_THAN, wallClockChain("Asia/Shanghai"), boundary)))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, instant)));
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, wallClockChain("Asia/Shanghai"), boundary)))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, instant + 1000)));
        assertThat(unwrap(comparison(GREATER_THAN, wallClockChain("Asia/Shanghai"), boundary)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant + 1000)));

        // a precision-only widening cast on the wall clock side is peeled
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL,
                castToTimestamp(wallClockChain("Asia/Shanghai"), 6), localTimestamp(6, "2026-09-01 00:00:00.000000"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant)));

        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            Expression original = comparison(operator, wallClockChain("Asia/Shanghai"), boundary);
            Expression rewritten = unwrap(original);
            assertThat(rewritten).isNotEqualTo(original);
            for (long n : new long[] {instant - 1000, instant, instant + 1, instant + 500, instant + 1000, instant + 2000}) {
                assertThat(evaluate(rewritten, n))
                        .as("operator %s, n=%s", operator, n)
                        .isEqualTo(evaluate(original, n));
            }
        }
    }

    @Test
    public void testWallClockDomainWideningPreservesRejectedZone()
    {
        for (int precision : List.of(6, 9, 12)) {
            Expression value = castToTimestamp(new Cast(atTimeZone(chain("Asia/Shanghai"), "UTC"),
                    createTimestampWithTimeZoneType(precision)), precision);
            Constant boundary = localTimestamp(precision, "2026-09-01 00:00:00." + "0".repeat(precision));
            for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
                assertUnchanged(comparison(operator, value, boundary));
            }
            // UTC renders this instant as August 31, so it must not be admitted as Shanghai's midnight.
            assertThat(evaluate(comparison(GREATER_THAN_OR_EQUAL, value, boundary), METABASE_BOUNDARY_K * 1000))
                    .isFalse();
        }
    }

    @Test
    public void testWallClockDomainWideningPreservesOutermostZone()
    {
        for (int precision : List.of(6, 9, 12)) {
            for (String zone : List.of("Asia/Shanghai", "+08:00")) {
                Expression inner = new Cast(atTimeZone(chain("UTC"), "America/New_York"), TIMESTAMP_TZ_MICROS);
                Expression value = atTimeZone(inner, zone);
                if (precision > 6) {
                    value = new Cast(value, createTimestampWithTimeZoneType(precision));
                }
                value = castToTimestamp(value, precision);
                Constant boundary = localTimestamp(precision, "2026-09-01 00:00:00." + "0".repeat(precision));
                for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
                    Expression original = comparison(operator, value, boundary);
                    Expression rewritten = unwrap(original);
                    assertThat(rewritten).isEqualTo(unwrap(comparison(operator, castToTimestamp(chain(zone), precision), boundary)));
                    for (Long n : new Long[] {METABASE_BOUNDARY_K * 1000 - 1, METABASE_BOUNDARY_K * 1000,
                            METABASE_BOUNDARY_K * 1000 + 1, METABASE_BOUNDARY_K * 1000 + 999,
                            METABASE_BOUNDARY_K * 1000 + 1000, METABASE_BOUNDARY_K * 1000 + 28_800_000, null}) {
                        assertThat(evaluate(rewritten, n))
                                .as("precision %s, zone %s, operator %s, n=%s", precision, zone, operator, n)
                                .isEqualTo(evaluate(original, n));
                    }
                }
            }
        }
    }

    @Test
    public void testWallClockDomainWideningPreservesInvalidZoneSemantics()
    {
        Constant boundary = localTimestamp(6, "2026-09-01 00:00:00");
        for (Expression invalidZone : List.of(new Constant(createVarcharType(1), null),
                new Constant(createVarcharType(9), utf8Slice("Not/AZone")), new Reference(createVarcharType(13), "zone"))) {
            Expression inner = atTimeZone(chain("Asia/Shanghai"), invalidZone);
            Expression widened = new Cast(inner, TIMESTAMP_TZ_MICROS);
            Expression original = comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(widened, 6), boundary);
            assertUnchanged(original);
            // A valid outer zone must not hide a NULL, invalid or dynamic inner zone.
            assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(atTimeZone(widened, "Asia/Shanghai"), 6), boundary));
        }
    }

    @Test
    public void testCastToTimestampOnlyAsiaShanghaiIsUnwrapped()
    {
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");
        long shanghai = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;

        // the one-argument from_unixtime attaches the session zone, and the zoneless cast renders in that zone
        Session shanghaiSession = testSessionBuilder().setTimeZoneKey(getTimeZoneKey("Asia/Shanghai")).build();
        assertThat(unwrap(shanghaiSession, comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain()), boundary)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, shanghai)));
        // an explicit Asia/Shanghai zone argument also matches, regardless of the session zone
        Session utcSession = testSessionBuilder().setTimeZoneKey(getTimeZoneKey("UTC")).build();
        assertThat(unwrap(utcSession, comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("Asia/Shanghai")), boundary)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, shanghai)));

        // fixed +08:00 zones render the same wall clock as Asia/Shanghai and are unwrapped as well;
        // GMT+8 is normalized to +08:00 by TimeZoneKey, and the +08:00 form matches directly
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("GMT+8")), boundary)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, shanghai)));
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("+08:00")), boundary)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, shanghai)));

        // every other zone is left alone, even fixed-offset ones with a different offset:
        // a non-Shanghai session zone, and an explicit UTC zone
        assertUnchanged(utcSession, comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain()), boundary));
        assertUnchanged(shanghaiSession, comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("UTC")), boundary));
    }

    @Test
    public void testWallClockDomainShanghaiDaylightSavingEraIsRejected()
    {
        // Asia/Shanghai ran daylight saving time 1986-1991, so the wall clock is not a pure shift of the
        // instant that far back; thresholds reaching into that era are abandoned
        Constant dstEra = localTimestamp("1988-06-01 00:00:00.000");
        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            assertUnchanged(comparison(operator, wallClockChain("Asia/Shanghai"), dstEra));
        }

        // right after the era the inversion applies again
        long shanghai1992 = ZonedDateTime.of(1992, 1, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("Asia/Shanghai")), localTimestamp("1992-01-01 00:00:00.000"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, shanghai1992)));

        // fixed +08:00 zones never changed offset and stay exact for any date
        long gmt8_1988 = ZonedDateTime.of(1988, 6, 1, 0, 0, 0, 0, ZoneId.of("GMT+8")).toEpochSecond() * 1000;
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("GMT+8")), dstEra)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, gmt8_1988)));
    }

    @Test
    public void testWallClockDomainSubMillisecondConstants()
    {
        // constants off the millisecond grid: the bound is rounded toward the strict side (ceil for >= and <,
        // floor+1 for <= and >) and then inverted exactly; equivalence is checked point-wise across the leaf
        // grid, which renders whole seconds
        long instant = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        Expression chain6 = castToTimestamp(atTimeZone(chain(), "Asia/Shanghai"), 6);
        for (String value : List.of("2026-09-01 00:00:00.000500", "2026-09-01 00:00:00.999500", "2026-08-31 23:59:59.999500")) {
            Constant boundary = localTimestamp(6, value);
            for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
                Expression original = comparison(operator, chain6, boundary);
                Expression rewritten = unwrap(original);
                assertThat(rewritten).as("operator %s, constant %s", operator, value).isNotEqualTo(original);
                for (long n = instant - 2000; n <= instant + 3000; n += 250) {
                    assertThat(evaluate(rewritten, n))
                            .as("operator %s, constant %s, n=%s", operator, value, n)
                            .isEqualTo(evaluate(original, n));
                }
                for (long n : new long[] {instant - 1, instant, instant + 1, instant + 999, instant + 1000}) {
                    assertThat(evaluate(rewritten, n))
                            .as("operator %s, constant %s, n=%s", operator, value, n)
                            .isEqualTo(evaluate(original, n));
                }
            }
        }
    }

    @Test
    public void testWallClockDomainNegativeEpoch()
    {
        // 1969 is inside Asia/Shanghai's daylight saving guard, so this uses a fixed +08:00 zone; the point is
        // bigint division truncating toward zero on negative inputs (n=-1500 renders -1s, not -2s)
        long instant = ZonedDateTime.of(1969, 12, 31, 23, 59, 59, 0, ZoneId.of("GMT+8")).toEpochSecond() * 1000;
        Constant boundary = localTimestamp("1969-12-31 23:59:59.000");
        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            Expression original = comparison(operator, castToTimestamp(chain("GMT+8")), boundary);
            Expression rewritten = unwrap(original);
            assertThat(rewritten).as("operator %s", operator).isNotEqualTo(original);
            for (long n : new long[] {instant - 2500, instant - 1500, instant - 1001, instant - 1000, instant - 999,
                    instant - 500, instant - 1, instant, instant + 1, instant + 500, instant + 999, instant + 1000, instant + 1500}) {
                assertThat(evaluate(rewritten, n))
                        .as("operator %s, n=%s", operator, n)
                        .isEqualTo(evaluate(original, n));
            }
        }
    }

    @Test
    public void testWallClockDomainShanghaiGuardBoundary()
    {
        // the guard compares the rounded millisecond grid threshold k against lastChange + 24h, so the
        // boundary cases are k == lastChange + 24h (window starts exactly at the last offset change: allowed)
        // and k == lastChange + 24h - 1 (one millisecond into the daylight saving era: abandoned)
        List<ZoneOffsetTransition> transitions = ZoneId.of("Asia/Shanghai").getRules().getTransitions();
        long lastChange = transitions.get(transitions.size() - 1).getInstant().toEpochMilli();
        long guardK = lastChange + 86_400_000L;

        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN)) {
            assertUnchanged(comparison(operator, wallClockChain("Asia/Shanghai"), localTimestamp(guardK - 1)));
        }
        // <= and > round to floor(k)+1, so their bound is exactly guardK at this constant: allowed
        for (ComparisonOperator operator : List.of(LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            assertThat(unwrap(comparison(operator, wallClockChain("Asia/Shanghai"), localTimestamp(guardK - 1))))
                    .isNotEqualTo(comparison(operator, wallClockChain("Asia/Shanghai"), localTimestamp(guardK - 1)));
        }

        // k == guardK is allowed and exact; the threshold instant is guardK - 8h (fixed +08:00 across the window)
        long threshold = guardK - 8 * 3_600_000L;
        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            Expression original = comparison(operator, wallClockChain("Asia/Shanghai"), localTimestamp(guardK));
            Expression rewritten = unwrap(original);
            assertThat(rewritten).as("operator %s", operator).isNotEqualTo(original);
            for (long n : new long[] {threshold - 2000, threshold - 1000, threshold - 1, threshold, threshold + 1, threshold + 1000, threshold + 2000}) {
                assertThat(evaluate(rewritten, n))
                        .as("operator %s, n=%s", operator, n)
                        .isEqualTo(evaluate(original, n));
            }
        }
    }

    @Test
    public void testWallClockDomainRejectsFallBackTransitions()
    {
        // a fall-back renders the same wall clock twice, so the satisfying rows are not a single instant range
        ZoneOffsetTransition fallBack = nextTransition(false);
        long repeatedWallClock = wallClockMillis(fallBack.getDateTimeBefore().minusMinutes(30));
        Constant boundary = localTimestamp(repeatedWallClock);
        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            assertUnchanged(comparison(operator, wallClockChain("America/New_York"), boundary));
        }
    }

    @Test
    public void testWallClockDomainRejectsNonShanghaiZones()
    {
        // zones with daylight saving transitions are never unwrapped, even on dates far from any transition:
        // the rule does not reason about monotonicity at all, it simply only knows +08:00 zones
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");
        for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
            assertUnchanged(comparison(operator, wallClockChain("America/New_York"), boundary));
            assertUnchanged(comparison(operator, wallClockChain("Europe/London"), boundary));
            assertUnchanged(comparison(operator, wallClockChain("UTC"), boundary));
            // a fixed offset other than +08:00 does not match either
            assertUnchanged(comparison(operator, wallClockChain("GMT+7"), boundary));
        }
    }

    @Test
    public void testWallClockDomainScope()
    {
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");
        Expression chain = wallClockChain("Asia/Shanghai");

        // equality and IN would also have to reason about wall clocks rendered twice or not at all
        assertUnchanged(comparison(EQUAL, chain, boundary));
        assertUnchanged(comparison(NOT_EQUAL, chain, boundary));
        assertUnchanged(new In(chain, ImmutableList.of(boundary)));

        // non-constant zone
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL,
                castToTimestamp(atTimeZone(chain(), new Reference(createVarcharType(3), "zone"))), boundary));


        // the nanosecond chain renders on a nanosecond grid
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(nanosChain(), 9),
                new Constant(createTimestampType(9), DateTimes.parseTimestamp(9, "2026-09-01 00:00:00.000000000"))));
    }

    // the chain can sit on the right of the comparison: the operator flips and the same wall clock bounds
    // apply, which must also hold for the strict operators whose bound is rounded toward the strict side
    @Test
    public void testWallClockDomainFlippedComparison()
    {
        long instant = METABASE_BOUNDARY_K * 1000;
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");
        Expression wallClock = wallClockChain("Asia/Shanghai");

        // boundary <= chain  <=>  chain >= boundary
        assertThat(unwrap(comparison(LESS_THAN_OR_EQUAL, boundary, wallClock)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant)));
        // boundary < chain  <=>  chain > boundary  <=>  chain >= boundary + 1s
        assertThat(unwrap(comparison(LESS_THAN, boundary, wallClock)))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant + 1000)));
        // boundary >= chain  <=>  chain <= boundary  <=>  chain < boundary + 1s
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, boundary, wallClock)))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, instant + 1000)));
        // boundary > chain  <=>  chain < boundary
        assertThat(unwrap(comparison(GREATER_THAN, boundary, wallClock)))
                .isEqualTo(comparison(LESS_THAN, N, new Constant(BIGINT, instant)));

        for (ComparisonOperator operator : List.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL)) {
            Expression original = comparison(operator, boundary, wallClock);
            Expression rewritten = unwrap(original);
            assertThat(rewritten).as("operator %s", operator).isNotEqualTo(original);
            for (long n : new long[] {instant - 1000, instant - 1, instant, instant + 1, instant + 999,
                    instant + 1000, instant + 1001, instant + 2000}) {
                assertThat(evaluate(rewritten, n))
                        .as("operator %s, n=%s", operator, n)
                        .isEqualTo(evaluate(original, n));
            }
        }
    }

    @Test
    public void testWallClockDomainPrecisionGuards()
    {
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");
        long instant = METABASE_BOUNDARY_K * 1000;
        Expression wallClock = atTimeZone(chain("Asia/Shanghai"), "Asia/Shanghai");

        // rendering below millisecond precision rounds the wall clock onto a coarser grid that the rule does
        // not invert, so the comparison is left alone
        for (int precision : List.of(0, 1, 2)) {
            Constant rounded = localTimestamp(precision, "2026-09-01 00:00:00");
            for (ComparisonOperator operator : List.of(GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN)) {
                assertUnchanged(comparison(operator, castToTimestamp(wallClock, precision), rounded));
            }
        }

        // narrowing a zoneless timestamp rounds it as well, so it is not peeled
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(castToTimestamp(wallClock, 6), 3), boundary));

        // widening a zoneless timestamp is lossless and only peeled
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL,
                castToTimestamp(castToTimestamp(wallClock, 6), 9), localTimestamp(9, "2026-09-01 00:00:00.000000000"))))
                .isEqualTo(comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant)));
    }

    // BETWEEN renders the chain twice, so the planner desugars it to a Let; both bounds must be inverted and
    // the Let must disappear, otherwise the predicate never reaches the scan
    @Test
    public void testWallClockDomainBetweenIsFullyUnwrapped()
    {
        long lowInstant = METABASE_BOUNDARY_K * 1000;
        long highInstant = ZonedDateTime.of(2026, 10, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        Expression letForm = between(
                castToTimestamp(chain("Asia/Shanghai")),
                localTimestamp("2026-09-01 00:00:00.000"),
                localTimestamp("2026-10-01 00:00:00.000"));

        Expression rewritten = unwrap(letForm);
        assertThat(rewritten).isEqualTo(new Logical(AND, ImmutableList.of(
                comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, lowInstant)),
                comparison(LESS_THAN, N, new Constant(BIGINT, highInstant + 1000)))));

        // inclusive upper bound: the whole final second of the wall clock satisfies it
        for (Long n : new Long[] {lowInstant - 1000, lowInstant - 1, lowInstant, lowInstant + 1,
                highInstant - 1, highInstant, highInstant + 999, highInstant + 1000, highInstant + 1001, null}) {
            assertThat(evaluate(rewritten, n)).as("n=%s", n).isEqualTo(evaluate(letForm, n));
        }
    }

    @Test
    public void testWallClockDomainFixedPlusEightZones()
    {
        long instant = METABASE_BOUNDARY_K * 1000;
        Constant boundary = localTimestamp("2026-09-01 00:00:00.000");
        Expression expected = comparison(GREATER_THAN_OR_EQUAL, N, new Constant(BIGINT, instant));

        // the three-argument form attaches a fixed offset zone
        Expression offsetChain = new Call(FROM_UNIXTIME_OFFSET, ImmutableList.of(
                new Cast(division(N, 1000), DOUBLE), new Constant(BIGINT, 8L), new Constant(BIGINT, 0L)));
        assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(offsetChain), boundary)))
                .isEqualTo(expected);

        // every fixed +08:00 zone renders the same wall clock, not only the two well-known spellings
        for (String zone : List.of("Asia/Shanghai", "GMT+8", "+08:00", "Etc/GMT-8")) {
            assertThat(unwrap(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain(zone)), boundary)))
                    .as("zone %s", zone)
                    .isEqualTo(expected);
        }

        // a fixed zone with any other offset stays untouched
        assertUnchanged(comparison(GREATER_THAN_OR_EQUAL, castToTimestamp(chain("Etc/GMT-7")), boundary));
    }

    // the first spring-forward (or fall-back) transition after mid 2025; scanning with nextTransition also
    // resolves the recurring rules the time zone database keeps for the future
    private static ZoneOffsetTransition nextTransition(boolean forward)
    {
        ZoneRules rules = ZoneId.of("America/New_York").getRules();
        Instant instant = Instant.parse("2025-06-01T00:00:00Z");
        for (int i = 0; i < 8; i++) {
            ZoneOffsetTransition transition = rules.nextTransition(instant);
            if (transition == null) {
                break;
            }
            if ((transition.getOffsetAfter().getTotalSeconds() > transition.getOffsetBefore().getTotalSeconds()) == forward) {
                return transition;
            }
            instant = transition.getInstant();
        }
        throw new IllegalStateException("no transition found");
    }

    // a wall clock as it is stored in a timestamp: epoch millis of the local date-time read as if it were UTC
    private static long wallClockMillis(java.time.LocalDateTime wallClock)
    {
        return wallClock.toInstant(ZoneOffset.UTC).toEpochMilli();
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
        // (an outer cast to a timestamp without time zone used to be out of scope; the wall clock domain
        //  covers it now, see testWallClockDomainRangeOperators)
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

    // the production Superset shape: the wall clock of the chain is compared after a zoneless CAST, so the
    // filter must become a plain bigint range directly above the scan (pushdown and file pruning follow)
    @Test
    public void testWallClockDomainQueryFilterRewrittenToBigintRange()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setTimeZoneKey(getTimeZoneKey("UTC"))
                .build();
        long low = ZonedDateTime.of(2026, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;
        long high = ZonedDateTime.of(2026, 10, 1, 0, 0, 0, 0, ZoneId.of("Asia/Shanghai")).toEpochSecond() * 1000;

        assertPlan(
                """
                SELECT count(*)
                FROM (
                    SELECT CAST(from_unixtime(orderkey / 1000) AT TIME ZONE 'Asia/Shanghai' AS TIMESTAMP) AS date, custkey
                    FROM orders
                ) AS virtual_table
                WHERE date >= TIMESTAMP '2026-09-01 00:00:00'
                  AND date < TIMESTAMP '2026-10-01 00:00:00'
                """,
                session,
                output(anyTree(
                        filter(
                                new Logical(AND, ImmutableList.of(
                                        comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, low)),
                                        comparison(LESS_THAN, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, high)))),
                                tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))))));
    }

    private static void assertUnchanged(Expression expression)
    {
        assertThat(unwrap(expression)).isEqualTo(expression);
    }

    private static void assertUnchanged(Session session, Expression expression)
    {
        assertThat(unwrap(session, expression)).isEqualTo(expression);
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

    // at_timezone(value, zone); a null zone builds a NULL varchar constant
    private static Expression atTimeZone(Expression value, String zone)
    {
        return atTimeZone(value, new Constant(createVarcharType(zone == null ? 1 : zone.length()), zone == null ? null : utf8Slice(zone)));
    }

    private static Expression atTimeZone(Expression value, Expression zone)
    {
        // AT TIME ZONE resolves per argument types; the zone varchar length is a literal parameter
        return new Call(FUNCTIONS.resolveFunction("at_timezone", fromTypes(value.type(), zone.type())), ImmutableList.of(value, zone));
    }

    // timestamp(p) constant without a time zone, from its wall clock
    private static Constant localTimestamp(String value)
    {
        return new Constant(createTimestampType(3), DateTimes.parseTimestamp(3, value));
    }

    private static Constant localTimestamp(int precision, String value)
    {
        return new Constant(createTimestampType(precision), DateTimes.parseTimestamp(precision, value));
    }

    private static Constant localTimestamp(long wallClockMillis)
    {
        return new Constant(createTimestampType(3), wallClockMillis * 1000);
    }

    // CAST(value AS timestamp(p)), the zoneless cast that renders the value's wall clock
    private static Expression castToTimestamp(Expression value)
    {
        return castToTimestamp(value, 3);
    }

    private static Expression castToTimestamp(Expression value, int precision)
    {
        return new Cast(value, createTimestampType(precision));
    }

    // CAST(at_timezone(chain, zone) AS timestamp(3))
    private static Expression wallClockChain(String zone)
    {
        return castToTimestamp(atTimeZone(chain(), zone));
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
