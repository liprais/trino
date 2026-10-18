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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.DoubleUnaryOperator;
import java.util.function.LongUnaryOperator;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.isUnwrapFromUnixTimeInComparisonEnabled;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static java.util.Objects.requireNonNull;

/**
 * Inverts comparisons of the form
 * <pre>
 *     from_unixtime(E(n) [, zone | hours, minutes]) {=, &lt;&gt;, &lt;, &lt;=, &gt;, &gt;=} constant
 *     from_unixtime_nanos(n) {=, ...} constant
 * </pre>
 * where E(n) is a column reference, a lossless cast of a bigint column to double, or division of a
 * column by a positive constant, into an equivalent comparison directly on the column, so that
 * predicate pushdown and file pruning apply.
 * <p>
 * The chain can also be compared as a wall clock rather than as an instant. {@code CAST(x AS timestamp(p))}
 * renders the instant {@code x} in the zone {@code x} carries, which is the zone of an enclosing
 * {@code x AT TIME ZONE <constant zone>} or the zone {@code from_unixtime} itself attaches. The ordering
 * comparisons of such a wall clock are inverted back to instant bounds. This is only attempted for
 * {@code Asia/Shanghai} and fixed +08:00 zones such as {@code GMT+8}: their offset never changes, so the
 * wall clock is a pure shift of the instant and the inversion is a mathematical identity — no daylight
 * saving transition can make a wall clock occur twice or not at all. Shanghai ran daylight saving time
 * 1986-1991, so thresholds whose ambiguity window reaches back into that era are abandoned. Zones with
 * transitions (a fall-back renders the same wall clock twice, so the satisfying rows are not a single
 * range of instants) are deliberately left alone, as are equality, IN and the nanosecond chain.
 * <p>
 * Semantics relied upon (DateTimeFunctions): {@code from_unixtime(x)} produces epoch millis by rounding
 * {@code x * 1000} half away from zero, treating a value as a tie when it is the double nearest a half
 * millisecond (see {@code DateTimeFunctions.unixTimeToEpochMillis}); {@code from_unixtime_nanos(n)} is
 * the identity in epoch nanos. Bigint division truncates toward zero. The zone argument only relabels
 * the instant and never affects comparison, but it is still validated so that invalid-zone errors
 * and NULL-zone NULL semantics are preserved.
 * <p>
 * Accepted semantic deviations (documented for this fork):
 * <ul>
 * <li>Inputs outside the representable timestamp range fail with INVALID_FUNCTION_ARGUMENT in the
 * original expression, while the rewritten comparison returns a boolean (row kept or excluded).
 * <li>For double chains the analytic real-number threshold is only a guess: every boundary is adjusted
 * against the actual floating point semantics (division, multiplication, and from_unixtime's rounding)
 * by direct evaluation, so the rewrite is exact by construction.
 * <li>For a NaN input on a double chain, {@code from_unixtime(NaN)} yields epoch 0 in the original
 * expression; the rewrite therefore adds an explicit {@code is_nan} disjunct whenever millis 0
 * satisfies the comparison, keeping the rewrite exact for NaN as well.
 * </ul>
 * NULL propagation is preserved: a NULL column value evaluates to NULL in both forms (empty ranges
 * are expressed as {@code n >= 0 AND n < 0} rather than a FALSE constant for this reason).
 *
 * @see UnwrapYearInComparison
 */
public class UnwrapFromUnixTimeInComparison
        extends ExpressionRewriteRuleSet
{
    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
    private static final BigDecimal HALF = new BigDecimal("0.5");

    private static final int MILLIS_PRECISION = 3;
    // the zones for which wall clock comparisons are inverted: Asia/Shanghai has had a fixed +08:00 offset
    // since September 1991, and fixed-offset +08:00 zones (GMT+8 and friends) render the same wall clock for
    // every instant, so in both cases the wall clock is a pure shift of the instant and the inversion is exact
    private static final TimeZoneKey WALL_CLOCK_ZONE = getTimeZoneKey("Asia/Shanghai");
    private static final ZoneOffset WALL_CLOCK_OFFSET = ZoneOffset.ofHours(8);
    // every zone offset is bounded by ±18h (TimeZoneKey validates that bound), so a wall clock comparison is
    // decided outside [threshold - 24h, threshold + 24h]
    private static final long AMBIGUITY_WINDOW_MILLIS = Duration.ofHours(24).toMillis();
    // Asia/Shanghai ran daylight saving time 1986-1991; this is the instant of its last offset change, after
    // which the offset stays fixed forever. A threshold is only inverted when its whole ambiguity window lies
    // in the fixed-offset era (bound - 24h >= lastChange), because before it the wall clock is not a pure
    // shift of the instant
    private static final BigInteger SHANGHAI_MONOTONE_BOUND =
            BigInteger.valueOf(lastOffsetChange(WALL_CLOCK_ZONE)).add(BigInteger.valueOf(AMBIGUITY_WINDOW_MILLIS));

    private static long lastOffsetChange(TimeZoneKey zone)
    {
        List<ZoneOffsetTransition> transitions = zone.getZoneId().getRules().getTransitions();
        if (transitions.isEmpty()) {
            return Long.MIN_VALUE;
        }
        return transitions.get(transitions.size() - 1).getInstant().toEpochMilli();
    }

    public UnwrapFromUnixTimeInComparison(PlannerContext plannerContext)
    {
        super(createRewrite(plannerContext));
    }

    private static ExpressionRewriter createRewrite(PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        return (expression, context) -> {
            if (!isUnwrapFromUnixTimeInComparisonEnabled(context.getSession())) {
                return expression;
            }
            return unwrapFromUnixTime(context.getSession(), plannerContext, context.getSymbolAllocator(), expression);
        };
    }

    @VisibleForTesting
    static Expression unwrapFromUnixTime(
            Session session,
            PlannerContext plannerContext,
            SymbolAllocator symbolAllocator,
            Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(plannerContext, session, symbolAllocator), expression);
    }

    // smallest long n with eval(n) >= k, adjusted from the analytic guess; the adjustment is bounded
    // (the guess is normally off by at most a few ulps) and any failure to converge falls back to empty
    @VisibleForTesting
    static Optional<Long> adjustIntThresholdGE(LongUnaryOperator eval, BigDecimal guess, long k)
    {
        long n;
        try {
            n = guess.setScale(0, RoundingMode.CEILING).toBigIntegerExact().longValueExact();
        }
        catch (ArithmeticException e) {
            return Optional.empty();
        }
        for (int i = 0; i < 100 && eval.applyAsLong(n) < k; i++) {
            if (n == Long.MAX_VALUE) {
                return Optional.empty();
            }
            n++;
        }
        for (int i = 0; i < 100 && n > Long.MIN_VALUE && eval.applyAsLong(n - 1) >= k; i++) {
            n--;
        }
        if (eval.applyAsLong(n) < k || (n > Long.MIN_VALUE && eval.applyAsLong(n - 1) >= k)) {
            return Optional.empty();
        }
        return Optional.of(n);
    }

    // smallest double x with eval(x) >= k, same construction
    @VisibleForTesting
    static Optional<Double> adjustDoubleThresholdGE(DoubleUnaryOperator eval, BigDecimal guess, long k)
    {
        double b = leastDoubleGE(guess);
        if (!Double.isFinite(b)) {
            return Optional.empty();
        }
        for (int i = 0; i < 100 && eval.applyAsDouble(b) < k; i++) {
            b = Math.nextUp(b);
        }
        for (int i = 0; i < 100 && b > -Double.MAX_VALUE && eval.applyAsDouble(Math.nextDown(b)) >= k; i++) {
            b = Math.nextDown(b);
        }
        if (eval.applyAsDouble(b) < k || eval.applyAsDouble(Math.nextDown(b)) >= k) {
            return Optional.empty();
        }
        return Optional.of(b);
    }

    private static double leastDoubleGE(BigDecimal value)
    {
        double result = value.doubleValue();
        if (!Double.isFinite(result)) {
            // out of the double range; callers reject non-finite thresholds with no rewrite
            return result;
        }
        if (new BigDecimal(result).compareTo(value) < 0) {
            result = Math.nextUp(result);
        }
        return result;
    }

    /**
     * A recognized from_unixtime chain whose comparison can be inverted onto the leaf column.
     */
    private sealed interface Chain
    {
        Reference leaf();
    }

    /**
     * f(n) = 1000 * trunc(n / divisor), divisor &gt;= 1 (divisor 1 covers a bare bigint epoch column
     * and its lossless cast to double). Leaf is always BIGINT.
     */
    private record IntChain(Reference leaf, long divisor)
            implements Chain {}

    /**
     * f(x) = from_unixtime(x / divisor) in epoch millis, divisor &gt; 0. Leaf may be BIGINT (integer
     * tightening applies) or DOUBLE.
     */
    private record DoubleChain(Reference leaf, BigDecimal divisor, boolean leafIsInteger)
            implements Chain {}

    /**
     * f(n) = n, epoch nanos identity. Leaf is always BIGINT.
     */
    private record NanosChain(Reference leaf)
            implements Chain {}

    /**
     * Where a comparison takes place: on the instants of a timestamp with time zone value, or on the wall clock
     * that value is rendered as in a given zone.
     */
    private sealed interface Domain
    {
        /**
         * compared as timestamp(p) with time zone: instants
         */
        record Tz()
                implements Domain {}

        /**
         * compared as timestamp(p): the chain's instant rendered as a wall clock in the zone
         */
        record Local(TimeZoneKey zone)
                implements Domain {}
    }

    private record Match(Chain chain, Domain domain) {}

    /**
     * a recognized chain together with its rendering zone (null for the nanos chain without AT TIME ZONE)
     */
    private record TzChain(Chain chain, TimeZoneKey zone) {}

    private record LocalChain(Chain chain, TimeZoneKey zone) {}

    private static class Visitor
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        private final IrExpressionOptimizer optimizer;
        private final Metadata metadata;
        private final Session session;
        private final SymbolAllocator symbolAllocator;

        public Visitor(PlannerContext plannerContext, Session session, SymbolAllocator symbolAllocator)
        {
            this.optimizer = plannerContext.getExpressionOptimizer();
            this.metadata = plannerContext.getMetadata();
            this.session = requireNonNull(session, "session is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Call expression = treeRewriter.defaultRewrite(node, null);
            if (!(matchComparison(expression) instanceof Comparison comparison)) {
                return expression;
            }
            // The unwrap logic expects the chain on the left. Try the flipped form when the chain is on the right,
            // but preserve the original comparison when it cannot be unwrapped.
            if (matchChain(comparison.right()).isPresent()) {
                return tryUnwrap(comparison.operator().flip(), comparison.right(), comparison.left())
                        .orElse(expression);
            }
            return tryUnwrap(comparison.operator(), comparison.left(), comparison.right())
                    .orElse(expression);
        }

        @Override
        public Expression rewriteIn(In node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            In in = treeRewriter.defaultRewrite(node, null);
            Optional<Match> match = matchChain(in.value());
            if (match.isEmpty()) {
                return in;
            }
            // unwrap only if every element can be inverted as an equality
            List<Expression> equalities = new ArrayList<>(in.valueList().size());
            for (Expression element : in.valueList()) {
                Optional<Expression> equality = foldToTimestampConstant(element, match.get().domain())
                        .flatMap(constant -> invertInDomain(match.get(), EQUAL, constant));
                if (equality.isEmpty()) {
                    return in;
                }
                equalities.add(equality.get());
            }
            return or(equalities);
        }

        // A BETWEEN over a non-trivial chain is desugared to `Let(symbol, chain, min <= symbol AND symbol <= max)`.
        // Invert both comparisons onto the leaf column and drop the Let entirely.
        @Override
        public Expression rewriteLet(Let node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Let let = treeRewriter.defaultRewrite(node, null);
            Optional<Match> chain = matchChain(let.value());
            if (chain.isEmpty()) {
                return let;
            }
            List<Expression> conjuncts = let.body() instanceof Logical logical && logical.operator() == Logical.Operator.AND
                    ? logical.terms()
                    : List.of(let.body());
            List<Expression> rewritten = new ArrayList<>(conjuncts.size());
            for (Expression conjunct : conjuncts) {
                if (!(conjunct instanceof Call call) || !(matchComparison(call) instanceof Comparison comparison)) {
                    return let;
                }
                Optional<Expression> inverted = invertBoundComparison(chain.get(), let.name(), comparison);
                if (inverted.isEmpty()) {
                    return let;
                }
                rewritten.add(inverted.get());
            }
            return and(rewritten);
        }

        private Optional<Expression> invertBoundComparison(Match match, Symbol bound, Comparison comparison)
        {
            boolean leftIsBound = comparison.left() instanceof Reference left && left.name().equals(bound.name());
            boolean rightIsBound = comparison.right() instanceof Reference right && right.name().equals(bound.name());
            if (leftIsBound == rightIsBound) {
                return Optional.empty();
            }
            ComparisonOperator operator = rightIsBound ? comparison.operator().flip() : comparison.operator();
            Expression constantSide = rightIsBound ? comparison.left() : comparison.right();
            return foldToTimestampConstant(constantSide, match.domain())
                    .flatMap(constant -> invertInDomain(match, operator, constant));
        }

        private Optional<Expression> tryUnwrap(ComparisonOperator operator, Expression chainSide, Expression constantSide)
        {
            return matchChain(chainSide)
                    .flatMap(match -> foldToTimestampConstant(constantSide, match.domain())
                            .flatMap(constant -> invertInDomain(match, operator, constant)));
        }

        // The constant is compared with the chain's output, so its type must match the comparison domain: an
        // instant for the instant domain, a wall clock for the wall clock domain.
        private Optional<Constant> foldToTimestampConstant(Expression expression, Domain domain)
        {
            Expression folded = optimizer.process(expression, session, symbolAllocator, ImmutableMap.of()).orElse(expression);
            if (!(folded instanceof Constant constant) || constant.value() == null) {
                return Optional.empty();
            }
            boolean matches = switch (domain) {
                case Domain.Tz() -> constant.type() instanceof TimestampWithTimeZoneType;
                case Domain.Local ignored -> constant.type() instanceof TimestampType;
            };
            return matches ? Optional.of(constant) : Optional.empty();
        }

        private Optional<Expression> invertInDomain(Match match, ComparisonOperator operator, Constant constant)
        {
            return switch (match.domain()) {
                case Domain.Tz() -> invert(match.chain(), operator, constant);
                case Domain.Local(TimeZoneKey zone) -> invertLocal(match.chain(), operator, constant, zone);
            };
        }

        // A comparison is made on instants (timestamp with time zone) or on a wall clock (timestamp). Try the
        // wall clock form first: it is the only one whose top-level expression is a zoneless cast.
        private Optional<Match> matchChain(Expression expression)
        {
            Optional<Match> local = matchLocalChain(expression);
            if (local.isPresent()) {
                return local;
            }
            return matchTzChain(expression).map(tz -> new Match(tz.chain(), new Domain.Tz()));
        }

        // CAST(... AS timestamp(p)): precision-only zoneless casts are peeled, and the cast of a timestamp with
        // time zone value to a zoneless timestamp enters the wall clock domain. The zone the value is rendered in
        // comes from the constant zone of an enclosing AT TIME ZONE, or from the zone from_unixtime attaches.
        private Optional<Match> matchLocalChain(Expression expression)
        {
            Expression current = expression;
            while (current instanceof Cast cast && cast.type() instanceof TimestampType target) {
                if (cast.expression().type() instanceof TimestampType source) {
                    if (target.getPrecision() < source.getPrecision() || target.getPrecision() < MILLIS_PRECISION) {
                        return Optional.empty();
                    }
                    current = cast.expression();
                    continue;
                }
                if (cast.expression().type() instanceof TimestampWithTimeZoneType source) {
                    if (target.getPrecision() < source.getPrecision() || target.getPrecision() < MILLIS_PRECISION) {
                        return Optional.empty();
                    }
                    return localChain(cast.expression())
                            .map(local -> new Match(local.chain(), new Domain.Local(local.zone())));
                }
                return Optional.empty();
            }
            return Optional.empty();
        }

        // The wall clock domain is only entered for Asia/Shanghai and fixed +08:00 zones (see isWallClockZone);
        // every other zone is left untouched so that no daylight saving transition can invalidate the inversion.
        private Optional<LocalChain> localChain(Expression expression)
        {
            // `value AT TIME ZONE <constant zone>` is instant preserving and only relabels the zone, so it decides
            // the zone in which the value is rendered when it is cast to a zoneless timestamp
            if (expression instanceof Call call
                    && call.function().name().equals(builtinFunctionName("at_timezone"))
                    && call.arguments().size() == 2
                    && call.type() instanceof TimestampWithTimeZoneType) {
                return constantZone(call.arguments().get(1))
                        .filter(zone -> isWallClockZone(zone))
                        .flatMap(zone -> matchTzChain(call.arguments().get(0))
                                .map(tz -> new LocalChain(tz.chain(), zone)));
            }
            // otherwise the value is rendered in the zone from_unixtime attached to it; the nanosecond chain
            // attaches the session zone as well but renders on a nanosecond grid, which this rule does not invert
            return matchTzChain(expression)
                    .filter(tz -> isWallClockZone(tz.zone()))
                    .map(tz -> new LocalChain(tz.chain(), tz.zone()));
        }

        private static boolean isWallClockZone(TimeZoneKey zone)
        {
            if (zone == null) {
                return false;
            }
            if (WALL_CLOCK_ZONE.equals(zone)) {
                return true;
            }
            ZoneRules rules = zone.getZoneId().getRules();
            return rules.isFixedOffset() && rules.getOffset(Instant.EPOCH).equals(WALL_CLOCK_OFFSET);
        }

        // Peels precision-widening timestamp with time zone casts, instant-preserving at_timezone calls with a
        // constant zone, and recognizes the from_unixtime chain
        private Optional<TzChain> matchTzChain(Expression expression)
        {
            Expression current = expression;
            TimeZoneKey renderingZone = null;
            while (true) {
                if (current instanceof Cast cast) {
                    if (!(cast.expression().type() instanceof TimestampWithTimeZoneType sourceType) ||
                            !(cast.type() instanceof TimestampWithTimeZoneType targetType) ||
                            targetType.getPrecision() <= sourceType.getPrecision()) {
                        return Optional.empty();
                    }
                    current = cast.expression();
                    continue;
                }
                // `value AT TIME ZONE <constant zone>` only relabels the zone of a timestamp with time zone and
                // never changes the instant, so it is transparent to every comparison operator and can be dropped
                // (same reasoning as UnwrapAtTimeZoneInComparison). Non-constant and invalid zones are left alone:
                // a null zone makes the result null, and an invalid zone fails the query at runtime.
                if (current instanceof Call call && isInstantPreservingAtTimeZone(call)) {
                    // Only the outermost AT TIME ZONE determines rendering; precision widening preserves it.
                    if (renderingZone == null) {
                        renderingZone = constantZone(call.arguments().get(1)).orElseThrow();
                    }
                    current = call.arguments().get(0);
                    continue;
                }
                break;
            }
            if (!(current instanceof Call call)) {
                return Optional.empty();
            }
            if (call.function().name().equals(builtinFunctionName("from_unixtime"))) {
                if (!call.type().equals(TIMESTAMP_TZ_MILLIS) || call.arguments().isEmpty() || call.arguments().size() > 3) {
                    return Optional.empty();
                }
                Optional<TimeZoneKey> zone = unixTimeZone(call.arguments());
                if (zone.isEmpty()) {
                    return Optional.empty();
                }
                TimeZoneKey effectiveZone = renderingZone == null ? zone.get() : renderingZone;
                return matchMillisInput(call.arguments().get(0)).map(chain -> new TzChain(chain, effectiveZone));
            }
            if (call.function().name().equals(builtinFunctionName("from_unixtime_nanos"))) {
                if (!call.type().equals(TIMESTAMP_TZ_NANOS) || call.arguments().size() != 1) {
                    return Optional.empty();
                }
                if (call.arguments().get(0) instanceof Reference reference && reference.type().equals(BIGINT)) {
                    return Optional.of(new TzChain(new NanosChain(reference), renderingZone));
                }
            }
            return Optional.empty();
        }

        private static boolean isInstantPreservingAtTimeZone(Call call)
        {
            if (!call.function().name().equals(builtinFunctionName("at_timezone")) ||
                    call.arguments().size() != 2 ||
                    !(call.type() instanceof TimestampWithTimeZoneType) ||
                    !call.arguments().get(0).type().equals(call.type())) {
                return false;
            }
            // only a constant, valid zone can be dropped: a null zone makes the result null and an invalid
            // zone fails the query at runtime, so dropping the call would change semantics
            return constantZone(call.arguments().get(1)).isPresent();
        }

        private static Optional<TimeZoneKey> constantZone(Expression expression)
        {
            if (!(expression.type() instanceof VarcharType) ||
                    !(expression instanceof Constant constant) ||
                    !(constant.value() instanceof Slice slice)) {
                return Optional.empty();
            }
            try {
                return Optional.of(getTimeZoneKey(slice.toStringUtf8()));
            }
            catch (TimeZoneNotSupportedException | IllegalArgumentException e) {
                return Optional.empty();
            }
        }

        // The zone from_unixtime attaches to its result: the explicit zone/offset argument, or the session zone
        // for the one-argument form. An unusable zone yields empty, so the original expression (and therefore the
        // runtime error it raises) is preserved.
        private Optional<TimeZoneKey> unixTimeZone(List<Expression> arguments)
        {
            try {
                if (arguments.size() == 1) {
                    return Optional.of(session.getTimeZoneKey());
                }
                if (arguments.size() == 2) {
                    return constantZone(arguments.get(1));
                }
                if (arguments.size() == 3) {
                    if (!(arguments.get(1) instanceof Constant hours) || !(hours.value() instanceof Long) ||
                            !(arguments.get(2) instanceof Constant minutes) || !(minutes.value() instanceof Long)) {
                        return Optional.empty();
                    }
                    return Optional.of(getTimeZoneKeyForOffset(Math.addExact(Math.multiplyExact((Long) hours.value(), 60), (Long) minutes.value())));
                }
                return Optional.empty();
            }
            catch (IllegalArgumentException | ArithmeticException | TimeZoneNotSupportedException e) {
                // preserve the original expression so that the runtime error still fires
                return Optional.empty();
            }
        }

        private Optional<Chain> matchMillisInput(Expression argument)
        {
            Expression inner = argument;
            if (inner instanceof Cast cast && cast.type().equals(DOUBLE)) {
                inner = cast.expression();
            }
            if (inner instanceof Reference reference) {
                if (reference.type().equals(BIGINT)) {
                    return Optional.of(new IntChain(reference, 1));
                }
                if (reference.type().equals(DOUBLE)) {
                    return Optional.of(new DoubleChain(reference, BigDecimal.ONE, false));
                }
                return Optional.empty();
            }
            if (!(inner instanceof Call divide) ||
                    !divide.function().name().equals(builtinFunctionName(DIVIDE)) ||
                    divide.arguments().size() != 2) {
                return Optional.empty();
            }
            Expression divisor = optimizer.process(divide.arguments().get(1), session, symbolAllocator, ImmutableMap.of())
                    .orElse(divide.arguments().get(1));
            if (!(divisor instanceof Constant divisorConstant) || divisorConstant.value() == null) {
                return Optional.empty();
            }
            Expression dividend = divide.arguments().get(0);
            if (divide.type().equals(BIGINT)) {
                if (dividend instanceof Reference reference && reference.type().equals(BIGINT) &&
                        divisorConstant.type().equals(BIGINT) && divisorConstant.value() instanceof Long value && value > 0) {
                    return Optional.of(new IntChain(reference, value));
                }
                return Optional.empty();
            }
            if (divide.type().equals(DOUBLE)) {
                if (!(divisorConstant.value() instanceof Double value) || !Double.isFinite(value) || value <= 0) {
                    return Optional.empty();
                }
                if (dividend instanceof Cast cast && cast.type().equals(DOUBLE) &&
                        cast.expression() instanceof Reference reference && reference.type().equals(BIGINT)) {
                    return Optional.of(new DoubleChain(reference, new BigDecimal(value), true));
                }
                if (dividend instanceof Reference reference && reference.type().equals(DOUBLE)) {
                    return Optional.of(new DoubleChain(reference, new BigDecimal(value), false));
                }
            }
            return Optional.empty();
        }

        // The chain's instant is rendered as a wall clock in the zone and the comparison is on the rendered
        // value, so invert it back to a comparison on the instant and let the existing inversion map it onto the
        // leaf column. Only the four ordering operators are supported: equality and IN would additionally have to
        // reason about wall clocks that no instant renders to or that are rendered twice. The zone is always
        // Asia/Shanghai or a fixed +08:00 zone (enforced when the wall clock domain is entered), whose fixed
        // offset makes the wall clock a pure shift of the instant, so the inversion is a mathematical identity.
        private Optional<Expression> invertLocal(Chain chain, ComparisonOperator operator, Constant constant, TimeZoneKey zone)
        {
            if (chain instanceof NanosChain) {
                return Optional.empty();
            }
            Optional<BigDecimal> localMillis = exactLocalMillis(constant);
            if (localMillis.isEmpty()) {
                return Optional.empty();
            }
            BigDecimal millis = localMillis.get();
            BigInteger bound;
            ComparisonOperator instantOperator;
            switch (operator) {
                // wall >= c  <=>  wall >= ceil(c)  <=>  instant >= thresholdGE(ceil(c))
                case GREATER_THAN_OR_EQUAL -> {
                    bound = ceilToBigInteger(millis);
                    instantOperator = GREATER_THAN_OR_EQUAL;
                }
                // wall < c  <=>  wall < ceil(c)  <=>  instant < thresholdGE(ceil(c))
                case LESS_THAN -> {
                    bound = ceilToBigInteger(millis);
                    instantOperator = LESS_THAN;
                }
                // wall <= c  <=>  wall < floor(c) + 1  <=>  instant < thresholdGE(floor(c) + 1)
                case LESS_THAN_OR_EQUAL -> {
                    bound = floorToBigInteger(millis).add(BigInteger.ONE);
                    instantOperator = LESS_THAN;
                }
                // wall > c  <=>  wall >= floor(c) + 1  <=>  instant >= thresholdGE(floor(c) + 1)
                case GREATER_THAN -> {
                    bound = floorToBigInteger(millis).add(BigInteger.ONE);
                    instantOperator = GREATER_THAN_OR_EQUAL;
                }
                default -> {
                    return Optional.empty();
                }
            }
            if (!isWallClockZone(zone)) {
                return Optional.empty();
            }
            // Asia/Shanghai only: the threshold's ambiguity window must lie entirely in the era after its
            // last offset change (September 1991). Fixed +08:00 zones never changed offset and need no guard.
            if (WALL_CLOCK_ZONE.equals(zone) && bound.compareTo(SHANGHAI_MONOTONE_BOUND) < 0) {
                return Optional.empty();
            }
            return epochMillisAtOrAbove(zone, bound)
                    .flatMap(instant -> invertMillis(chain, instantOperator, BigDecimal.valueOf(instant)));
        }

        // invert() for a threshold that already is an instant in epoch millis, bypassing the constant decoding
        private Optional<Expression> invertMillis(Chain chain, ComparisonOperator operator, BigDecimal millis)
        {
            return switch (chain) {
                case IntChain intChain -> invertInt(intChain, operator, millis);
                case DoubleChain doubleChain -> invertDouble(doubleChain, operator, millis);
                case NanosChain ignored -> Optional.empty();
            };
        }

        // The wall clock of a timestamp constant, in epoch millis: both the short (precision <= 6) and the long
        // representation hold epoch microseconds.
        private static Optional<BigDecimal> exactLocalMillis(Constant constant)
        {
            if (!(constant.type() instanceof TimestampType type) || constant.value() == null) {
                return Optional.empty();
            }
            if (type.isShort()) {
                return Optional.of(BigDecimal.valueOf((Long) constant.value()).scaleByPowerOfTen(-3));
            }
            LongTimestamp timestamp = (LongTimestamp) constant.value();
            return Optional.of(BigDecimal.valueOf(timestamp.getEpochMicros())
                    .scaleByPowerOfTen(-3)
                    .add(BigDecimal.valueOf(timestamp.getPicosOfMicro()).scaleByPowerOfTen(-9)));
        }

        // Smallest instant millis whose wall clock in the zone is at or after the millisecond grid point k, that
        // is the smallest t with t + offset(t) >= k. The zone has a fixed offset, so the wall clock is monotone
        // in the instant and a binary search over [k - 24h, k + 24h] finds it exactly; outside the window the
        // comparison is decided because every zone offset is bounded by ±18h.
        private static Optional<Long> epochMillisAtOrAbove(TimeZoneKey zone, BigInteger k)
        {
            long target;
            try {
                target = k.longValueExact();
            }
            catch (ArithmeticException e) {
                return Optional.empty();
            }
            ZoneRules rules = zone.getZoneId().getRules();
            try {
                long low = Math.subtractExact(target, AMBIGUITY_WINDOW_MILLIS);
                long high = Math.addExact(target, AMBIGUITY_WINDOW_MILLIS);
                if (wallClockMillis(rules, high) < target) {
                    return Optional.empty();
                }
                while (low < high) {
                    long middle = low + Math.floorDiv(high - low, 2L);
                    if (wallClockMillis(rules, middle) >= target) {
                        high = middle;
                    }
                    else {
                        low = middle + 1;
                    }
                }
                return Optional.of(low);
            }
            catch (ArithmeticException e) {
                return Optional.empty();
            }
        }

        private static long wallClockMillis(ZoneRules rules, long epochMillis)
        {
            return Math.addExact(epochMillis, offsetMillis(rules, epochMillis));
        }

        private static long offsetMillis(ZoneRules rules, long epochMillis)
        {
            return rules.getOffset(Instant.ofEpochMilli(epochMillis)).getTotalSeconds() * 1000L;
        }

        private Optional<Expression> invert(Chain chain, ComparisonOperator operator, Constant constant)
        {
            return switch (chain) {
                case IntChain intChain -> invertInt(intChain, operator, exactEpochMillis(constant));
                case DoubleChain doubleChain -> invertDouble(doubleChain, operator, exactEpochMillis(constant));
                case NanosChain nanosChain -> invertNanos(nanosChain, operator, constant);
            };
        }

        // exact epoch millis of the constant as a decimal (short: packed long; long: millis + picosOfMilli / 10^9)
        private static BigDecimal exactEpochMillis(Constant constant)
        {
            TimestampWithTimeZoneType type = (TimestampWithTimeZoneType) constant.type();
            if (type.isShort()) {
                return BigDecimal.valueOf(unpackMillisUtc((Long) constant.value()));
            }
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) constant.value();
            return BigDecimal.valueOf(timestamp.getEpochMillis())
                    .add(BigDecimal.valueOf(timestamp.getPicosOfMilli()).scaleByPowerOfTen(-9));
        }

        // smallest bigint n with trunc(n / c) >= k; bigint division truncates toward zero
        private static BigInteger lowerBound(long c, BigInteger k)
        {
            BigInteger divisor = BigInteger.valueOf(c);
            BigInteger base = divisor.multiply(k);
            return k.signum() > 0 ? base : base.subtract(divisor).add(BigInteger.ONE);
        }

        private static BigInteger ceilDiv1000(BigDecimal millis)
        {
            return millis.divide(THOUSAND).setScale(0, RoundingMode.CEILING).toBigIntegerExact();
        }

        private static BigInteger floorDiv1000(BigDecimal millis)
        {
            return millis.divide(THOUSAND).setScale(0, RoundingMode.FLOOR).toBigIntegerExact();
        }

        private Optional<Expression> bigintConstant(BigInteger value)
        {
            try {
                return Optional.of(new Constant(BIGINT, value.longValueExact()));
            }
            catch (ArithmeticException e) {
                return Optional.empty();
            }
        }

        private Optional<Expression> intComparison(ComparisonOperator operator, Reference leaf, BigInteger bound)
        {
            return bigintConstant(bound)
                    .map(constant -> comparison(metadata, getCharVarcharCoercion(session), operator, leaf, constant));
        }

        private Optional<Expression> invertInt(IntChain chain, ComparisonOperator operator, BigDecimal millis)
        {
            Reference leaf = chain.leaf();
            long c = chain.divisor();
            BigInteger qCeil = ceilDiv1000(millis);
            BigInteger qFloor = floorDiv1000(millis);
            return switch (operator) {
                // f(n) >= m ⟺ q >= ceil(m/1000); f(n) < m ⟺ q < ceil(m/1000)
                case GREATER_THAN_OR_EQUAL -> intComparison(GREATER_THAN_OR_EQUAL, leaf, lowerBound(c, qCeil));
                case LESS_THAN -> intComparison(LESS_THAN, leaf, lowerBound(c, qCeil));
                // f(n) <= m ⟺ q <= floor(m/1000); f(n) > m ⟺ q >= floor(m/1000) + 1
                case LESS_THAN_OR_EQUAL -> intComparison(LESS_THAN, leaf, lowerBound(c, qFloor.add(BigInteger.ONE)));
                case GREATER_THAN -> intComparison(GREATER_THAN_OR_EQUAL, leaf, lowerBound(c, qFloor.add(BigInteger.ONE)));
                case EQUAL -> {
                    if (millis.remainder(THOUSAND).compareTo(BigDecimal.ZERO) != 0) {
                        // output is always a whole second: equality with a fractional constant is never true.
                        // expressed as an empty range to preserve NULL propagation
                        yield Optional.of(and(
                                comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, 0L)),
                                comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, 0L))));
                    }
                    BigInteger k = millis.divide(THOUSAND).toBigIntegerExact();
                    Optional<Expression> low = intComparison(GREATER_THAN_OR_EQUAL, leaf, lowerBound(c, k));
                    Optional<Expression> high = intComparison(LESS_THAN, leaf, lowerBound(c, k.add(BigInteger.ONE)));
                    if (low.isEmpty() || high.isEmpty()) {
                        yield Optional.empty();
                    }
                    yield Optional.of(and(low.get(), high.get()));
                }
                case NOT_EQUAL -> {
                    if (millis.remainder(THOUSAND).compareTo(BigDecimal.ZERO) != 0) {
                        // always true for non-NULL input; NULL-safe disjunction covers the whole domain
                        yield Optional.of(or(
                                comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, 0L)),
                                comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, 0L))));
                    }
                    BigInteger k = millis.divide(THOUSAND).toBigIntegerExact();
                    Optional<Expression> low = intComparison(LESS_THAN, leaf, lowerBound(c, k));
                    Optional<Expression> high = intComparison(GREATER_THAN_OR_EQUAL, leaf, lowerBound(c, k.add(BigInteger.ONE)));
                    if (low.isEmpty() || high.isEmpty()) {
                        yield Optional.empty();
                    }
                    yield Optional.of(or(low.get(), high.get()));
                }
                case IDENTICAL -> Optional.empty();
            };
        }

        private Optional<Expression> invertDouble(DoubleChain chain, ComparisonOperator operator, BigDecimal millis)
        {
            // from_unixtime output is integer millis; snap the constant to that grid before inverting:
            // y >= m ⟺ y >= ceil(m);  y < m ⟺ y < ceil(m);  y <= m ⟺ y < floor(m)+1;  y > m ⟺ y >= floor(m)+1
            BigInteger k;
            ComparisonOperator gridOperator;
            switch (operator) {
                case GREATER_THAN_OR_EQUAL -> {
                    k = ceilToBigInteger(millis);
                    gridOperator = GREATER_THAN_OR_EQUAL;
                }
                case LESS_THAN -> {
                    k = ceilToBigInteger(millis);
                    gridOperator = LESS_THAN;
                }
                case LESS_THAN_OR_EQUAL -> {
                    k = floorToBigInteger(millis).add(BigInteger.ONE);
                    gridOperator = LESS_THAN;
                }
                case GREATER_THAN -> {
                    k = floorToBigInteger(millis).add(BigInteger.ONE);
                    gridOperator = GREATER_THAN_OR_EQUAL;
                }
                case EQUAL -> {
                    if (millis.stripTrailingZeros().scale() > 0) {
                        // never true for non-NULL input; empty range preserves NULL propagation
                        return Optional.of(and(
                                comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, chain.leaf(), zeroOf(chain.leaf())),
                                comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, chain.leaf(), zeroOf(chain.leaf()))));
                    }
                    k = millis.toBigIntegerExact();
                    gridOperator = EQUAL;
                }
                case NOT_EQUAL -> {
                    if (millis.stripTrailingZeros().scale() > 0) {
                        // always true for non-NULL input; whole-domain disjunction preserves NULL propagation
                        return Optional.of(or(
                                comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, chain.leaf(), zeroOf(chain.leaf())),
                                comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, chain.leaf(), zeroOf(chain.leaf()))));
                    }
                    k = millis.toBigIntegerExact();
                    gridOperator = NOT_EQUAL;
                }
                default -> {
                    return Optional.empty();
                }
            }
            // The exact real-arithmetic thresholds t = d*(k∓0.5)/1000 are only an initial guess: the actual
            // computation is step-by-step IEEE 754 (division, multiplication, then from_unixtime's rounding),
            // and double rounding can shift the outcome at half-millis boundaries. So each threshold is
            // adjusted against the real floating-point semantics by construction: thresholdGE(k) is the
            // smallest leaf value whose chain evaluates to >= k, verified by direct evaluation. Monotonicity
            // makes this exact.
            Reference leaf = chain.leaf();
            if (chain.leafIsInteger()) {
                Optional<Long> bLow = intThresholdGE(chain, k);
                return switch (gridOperator) {
                    case GREATER_THAN_OR_EQUAL -> bLow.map(b -> comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, b)));
                    case LESS_THAN -> bLow.map(b -> comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, b)));
                    case EQUAL, NOT_EQUAL -> {
                        Optional<Long> bHigh = intThresholdGE(chain, k.add(BigInteger.ONE));
                        if (bLow.isEmpty() || bHigh.isEmpty()) {
                            yield Optional.empty();
                        }
                        yield gridOperator == EQUAL
                                ? Optional.of(and(
                                comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, bLow.get())),
                                comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, bHigh.get()))))
                                : Optional.of(or(
                                comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, bLow.get())),
                                comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, bHigh.get()))));
                    }
                    default -> Optional.empty();
                };
            }
            Optional<Double> bLow = doubleThresholdGE(chain, k);
            Optional<Expression> result = switch (gridOperator) {
                case GREATER_THAN_OR_EQUAL -> bLow.map(b -> comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(DOUBLE, b)));
                case LESS_THAN -> bLow.map(b -> comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(DOUBLE, b)));
                case EQUAL, NOT_EQUAL -> {
                    Optional<Double> bHigh = doubleThresholdGE(chain, k.add(BigInteger.ONE));
                    if (bLow.isEmpty() || bHigh.isEmpty()) {
                        yield Optional.empty();
                    }
                    yield gridOperator == EQUAL
                            ? Optional.of(and(
                            comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(DOUBLE, bLow.get())),
                            comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(DOUBLE, bHigh.get()))))
                            : Optional.of(or(
                            comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(DOUBLE, bLow.get())),
                            comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(DOUBLE, bHigh.get()))));
                }
                default -> Optional.empty();
            };
            // NaN: the original chain maps NaN to epoch millis 0 (from_unixtime(NaN) = 0), while every
            // rewritten double comparison is false for NaN. When millis 0 satisfies the comparison,
            // keep NaN rows via an explicit is_nan disjunct so the rewrite is exact.
            if (result.isPresent() && nanSatisfies(gridOperator, k)) {
                result = Optional.of(or(result.get(), isNan(leaf)));
            }
            return result;
        }

        private static boolean nanSatisfies(ComparisonOperator operator, BigInteger k)
        {
            return switch (operator) {
                case GREATER_THAN_OR_EQUAL -> k.signum() <= 0;   // 0 >= k
                case LESS_THAN -> k.signum() > 0;                // 0 < k
                case EQUAL -> k.signum() == 0;                   // 0 = k
                case NOT_EQUAL -> k.signum() != 0;               // 0 <> k
                default -> false;
            };
        }

        private Call isNan(Reference leaf)
        {
            ResolvedFunction isNan = metadata.resolveBuiltinFunction(getCharVarcharCoercion(session), "is_nan", ImmutableList.of(DOUBLE));
            return new Call(isNan, ImmutableList.of(leaf));
        }

        // the chain's millis output evaluated with the actual floating point operations the engine performs,
        // including from_unixtime's half-away-from-zero rounding of a half millisecond
        private long evalChainMillis(DoubleChain chain, double leafValue)
        {
            return unixTimeToEpochMillis(leafValue / chain.divisor().doubleValue());
        }

        // Mirrors DateTimeFunctions.unixTimeToEpochMillis: round half away from zero, treating unixTime as a
        // tie when it is the double nearest a half millisecond. A negative exact tie therefore rounds down
        // (-0.0625 s is -63 ms, not -62 ms), unlike Math.round. This must stay in sync with the engine:
        // the adjusted thresholds are verified through this evaluator, so a divergence silently turns the
        // rewrite into a different predicate at the tie values.
        private static long unixTimeToEpochMillis(double unixTime)
        {
            double millis = unixTime * 1000;
            if (Math.abs(millis) >= 0x1p52) {
                // Integral and too large to pack; the saturating cast keeps it so
                return (long) millis;
            }
            double floor = Math.floor(millis);
            double fraction = millis - floor;
            if (Math.abs(fraction - 0.5) > 2 * Math.ulp(millis)) {
                return Math.round(millis);
            }
            long lowerMillis = (long) floor;
            double halfMillisecond = (2 * lowerMillis + 1) / 2000.0;
            if (unixTime > halfMillisecond || (unixTime == halfMillisecond && unixTime > 0)) {
                return lowerMillis + 1;
            }
            return lowerMillis;
        }

        // smallest bigint n whose chain output is >= k, found by adjusting the analytic guess against
        // real floating point evaluation; empty if the bound does not fit bigint or fails to converge
        private Optional<Long> intThresholdGE(DoubleChain chain, BigInteger k)
        {
            BigDecimal guess = chain.divisor().multiply(new BigDecimal(k).subtract(HALF)).divide(THOUSAND);
            try {
                return adjustIntThresholdGE(n -> evalChainMillis(chain, n), guess, k.longValueExact());
            }
            catch (ArithmeticException e) {
                return Optional.empty();
            }
        }

        // smallest double x whose chain output is >= k, same verify-and-adjust construction
        private Optional<Double> doubleThresholdGE(DoubleChain chain, BigInteger k)
        {
            BigDecimal guess = chain.divisor().multiply(new BigDecimal(k).subtract(HALF)).divide(THOUSAND);
            try {
                return adjustDoubleThresholdGE(x -> evalChainMillis(chain, x), guess, k.longValueExact());
            }
            catch (ArithmeticException e) {
                return Optional.empty();
            }
        }

        private static Constant zeroOf(Reference leaf)
        {
            return leaf.type().equals(BIGINT) ? new Constant(BIGINT, 0L) : new Constant(DOUBLE, 0.0);
        }

        private static BigInteger floorToBigInteger(BigDecimal value)
        {
            return value.setScale(0, RoundingMode.FLOOR).toBigIntegerExact();
        }

        private static BigInteger ceilToBigInteger(BigDecimal value)
        {
            return value.setScale(0, RoundingMode.CEILING).toBigIntegerExact();
        }

        private Optional<Expression> invertNanos(NanosChain chain, ComparisonOperator operator, Constant constant)
        {
            TimestampWithTimeZoneType type = (TimestampWithTimeZoneType) constant.type();
            long epochMillis;
            int picosOfMilli;
            if (type.isShort()) {
                epochMillis = unpackMillisUtc((Long) constant.value());
                picosOfMilli = 0;
            }
            else {
                LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) constant.value();
                epochMillis = timestamp.getEpochMillis();
                picosOfMilli = timestamp.getPicosOfMilli();
            }
            BigInteger nanos = BigInteger.valueOf(epochMillis).multiply(BigInteger.valueOf(1_000_000))
                    .add(BigInteger.valueOf(picosOfMilli / 1000));
            boolean fraction = picosOfMilli % 1000 != 0;
            Reference leaf = chain.leaf();
            try {
                if (!fraction) {
                    Constant bound = new Constant(BIGINT, nanos.longValueExact());
                    return switch (operator) {
                        case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> Optional.of(comparison(metadata, getCharVarcharCoercion(session), operator, leaf, bound));
                        case IDENTICAL -> Optional.empty();
                    };
                }
                // sub-nanosecond constant: tighten toward the satisfying side
                return switch (operator) {
                    case GREATER_THAN_OR_EQUAL, GREATER_THAN -> intComparison(GREATER_THAN_OR_EQUAL, leaf, nanos.add(BigInteger.ONE));
                    case LESS_THAN, LESS_THAN_OR_EQUAL -> intComparison(LESS_THAN_OR_EQUAL, leaf, nanos);
                    case EQUAL -> Optional.of(and(
                            comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, 0L)),
                            comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, 0L))));
                    case NOT_EQUAL -> Optional.of(or(
                            comparison(metadata, getCharVarcharCoercion(session), LESS_THAN, leaf, new Constant(BIGINT, 0L)),
                            comparison(metadata, getCharVarcharCoercion(session), GREATER_THAN_OR_EQUAL, leaf, new Constant(BIGINT, 0L))));
                    case IDENTICAL -> Optional.empty();
                };
            }
            catch (ArithmeticException e) {
                return Optional.empty();
            }
        }
    }
}
