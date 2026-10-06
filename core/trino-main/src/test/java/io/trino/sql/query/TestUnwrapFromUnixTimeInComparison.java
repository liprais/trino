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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runtime behavior of {@code UnwrapFromUnixTimeInComparison}: error cases must still fail at
 * execution time with the original error code, on column input that is actually consumed per row.
 * (Expression-level rewrite coverage lives in {@code io.trino.sql.planner.iterative.rule}.)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUnwrapFromUnixTimeInComparison
{
    private final QueryAssertions assertions = new QueryAssertions();

    @BeforeAll
    public void setup()
    {
        assertions.getQueryRunner().installPlugin(new TpchPlugin());
        assertions.getQueryRunner().createCatalog("tpch", "tpch", ImmutableMap.of());
    }

    @AfterAll
    public void close()
    {
        assertions.close();
    }

    @Test
    public void testInvalidZoneFailsAtRuntime()
    {
        String filter = "from_unixtime(ts / 1000, 'Invalid/Zone') >= TIMESTAMP '2026-01-01 00:00:00 UTC'";
        // planning succeeds and the plan still contains the function (no rewrite, no folding),
        // so the failure below happens at execution time
        assertThat(explainText("SELECT count(*) FROM (VALUES (1700000000000)) t(ts) WHERE " + filter))
                .contains("from_unixtime");
        // (the two-arg zone overload reports NOT_SUPPORTED, not INVALID_FUNCTION_ARGUMENT)
        assertThat(assertions.query("SELECT count(*) FROM (VALUES (1700000000000)) t(ts) WHERE " + filter))
                .failure()
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testDivisionByZeroFailsAtRuntime()
    {
        String filter = "from_unixtime(ts / 0) >= TIMESTAMP '2026-01-01 00:00:00 UTC'";
        assertThat(explainText("SELECT count(*) FROM (VALUES (1700000000000)) t(ts) WHERE " + filter))
                .contains("from_unixtime");
        assertThat(assertions.query("SELECT count(*) FROM (VALUES (1700000000000)) t(ts) WHERE " + filter))
                .failure()
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testRuleToggle()
    {
        // a real table scan so the predicate cannot be constant-folded away
        String sql = "SELECT count(*) FROM tpch.tiny.orders " +
                "WHERE from_unixtime(orderkey / 1000) >= TIMESTAMP '2026-01-01 00:00:00 UTC'";
        // enabled by default: the filter is rewritten and from_unixtime disappears from the plan
        assertThat(explainText(assertions.getDefaultSession(), sql)).doesNotContain("from_unixtime");
        // disabled via session property: the chain stays
        Session disabled = Session.builder(assertions.getDefaultSession())
                .setSystemProperty("unwrap_from_unixtime_in_comparison_enabled", "false")
                .build();
        assertThat(explainText(disabled, sql)).contains("from_unixtime");
    }

    private String explainText(String sql)
    {
        return explainText(assertions.getDefaultSession(), sql);
    }

    private String explainText(Session session, String sql)
    {
        return assertions.execute(session, "EXPLAIN " + sql)
                .getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList())
                .toString();
    }

    @Test
    public void testInfinityInputNoLongerFails()
    {
        // accepted deviation, recorded: pre-patch this fails with INVALID_FUNCTION_ARGUMENT (millis
        // overflow in packing); the rewritten comparison keeps the row
        assertThat(assertions.query(
                "SELECT count(*) FROM (VALUES (infinity())) t(x) " +
                        "WHERE from_unixtime(x) >= TIMESTAMP '2026-01-01 00:00:00 UTC'"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testNullAndNanInput()
    {
        // NULL: both forms filter the row out
        assertThat(assertions.query(
                "SELECT count(*) FROM (VALUES (CAST(NULL AS double))) t(x) " +
                        "WHERE from_unixtime(x) >= TIMESTAMP '1970-01-01 00:00:00 UTC'"))
                .matches("VALUES BIGINT '0'");

        // NaN: exact — the original maps NaN to epoch 0 and keeps the row; the rewrite preserves this
        // via an explicit is_nan disjunct
        assertThat(assertions.query(
                "SELECT count(*) FROM (VALUES (nan())) t(x) " +
                        "WHERE from_unixtime(x) >= TIMESTAMP '1970-01-01 00:00:00 UTC'"))
                .matches("VALUES BIGINT '1'");
    }
}
