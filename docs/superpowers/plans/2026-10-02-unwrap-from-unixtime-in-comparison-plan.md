# UnwrapFromUnixTimeInComparison 实现计划

对应 spec：`docs/superpowers/specs/2026-10-02-unwrap-from-unixtime-in-comparison-design.md`（已批准，commit 3baf55a9）

## 已确认的代码事实（实现依据）

- 规则骨架：参照 `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UnwrapYearInComparison.java`，继承 `ExpressionRewriteRuleSet`（`ExpressionRewriteRuleSet.java` 已提供 Filter/Project/Join/Values 等节点的规则集）。
- 注册位置：`core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java:383` 附近，与 `UnwrapYearInComparison` 同组。
- `from_unixtime(double)` 实现为 `Math.round(unixTime * 1000)`（`DateTimeFunctions.java:144`）。
- Trino 整数除法向零截断：`BigintOperators.java:93-103`，`bigint / bigint → bigint`。IR 中除法是 `Call`，名为 `builtinFunctionName(DIVIDE)`（用法见 `IrExpressions.java:532`）。
- 常量折叠：规则内用 `plannerContext.getExpressionOptimizer().process(...)` 折叠常量侧和除数（见 `UnwrapYearInComparison.java:193`）。
- `timestamp with time zone` 常量值：precision ≤ 6 为 short 表示（packed long，epoch 毫秒需解包）；precision ≥ 7 为 `LongTimestampWithTimeZone`（`getEpochMillis()` + `getPicosOfMilli()`）。
- 函数名匹配用 `builtinFunctionName("from_unixtime")`（见 `UnwrapYearInComparison.java:143`）。

## 任务清单

### 任务 1：新规则类 `UnwrapFromUnixTimeInComparison`

文件：`core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UnwrapFromUnixTimeInComparison.java`

结构仿 `UnwrapYearInComparison`：构造函数建 `ExpressionRewriteRuleSet`，内部 Visitor 继承 `io.trino.sql.ir.ExpressionRewriter<Void>`，覆写 `rewriteCall`。核心方法 `tryUnwrapFromUnixTime(ComparisonOperator operator, Expression chainSide, Expression constantSide)`，返回 `Optional<Expression>`，任一条件不满足即返回 empty（保留原表达式）。

**链匹配（chainSide），按顺序剥壳：**

1. 剥外层精度提升 CAST：零到多层 `Cast`，要求源和目标都是 `TimestampWithTimeZoneType` 且目标精度 > 源精度；其他 CAST 一律不匹配。
2. 必须是 `from_unixtime` 的 `Call`，1 或 2 个参数：
   - 2 参数时第二参数必须是 `Constant`（varchar），值非 NULL 且为有效时区（`TimeZoneKey.getTimeZoneKey` 验证，异常则不匹配）；
   - 确认返回类型为 `timestamp(3) with time zone`。
3. 第一参数必须是目标类型为 `DOUBLE` 的 `Cast`，其表达式为：
   - `Call`，名为 `builtinFunctionName(DIVIDE)`，返回类型 `BIGINT`；
   - 左操作数是 `BIGINT` 类型的 `Reference`（列引用）；
   - 右操作数经 `optimizer.process` 折叠后为 `Constant(BIGINT, 1000)`；其他值不匹配。

**常量侧（constantSide）：**

4. 经 `optimizer.process` 折叠；必须是 `Constant` 且类型为 `TimestampWithTimeZoneType`、值非 NULL。NULL 常量不匹配（首版不支持，保留原式）。
5. 提取 epoch 毫秒：short 表示解包 packed long；long 表示取 `getEpochMillis()` 且要求 `getPicosOfMilli() == 0`。要求毫秒值 `m` 满足 `m % 1000 == 0`（整秒），`k = m / 1000`；否则不匹配。

**边界重建：**

6. `L(k) = k > 0 ? 1000*k : 1000*k - 999`，用 `Math.multiplyExact` / `Math.subtractExact` 防守溢出（溢出则不匹配）。
7. 算符映射（链在左）：`GREATER_THAN_OR_EQUAL` → `n >= L(k)`；`LESS_THAN` → `n < L(k)`；其他算符不匹配。链在右时先 `operator.flip()` 再走同一逻辑（参照 `UnwrapYearInComparison.java:132-137`）。
8. 用 `io.trino.sql.ir.Comparison` 直接构造新比较：左为列 `Reference`，右为 `Constant(BIGINT, L(k))`。

**javadoc**：写明已接受的语义偏差——源 bigint 值超出可表示时间戳范围时，原表达式抛 `INVALID_FUNCTION_ARGUMENT`，改写后返回布尔值（行被保留或排除）。

### 任务 2：注册规则

`PlanOptimizers.java`：import 并紧跟 `UnwrapYearInComparison` 一行加入 `.addAll(new UnwrapFromUnixTimeInComparison(plannerContext).rules())`。

### 任务 3：单元测试

文件：`core/trino-main/src/test/java/io/trino/sql/planner/iterative/rule/TestUnwrapFromUnixTimeInComparison.java`，仿 `TestUnwrapYearInComparison`（直接调用规则的 `unwrap` 静态方法做表达式级断言）。

改写断言：
- `>=`、`<` 两种算符，链在左 / 链在右（翻转）；
- `k = -1、0、1`（验证 `L(k) = -1999、-999、1000`）及真实 2026 年边界；
- 单参数与两参数有效时区（如 `'Asia/Shanghai'`、`'+08:00'`）；
- 不同 session 时区下常量侧按 session 时区解释；
- 整秒的 `timestamp(3/6/9/12) with time zone` 常量（含 long 表示）与函数侧精度提升 CAST（多层）。

不改写断言（结果表达式等于原表达式）：
- `=`、`<>`、`>`、`<=`；
- 非整秒常量（毫秒/微秒/皮秒非零）；
- 常量侧 NULL、非常量、非确定性表达式；
- 无效或 NULL 时区、非常量时区；
- 外层无时区 CAST、降精度 CAST；
- `/ 1000.0`、其他除数、乘法、无除法、double 源列；
- TRY_CAST / json_extract_scalar 源表达式。

结果等价性：对若干 `n ∈ {L(k)-1, L(k), L(k)+1, 各商区间内部值, NULL, 负值}`，分别求值原表达式与改写表达式，断言过滤结果一致（用 `IrExpressionOptimizer`/解释器求值，不只比 IR）。

### 任务 4：Planner 测试

在合适的 planner 测试类（参照现有 unwrap 规则的 planner 测试）新增用例：spec 背景中的完整 Metabase SQL 形态（子查询内 `from_unixtime(ts/1000, 'Asia/Shanghai')`，外层 `>=` / `<` 范围过滤），断言：

- Filter 消失或谓词变为 `ts >= <起始毫秒> AND ts < <结束毫秒>`；
- TableScan 的 constraint 携带该 bigint 范围（不能仅以 Filter 靠近 TableScan 为据）。

### 任务 5：验证

- `./mvnw test -pl core/trino-main -Dtest=TestUnwrapFromUnixTimeInComparison` 及新增 planner 测试全绿；
- 回归：`TestUnwrapYearInComparison`、`TestUnwrapCastInComparison` 等既有 unwrap 测试不受影响；
- `./mvnw verify -pl core/trino-main -P ci` 过重时至少跑 checkstyle（`./mvnw checkstyle:check -pl core/trino-main`）。

## 不做（与 spec 非目标一致）

`=`/`<>`/`>`/`<=`、`BETWEEN`/`IN` 专门展开、非整秒边界、double 源列、JSON 链、外层无时区 CAST、`from_unixtime_nanos`、三参数变体、SQLFluff lint（后续独立任务）。
