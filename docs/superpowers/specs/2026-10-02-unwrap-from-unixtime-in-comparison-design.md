# UnwrapFromUnixTimeInComparison 优化器规则设计

日期：2026-10-02
状态：已批准（设计阶段）；本次修订收窄为固定模式的比较边界反算

## 背景与目标

Metabase 自动生成的 SQL 常把 bigint epoch 毫秒列转换为时间，再做范围过滤：

```sql
SELECT ...
FROM (
    SELECT from_unixtime(ts / 1000, 'Asia/Shanghai') AS ts, ...
    FROM iceberg.prod.cls_logs
) AS virtual_table
WHERE ts >= TIMESTAMP '2026-09-01 00:00:00.000000'
  AND ts <  TIMESTAMP '2026-10-01 00:00:00.000000'
```

表结构不能改。首版新增 `UnwrapFromUnixTimeInComparison`，识别这一固定模式，把列侧的转换反算到常量侧，生成 bigint 列与常量的范围比较，使 connector 有机会恢复谓词下推和文件裁剪。

实现流程只有三步：识别支持的函数链 → 反算常量边界 → 重建比较。无法满足适用条件时保留原表达式。

## 首版范围

- 源值是 bigint 列引用，先执行 bigint 整数除法 `/ 1000`，再转为 double 传入 `from_unixtime`。
- 支持单参数 `from_unixtime(x)` 和两参数 `from_unixtime(x, zone)`；两参数的 zone 必须是可确定的、非 NULL、有效的 varchar 时区常量。
- 函数链位于左侧时，只支持 `>=` 和 `<`；位于右侧时先翻转比较符，例如 `T <= F` 对应 `F >= T`。
- 比较的另一侧必须能确定性求值为非 NULL 时间戳常量，且在完成原有类型、时区转换后对齐整秒。
- 支持函数结果外部仅提升精度的 `timestamp with time zone` CAST，以覆盖示例中的六位小数时间戳常量。
- 保留 SQL 的 NULL 传播语义，并正确处理 epoch 之前的负值和零附近的整数除法。

首版不支持：

- `=`、`<>`、链在左侧的 `>`、`<=`，以及 `IS DISTINCT FROM` 等其他比较。
- 专门展开 `BETWEEN`、`IN`。若现有优化器已产生独立比较，只对满足本规则条件的子表达式改写。
- DOUBLE、REAL、DECIMAL 源列或算术；`/ 1000.0`、`CAST(ts AS double) / 1000`、`/ 1`、其他除数、乘法、多层算术，以及不带 `/ 1000` 的调用。
- 非整秒常量边界，包括非零毫秒、微秒、纳秒或皮秒部分。
- 外层 `CAST(... AS timestamp(p))`（无时区），以及降低带时区时间戳精度的 CAST。
- `TRY_CAST`、JSON 提取或任意复杂源表达式；非常量、NULL 或无效时区。
- `from_unixtime_nanos`、三参数 `from_unixtime`，以及通用函数求逆框架。

原始 JSON 变体包含复杂源表达式和外层无时区 CAST，留待后续独立扩展。它仍需逐行解析 JSON，不能承诺获得真实 epoch 列同样的文件裁剪收益。

## 匹配与类型语义

SQL 分析后，典型函数链的逻辑形态为：

```text
[仅提升精度的 CAST AS timestamp(p) with time zone]
  from_unixtime(
    CAST(bigint_column / BIGINT '1000' AS double)
    [, 有效的常量时区]
  )
```

实现按已解析的内置函数、运算符签名和 IR 类型匹配，不按 SQL 文本匹配：

1. 除法必须是 bigint 除法，左操作数为 bigint 列引用，右操作数折叠后为 bigint 常量 1000。
2. double CAST 必须作用于该整数除法的结果；不把任意 bigint → double CAST 当作全域无损转换。
3. 可穿过零到多层仅提升精度的带时区时间戳 CAST；不得穿过无时区、降精度或其他 CAST。
4. 常量侧沿用原比较的类型转换，并经 `IrExpressionOptimizer` 折叠；求值失败、仍非常量或值为 NULL 时不改写。
5. 时区参数须验证有效性。即使时区不影响最终瞬时比较，也不能删除无效时区的报错或 NULL 时区的 NULL 语义。

`from_unixtime` 返回 `timestamp(3) with time zone`。与 `timestamp(6)` 常量比较时，共同类型可以是 `timestamp(6) with time zone`，因此函数侧也可能存在精度提升 CAST，不能只考虑常量侧转换。

无时区常量按 **session 时区** 转成带时区常量；不能用 `from_unixtime` 的 zone 参数重新解释它。带时区时间戳比较的是瞬时值；本规则没有墙钟时间 CAST，因而不需要求墙钟时间在夏令时切换处的逆映射。

读取常量时必须检查完整精度。短表示解包 epoch 毫秒；长表示同时检查毫秒内的皮秒部分。仅当亚毫秒部分为零且 epoch 毫秒是 1000 的整数倍时继续。不能先截断或舍入到毫秒再判断。

## 比较边界反算

设列值为 `n`，原比较中已完成转换的时间常量为 `T`，对应整秒 epoch 值 `k`，其 epoch 毫秒 `m = 1000 * k`。

### 先保留整数除法语义

原链实际执行：

```text
q = truncTowardZero(n / 1000)       // bigint 除法，向零截断
resultMillis = Math.round((double) q * 1000)
```

对于原函数能够正常返回的输入，`q` 及 `1000 * q` 均可被 double 精确表示，因此 `resultMillis = 1000 * q`。这里没有一般 DOUBLE 输入的 ±0.5ms 边界问题。

这不意味着 `resultMillis = n`。例如 `n = 1001` 时，结果是 1000ms；整数除法丢失的毫秒部分必须在逆推时保留为商区间。

### 再反算列的阈值

定义整数商达到 `k` 的最小 bigint 值：

```text
L(k) = 1000 * k         当 k > 0
L(k) = 1000 * k - 999   当 k <= 0
```

由向零截断的定义可得：

| 原比较（链在左） | 商的条件 | 新的列比较 |
|---|---|---|
| `from_unixtime(n / 1000, ...) >= T` | `q >= k` | `n >= L(k)` |
| `from_unixtime(n / 1000, ...) < T` | `q < k` | `n < L(k)` |

边界示例：

| `T` 对应的 epoch 秒 `k` | `L(k)` | 说明 |
|---|---|---|
| 1 | 1000 | 现代正 epoch 边界同样直接得到 `m` |
| 0 | -999 | `-999 / 1000 = 0`，不能错误改成边界 0 |
| -1 | -1999 | `-1999 / 1000 = -1`，负数需保留向零截断语义 |

因此示例中的 2026 年时间窗口可改写为：

```sql
WHERE ts >= <按原比较语义折叠的起始 epoch 毫秒>
  AND ts <  <按原比较语义折叠的结束 epoch 毫秒>
```

边界只用整数运算计算，生成 bigint 常量；转换、乘法或减法若无法安全完成，则不改写。NULL 列值在原比较和新比较中均返回 NULL。

## 组件与改动点

1. 新增 `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UnwrapFromUnixTimeInComparison.java`，继承 `ExpressionRewriteRuleSet`，参照 `UnwrapYearInComparison` 使用现有比较识别、翻转和构造接口。只实现上述固定模式，不新增通用求逆抽象。
2. 在 `core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java` 中，与现有 unwrap 规则同组注册，使改写后的列比较进入现有谓词下推流程。
3. 新增 `core/trino-main/src/test/java/io/trino/sql/planner/iterative/rule/TestUnwrapFromUnixTimeInComparison.java`，覆盖改写形态、不改写条件和结果等价性。
4. 增加完整 SQL 的 planner 测试，覆盖子查询展开、六位小数常量带来的精度提升 CAST，以及扫描端实际得到的 bigint 范围约束。

## 测试与验收

### 等价性与改写形态

- 两种受支持比较，函数链在左及翻转后的右侧形式。
- `k = -1、0、1` 和真实 2026 年边界；每个阈值取 `L(k)-1、L(k)、L(k)+1`，并覆盖各整数商区间内部值。
- NULL 列值、负 epoch、边界附近的毫秒值；比较原表达式和改写表达式的执行结果，而不只断言生成的 IR。
- 单参数、有效常量时区参数；不同 session 时区，尤其 session 时区与函数时区参数不一致的情况。
- 整秒值的 `timestamp(3/6/9/12) with time zone` 常量，以及示例中的 `timestamp(6)` 常量；确认保留正确的原比较时区语义并处理精度提升 CAST。
- 对确定性常量表达式折叠后匹配；对 NULL、非常量、非确定性或求值失败的常量侧保留原式。

### 明确不改写的回归用例

- `ts / 1000 = 1` 等未支持比较；不误改成 `ts = 1000`。
- DOUBLE 浮点反例 `Math.nextDown(0.0585)`，以及其他不支持的源类型和算术形态。
- 亚毫秒常量、非整秒毫秒常量，以及降精度 CAST。
- 外层无时区 CAST：包括 `timestamp(0)` 的额外舍入和 `America/New_York` 夏令时回拨时的重复墙钟时间。
- JSON / TRY_CAST 源表达式、无效或 NULL 时区、非常量时区，以及其他不支持的函数重载。

### 下推与收益

- 对背景中的完整 Metabase SQL，确认扫描端获得正确的 bigint 范围约束；不能仅以 Filter 节点靠近 TableScan 作为下推证明。
- 在代表性 Iceberg/Hive 表上核对结果一致性，并记录扫描文件数、扫描字节和耗时的变化。收益取决于 connector 支持和文件统计信息，不预设加速倍数。
- 若查询仍投影转换后的时间列，该投影仍可执行 `from_unixtime`；本规则消除的是受支持过滤条件中的函数链。

## 沿用的语义偏差

沿用此前已接受的 fork 内部约定：源 bigint 值可能使原 `from_unixtime` 超出带时区时间戳范围并抛出 `INVALID_FUNCTION_ARGUMENT`；删除函数后，新比较可能返回布尔值。在过滤场景中，该行可能被保留或被排除，不能笼统描述为“被过滤掉”。

该偏差须写入规则 javadoc，并通过专门用例记录；正常可表示输入必须满足上述结果等价性。无效时区等其他错误不在接受范围内，仍应保留原式。

首版没有 DOUBLE 源值，NaN 偏差不再适用。若以后扩大输入类型，需重新审查浮点、NaN、舍入和错误语义。

## 后续独立任务

- 根据真实查询需求扩展等值比较、其他范围比较、非整秒边界或 JSON 链，每项均单独定义反算规则和等价性测试。
- SQLFluff lint 与内部 SQL 规范仍可独立交付，不作为本规则落地的前置条件。
