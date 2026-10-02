# UnwrapFromUnixTimeInComparison 优化器规则设计

日期：2026-10-02
状态：已批准（设计阶段）

## 背景与问题

分析师（主要通过 Metabase 自动生成 SQL）习惯把 epoch 列转成时间再过滤，典型形态：

```sql
SELECT ...
FROM (
    SELECT from_unixtime(ts/1000, 'Asia/Shanghai') AS ts, ...
    FROM iceberg.prod.cls_logs
) AS virtual_table
WHERE ts >= TIMESTAMP '2026-09-01 00:00:00.000000'
  AND ts <  TIMESTAMP '2026-10-01 00:00:00.000000'
```

以及更糟的 JSON 变体：

```sql
CAST(from_unixtime(
    TRY_CAST(TRY_CAST(json_extract_scalar(props, '$["$receive_time"]') AS DOUBLE) AS BIGINT) / 1000,
    'Asia/Shanghai') AS TIMESTAMP)
```

谓词作用在函数链上，优化器无法把它转换成"列 vs 常量"，导致：

- Iceberg/Hive 的 min/max 文件裁剪失效；
- connector 谓词下推失效；
- 只能全量扫描后逐行过滤。

表结构不能改（已确认），因此解法是在 Trino 优化器中新增等价改写规则。

## 目标

新增迭代优化器规则 `UnwrapFromUnixTimeInComparison`，把

```
from_unixtime 链(表达式) ⊕ 常量时间戳
```

逐层逆转为

```
表达式 ⊕' 常量边界
```

使谓词恢复"列 vs 常量"形态。末端是真列时恢复下推/裁剪（Iceberg 场景）；末端是 JSON 提取等表达式时至少消除整条链的逐行函数开销。

## 非目标

- 不处理 `from_unixtime_nanos`；
- 不处理三参数 `from_unixtime(x, hours, minutes)`；
- 不逆转 `TRY_CAST` / `json_extract_scalar` 层（剥到那里为止，直接以该表达式作为比较左侧）；
- 不改表结构、不做 Metabase 侧改造。

## 语义基础

单参数与两参数时区变体的实现（`DateTimeFunctions.java:144`、`:172`）：

```java
packDateTimeWithZone(Math.round(unixTime * 1000), zone)
```

即结果为 `timestamp(3) with time zone`，epoch 毫秒 = `Math.round(x * 1000)`。`timestamp with time zone` 之间的比较只比 epoch 毫秒，与时区无关。

与 `TIMESTAMP`（无时区）常量比较时，Trino 按 **session 时区** 把常量强转为带时区时间戳；该强转发生在常量上，规划阶段即被折叠，规则拿到的边界是确定的毫秒值。

外层 `CAST(... AS timestamp(p))`（无时区）使时区参数产生意义：结果是以该时区渲染的墙钟时间。其逆映射为：边界常量墙钟时间在该常量时区下对应的 epoch 毫秒。

## 匹配模式

规则匹配比较表达式（含函数链在左或右，右侧时翻转比较符），其中一侧是常量，另一侧是以下链：

```
[CAST(... AS timestamp(p))]                     -- 可选，外层强转
  from_unixtime( E [, zone常量 varchar] )        -- 单参数或两参数常量时区
    E = 列引用
      | CAST(E' AS double)                       -- lossless 强转，剥除
      | E' / 正常量 | E' * 正常量                 -- 算术逆转（仅正常量乘除）
```

`BETWEEN` 展开为两个比较；`IN` 展开为等值比较的 OR（仿 `UnwrapYearInComparison.rewriteIn`，要求全部值都能逆转才改写）。

右/左侧必须是确定性可求值常量（经 `IrExpressionOptimizer` 判定）；求值失败、NULL、非常量一律保留原表达式。

## 边界推导

设常量边界对应的 epoch 毫秒为 `m`（整数），`from_unixtime` 内部舍入为 `round(x*1000)`：

| 原比较（链在左） | 逆转后（`x` 为 double 秒） |
|---|---|
| `= T`  | `x >= (m-0.5)/1000 AND x < (m+0.5)/1000` |
| `<>`   | `x < (m-0.5)/1000 OR  x >= (m+0.5)/1000` |
| `>= T` | `x >= (m-0.5)/1000` |
| `> T`  | `x >= (m+0.5)/1000` |
| `<= T` | `x <  (m+0.5)/1000` |
| `< T`  | `x <  (m-0.5)/1000` |

边界用精确小数（BigDecimal 或整数运算）计算，避免 double 中间误差。

**整数末端收紧**：若剥壳后末端是整数类型列（典型：`ts` bigint 毫秒，经 `/1000` 进入函数），`round(ts/1000*1000)` 在 double 精确范围（2^53）内精确等于 `ts`，边界直接收紧为整数：

- `>= T` → `ts >= m`；`> T` → `ts >= m+1`；`<= T` → `ts <= m`；`< T` → `ts <= m-1`
- `= T` → `ts = m`；`<>` → `ts <> m`

整数常量比较最有利于 connector 下推与文件裁剪。

**算术逆转**：`E' / c ⊕ b`（c 为正常量）→ `E' ⊕ b*c`；乘法同理。边界乘除在精确小数域完成后再落成末端类型常量。

## 组件与改动点

1. **新规则类** `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/UnwrapFromUnixTimeInComparison.java`
   - `extends ExpressionRewriteRuleSet`，结构仿 `UnwrapYearInComparison.java:82`；
   - Visitor 内做：识别比较 → 识别链（剥外层 CAST、from_unixtime、lossless cast、算术层）→ 常量求边界 → 重建比较表达式。
2. **注册** `core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java:383` 附近，与现有 unwrap 规则同组。
3. **单元测试** `core/trino-main/src/test/java/io/trino/sql/planner/iterative/rule/TestUnwrapFromUnixTimeInComparison.java`，仿 `TestUnwrapYearInComparison`。
4. **Planner 测试**：对 Metabase 形态的查询（子查询内 `from_unixtime(ts/1000,'Asia/Shanghai')`，外层时间范围过滤）断言改写后谓词下推到 TableScan。

## 测试用例清单

- 六种比较符（`=`、`<>`、`<`、`<=`、`>`、`>=`），链在左 / 在右（翻转）；
- `BETWEEN`、`IN`（含部分值不可逆转时不改写）；
- 末端类型：bigint（整数收紧）、double（±0.5ms 边界）；
- 算术层：`/1000`、`/1`、不支持的形式（如减常量、负乘数）不改写；
- 时区：单参数（session 时区）、两参数常量时区（含外层 `CAST AS TIMESTAMP`）、非常量时区不改写；
- 非常量右侧、NULL 常量、非确定性表达式：不改写；
- 不同 session 时区下边界值正确（`TIMESTAMP` 字面量按 session 时区解释）。

## 已接受的语义偏差

- **超范围输入**：原语义下 `round(x*1000)` 超出 timestamp 毫秒范围会抛 `INVALID_FUNCTION_ARGUMENT`；改写后变成被过滤掉。实际 epoch 列不会触发，作为 fork 内部规则接受此偏差，写入规则 javadoc。
- **NaN 输入**（double 末端）：`Math.round(NaN)` 为 0，原式与 epoch 0 比较；改写后 `NaN >= 常量` 为 false。同上记录。

## 兜底方案（独立交付，不依赖规则落地）

- SQLFluff 自定义 lint：检测"比较谓词一侧为 `from_unixtime(...)` 链、另一侧为常量"的模式，CI 拦截并提示把函数写到常量侧；
- 一页内部 SQL 规范文档。

## 交付顺序

1. 优化器规则 + 测试（主线，本 spec 范围）；
2. lint 兜底（后续独立任务）。
