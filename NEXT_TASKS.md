# NEXT_TASKS.md

## 2026-05-12 笔层回溯后续判断

当前 `src/chan/bi.py` 已加入 `MAX_BI_ROLLBACK = 5` 的有限回溯机制。600497 日线尾部诊断显示：

- 默认规则内已触发 rollback：`rollback_trigger_count=1`。
- 回退 1..5 笔均未能让最后一笔超过 `raw_index=4374`。
- `output/bi_rollback_debug.csv` 中 1..5 次回退的拒绝原因均为 `reject_no_later_coverage`。
- 额外诊断显示第一次可推进尾部的方案需要 `rollback_count=10`，可把最后覆盖推进到 `raw_index=5055`，但这超过当前指定的最大回退 5 笔规则。

下一步如果要让 600497 右侧 `raw_index=4375..5064` 出现 confirmed_bis 覆盖，需要先决定：

1. 是否把最大有限回溯从 5 放宽到至少 10；
2. 或者仍保持 5，但引入更早触发的路径修正机制，而不是等到尾部完全卡死后才回溯。

在规则决定前，不要改 UI，不要在 chart.py 补线，不要放宽成笔硬条件。

下一步暂停新功能开发。包含关系规范化、`standard_bars` 生成、分型识别入口和笔生成入口已经完成本轮重检，不要重复实现。

## 唯一优先级

继续排查真实走势中的疑似漏画笔之前，先沿用本轮已经确认的数据流：

```text
raw_bars -> standard_bars + inclusion_groups -> candidate_fractals -> valid_fractals_for_bi -> confirmed_bis -> chart mapping
```

当前 600497 日线诊断基准：

- `raw_bars_count=5065`
- `standard_bars_count=3795`
- `candidate_fractals_count=1842`
- `confirmed_bis_count=217`
- `continuity_ok=True`
- `extreme_ok=True`
- `center_gap_at_least_4=True`
- `no_shared_standard_bar=True`
- `neutral_bar_between_fractals=True`

下一步优先使用 `output/bi_debug_report.txt`、`output/bi_attempts_debug.csv` 和 `output/suspected_missing_bis.csv` 对照真实图形，确认是否仍有人工意义上的漏笔；如有，再小范围检查 `src/chan/bi.py` 的 `locked_bis + pending_bi + active_bi` 状态机。

必须遵守：

- 不进入线段、中枢、背驰、买卖点。
- 不重构 UI，不修改画图样式。
- 不要再另写一套包含关系处理；后续算法统一使用 `src/chan/inclusion.py` 生成的 `standard_bars`。
- 不允许直接在 `raw_bars` / `raw_df` 上识别分型、生成笔、判断笔端点极值或做任何缠论结构分析。
- `raw_bars` / `raw_df` 只用于主图显示、保留原始行情字段，以及通过 `inclusion_groups` 映射显示分析结果。
- `standard_bars` 是分型、笔、未来线段、中枢、背驰、买卖点和多级别分析的唯一基础数据。
- `inclusion_groups` 只负责映射，不直接负责缠论结构分析。
- 不删除 `validate_bi_extreme()`，不绕过端点极值校验。
- 不为了多画笔而降低全部过滤条件。
- 不在绘图阶段强行补线、移动笔端点或画候选笔。
- 允许最后一笔 `active_bi` 在后一笔确认前动态延伸。
- 禁止改写已经被后一笔确认过的历史笔，也就是 `locked_bis`。
- 允许只回收尚未锁定的 `pending_bi + active_bi` 活动窗口；不要从 `locked_bis` 中弹出历史笔再改写。
- 最终笔必须满足：顶底交替、不共用 K 线、至少 5 根虚拟 K 线、端点为该笔区间最高/最低、笔序列连续。

## 明天继续入口

1. 先读 `AGENTS.md`、`PROJECT_PLAN.md`、`DEV_LOG.md`、`NEXT_TASKS.md`。
2. 不要进入线段、中枢、背驰、买卖点；不要重构 UI。
3. 先查看 `output/suspected_missing_bis.csv` 中 `suspected_missing=True` 的长跨度区间。
4. 查看 `output/bi_attempts_debug.csv` 中 `reject_extreme_check_failed`、`reject_not_enough_center_gap`、`reject_no_neutral_bar_between_fractals` 集中的区间。
5. 用 `manual_expected_bis=[(start_index, end_index), ...]` 对人工认为应成笔的区间生成诊断报告。
6. 先确认人工指定笔是否满足 candidate_fractals 存在、center_gap、至少 1 根中间 standard_bar、端点极值和最终连续性，再判断是否仍是 active/anchor 路径问题。
7. 如果继续修改 `src/chan/bi.py`，保持 `locked_bis` 不可回写，只能调整未锁定活动窗口或更早的候选选择逻辑。
8. 修完后运行：

```powershell
.\.venv\Scripts\python.exe -m pytest
```
