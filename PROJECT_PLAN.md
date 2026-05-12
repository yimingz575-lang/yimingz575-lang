# PROJECT_PLAN.md

本文件只记录路线和阶段边界，详细历史看 `DEV_LOG.md`。

## 长期路线

1. K 线数据层
2. 包含关系处理层
3. 分型层
4. 笔层
5. 段层
6. 中枢层
7. 多级别分析
8. 图形显示
9. 回测与选股

## 当前阶段

当前正在完善第 4 层：笔层。

K 线界面、数据加载、包含关系标记、`standard_bars` 标准化虚拟 K 线、候选分型识别、有效分型与笔生成已经完成到可诊断阶段。笔层当前具备端点极值校验、连续笔序列校验、`locked_bis + pending_bi + active_bi` 状态机和漏画笔诊断文件输出。下一步不要进入线段、中枢、背驰或买卖点，先检查全项目是否严格遵守数据流，再继续修正笔层真实走势中的疑似漏画笔问题。

## 架构数据流

原始 K 线 `raw_bars` / `raw_df` 只负责显示和保留行情。

缠论结构分析必须按以下数据流执行：

```text
raw_bars
    ↓
包含关系处理
    ↓
standard_bars + inclusion_groups
    ↓
fractals
    ↓
bis
    ↓
segments / zhongshu / divergence / buy_sell_points
```

架构约束：

- 原始 K 线 `raw_bars` 只负责主图显示和保留原始行情字段。
- 包含关系处理后生成 `standard_bars`。
- `standard_bars` 是分型、笔、线段、中枢、背驰、买卖点的唯一分析基础。
- `inclusion_groups` 负责把 `standard_bars` 上的分析结果映射回原始 K 线图。
- 后续线段、中枢、背驰、买卖点、多级别分析都必须沿用这条数据流。

## 阶段计划

### 1. K 线数据层

目标：统一读取 DEMO、真实 CSV、AkShare 日线/周线/月线数据。

已完成：

- DEMO 数据只使用 `data/sample/sample_demo_daily.csv`。
- 真实数据路径为 `data/real/{stock_code}_{period}.csv`。
- 6 位 A 股代码可自动读取或下载日线、周线、月线。

暂不做：

- 分钟线实时源。
- 实时自动刷新。

### 2. 包含关系处理层

目标：识别原始 K 线包含关系，并为后续算法构造内部 `standard_bars` 标准化虚拟 K 线序列与 `inclusion_groups` 映射关系。

已完成：

- 原始 K 线包含关系黄色标记。
- `src/chan/inclusion.py` 生成 `standard_bars`。
- `src/chan/inclusion.py` 生成 `inclusion_groups`，记录 `standard_bars` 与原始 K 线的映射关系。

暂不做：

- 在界面上用合并 K 线替代原始 K 线。

### 3. 分型层

目标：在 `standard_bars` 上识别顶分型、底分型，并通过 `inclusion_groups` / source 映射回原图。

已完成：

- 基于 `standard_bars` 的候选分型识别。
- 有效分型由笔生成逻辑确认。

暂不做：

- 脱离笔规则直接把所有候选分型当有效分型。

### 4. 笔层

目标：按缠论规则在 `standard_bars` 上确认有效顶底分型并生成笔，再映射回原始 K 线显示。

已完成：

- 第一版 `src/chan/bi.py`。
- 第一版测试 `tests/test_bi.py`。
- 图上勾选“显示笔”后绘制有效笔。
- `validate_bi_extreme()`：生成笔前校验端点是该笔闭区间最高/最低。
- `validate_bi_sequence_continuity()`：校验最终笔序列连续且方向交替。
- `build_bis_incremental()`：当前使用 `locked_bis + pending_bi + active_bi` 模型，历史笔锁定，未锁定活动窗口可回收，最后一笔可在后一笔确认前动态延伸。
- `write_bi_debug_report()` / `debug_bi_generation()`：输出漏画笔诊断报告和 CSV。
- `tests/test_bi_extreme.py`、`tests/test_bi_incremental.py`、`tests/test_bi_continuity.py`、`tests/test_bi_no_missing.py`、`tests/test_bi_debug.py`、`tests/test_bi_active_leg.py`。

下一步：

- 在继续修正漏画笔之前，先检查全项目是否严格遵守 `raw_bars -> standard_bars + inclusion_groups -> fractals -> bis`。
- 只排查笔层漏画笔问题。
- 优先检查 `locked_bis + pending_bi + active_bi` 状态机是否仍会让某些有效结构被拒绝。
- 对照 `output/bi_debug_report.txt`、`output/bi_attempts_debug.csv`、`output/suspected_missing_bis.csv` 和人工标记的 `manual_expected_bis` 精确修正。
- 修完必须运行 `.\.venv\Scripts\python.exe -m pytest`。

暂不做：

- 线段。
- 中枢。
- 背驰。
- 买卖点。

### 5. 段层

目标：在稳定笔序列基础上识别线段。

前置条件：

- 笔层规则稳定，测试覆盖充足。

### 6. 中枢层

目标：在线段或笔基础上识别中枢区间。

前置条件：

- 线段层稳定。

### 7. 多级别分析

目标：统一不同周期结构结果，支持多级别对照。

前置条件：

- 单级别的包含关系、分型、笔、线段、中枢稳定。

### 8. 图形显示

目标：保持当前 Dash + Plotly 界面，逐步优化缠论图层显示。

长期注意：

- Plotly 对大量 K 线和高频交互有性能边界。
- 若未来追求通达信级别丝滑，可评估 Lightweight Charts、ECharts、PyQtGraph 或专门 K 线引擎。

### 9. 回测与选股

目标：在算法稳定后做批量扫描、统计、回测。

暂不做：

- 自动交易。
- 收益承诺。
- 未来函数。
