# DEV_LOG.md

## 2026-05-12 笔层回溯深度改为可配置

本次没有修改 UI、`src/ui/chart.py`、`src/ui/app.py`，没有修改分型定义，没有删除极值校验，也没有降低成笔条件。

修改文件：
- `src/chan/bi.py`
- `tests/test_bi_rollback_depth.py`
- `DEV_LOG.md`

本次调整：
- 将固定 `MAX_BI_ROLLBACK = 5` 改为 `DEFAULT_MAX_BI_ROLLBACK = 15`，并继续通过 `build_bis_incremental(..., max_rollback=...)` / `try_rollback_and_rebuild_tail(..., max_rollback=...)` 支持显式配置。
- 回溯仍从 `rollback_count=1` 开始逐步试到 `max_rollback`，发现第一个合法且能推进更远的方案后立即接受，不继续试更大的回溯。
- 接受方案仍必须满足：顶底交替、不共享 standard_bar、至少 1 根中间 standard_bar、`center_gap >= 4`、`validate_bi_extreme()`、笔序列连续。
- 新增 `tests/test_bi_rollback_depth.py`，覆盖 rollback 1..5 失败、rollback 10 首次成功、选择最小成功回溯数且不全局重算。

600497 日线诊断结果：
- `max_rollback=15`
- 第一次成功的 `rollback_count=10`
- 修正前 `last_confirmed_bi_end_raw_index=4374`
- 修正后 `last_confirmed_bi_end_raw_index=5055`
- 修正前 `confirmed_bis=217`
- 修正后 `confirmed_bis=250`
- `output/bi_rollback_debug.csv` 已记录 1..10 次回溯试探，其中 1..9 为 `reject_no_later_coverage`，10 为 `accepted`。
- 右侧 `raw_index=4375..5064` 已有 confirmed_bis 覆盖，最后覆盖推进到 `raw_index=5055`。

## 2026-05-12 笔层有限回溯机制

本次没有修改 UI、`src/ui/chart.py`、`src/ui/app.py`，没有进入线段、中枢、背驰、买卖点。

修改文件：
- `src/chan/bi.py`
- `tests/test_bi_rollback.py`
- `tests/test_bi_debug.py`
- `DEV_LOG.md`

本次新增：
- `MAX_BI_ROLLBACK = 5`。
- `STUCK_CANDIDATE_THRESHOLD = 20`。
- `try_rollback_and_rebuild_tail()`：当最后确认笔之后仍有足够多候选顶底分型、但尾部无法继续推进时，按 1..5 笔逐步回退并重建尾段。
- 回溯重建仍使用原有硬规则：顶底交替、不共享 standard_bar、至少 1 根中间 standard_bar、`center_gap >= 4`、`validate_bi_extreme()`、笔序列连续。
- 新增 `output/bi_rollback_debug.csv`，并在 `output/bi_debug_report.txt` 中记录 rollback 触发、成功、失败次数和接受的回溯笔数。

600497 日线诊断结果：
- `confirmed_bis_count=217`
- `last_confirmed_bi_end_raw_index=4374`
- `rollback_trigger_count=1`
- `rollback_success_count=0`
- `rollback_failed_count=1`
- `accepted_rollback_count=None`
- 尾部 `raw_index=4375..5064` 仍有 `candidate_fractals=240`，其中 `top=120`、`bottom=120`。
- 在 `MAX_BI_ROLLBACK=5` 限制内，回退 1..5 笔后的新尾段都只能推进到 `raw_index=4374`，拒绝原因为 `reject_no_later_coverage`。
- 额外诊断显示，如果把最大回退试探放宽到 10，第一次可接受方案出现在 `rollback_count=10`，新尾段可推进到 `raw_index=5055`，但这超过了本次指定的 `MAX_BI_ROLLBACK=5` 规则，因此未写入默认算法结果。

验证：
- `.\.venv\Scripts\python.exe -m pytest`：`88 passed`

## 2026-05-12 图表默认视图与拖动 Y 轴修正

本次仍未修改分型和笔算法，未修改 `src/chan/fractal.py` 和 `src/chan/bi.py`，只处理图表显示和交互问题。

修改过的文件：
- `src/ui/app.py`
- `src/ui/chart.py`
- `tests/test_chart_bi_mapping.py`
- `DEV_LOG.md`

问题原因：
- 图表真正绘制时使用的是当前窗口内的 `chart_x = 0..N-1`。
- Dash 拖动后的 Y 轴自适应回调使用的是缓存中的 `plot_df`，其中 `x/raw_index` 仍然是全量原始索引，例如 600497 最近 1000 根为 `4065..5064`。
- 因此用户在图上拖动到 `chart_x=400..600` 时，回调用 `4065..5064` 的坐标体系切片，切不到当前可见 K 线，Y 轴范围不会随当前 K 线更新。
- 旧逻辑还会在默认最新窗口没有笔时，自动把初始 X 轴挪到“最后一笔附近”，导致打开图表时看起来仍停在历史旧区间。

本次修正：
- `calculate_visible_yaxis_ranges()` 在计算可见 Y 轴范围前，先把传入数据重新归一化为图表实际使用的 `chart_x` 坐标。
- `_make_default_xaxis_range()` 不再根据最后一笔自动移动视图，默认回到最新 K 线窗口。
- `ANALYSIS_VERSION` 更新为 `standard_bars_v3_chart_x_yaxis`，避免继续复用旧缓存。
- 新增测试覆盖：缓存数据 `x/raw_index` 与图表 `chart_x` 不一致时，Y 轴自适应仍按图表窗口计算；默认 X 轴保持最新 K 线窗口，不再跳到历史最后一笔附近。

600497 日线验证：
- 默认性能绘图窗口仍为最近 1000 根：`raw_index=4065..5064`。
- 默认可见 X 轴范围现为 `[700, 999]`，即最后 300 根 K 线。
- 后端 `confirmed_bis=217`。
- 当前最近 1000 根窗口内可绘制笔仍为 `bi_trace_count=17`，其余 `200` 笔端点在绘图窗口外。
- `calculate_visible_yaxis_ranges(plot_df, [400, 600])` 已可返回价格区间 `[4.1555, 6.4545]`，不再因为缓存 raw index 而返回空。

验证结果：
- `.\.venv\Scripts\python.exe -m pytest`：81 passed。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；命令行检查超时是因为 Dash 服务会持续运行。

## 2026-05-12 图表笔映射排查

本次暂停分型和笔算法修改，未修改 `src/chan/fractal.py` 和 `src/chan/bi.py`，只排查后端 `confirmed_bis` 到 Dash 图表的显示链路。

修改过的文件：
- `src/ui/chart.py`
- `tests/test_chart_bi_mapping.py`
- `DEV_LOG.md`

本次确认：
- `engine.analyze_chan_marks()` 返回字段包含 `confirmed_bis`，同时保留兼容字段 `bis`。
- `chart.py` 绘图统一读取 `confirmed_bis`，不再用旧字段生成笔线，也不在图表层重新计算分型或笔。
- K 线、均线、MACD、分型和笔统一使用当前 `raw_bars_plot_window` 的 `chart_x = 0..N-1`。
- 笔端点映射流程为：`confirmed_bis.start/end_center_index -> inclusion_groups.source_end_index -> raw_bars_plot_window.raw_index -> chart_x`。
- `chart.py` 增加调试输出：raw 总数、当前绘图窗口数量、窗口 raw index 范围、standard_bars 数量、inclusion_groups 数量、confirmed_bis 数量、bi_trace_count、映射失败数量、端点不在窗口数量，以及前 20 条笔的详细映射信息。

600497 日线诊断结果：
- 全量 raw_bars：5065
- standard_bars：3795
- candidate_fractals：1842
- confirmed_bis：217
- 默认性能绘图窗口：raw_index 4065..5064，共 1000 根 K 线
- 默认窗口内 bi_trace_count：17
- skipped_bi_mapping_failed：0
- skipped_bi_endpoint_outside_plot_window：200
- 全量显示模式 bi_trace_count：217

结论：
- 当前不是 `inclusion_groups` 映射失败，映射失败数量为 0。
- 默认窗口只有少量笔，是因为后端 `confirmed_bis` 中只有 17 笔的两个端点同时落在最后 1000 根 K 线窗口内；其余 200 笔位于更早 raw_index，被图表按窗口边界正确跳过。
- 若中右侧仍大面积无笔，说明该区间后端本身没有生成 `confirmed_bis` 覆盖，后续应在允许修改笔算法时回到 `locked_bis + pending_bi + active_bi` 状态机和诊断文件继续排查。

验证结果：
- `.\.venv\Scripts\python.exe -m pytest`：79 passed。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；命令行检查超时是因为 Dash 服务会持续运行。

更新时间：`2026-05-12 17:43:08 +08:00`

当前阶段：包含关系已按规范生成内部 `standard_bars`，分型与笔层基于 `standard_bars` 运行；笔层保留端点区间极值校验、`locked_bis + pending_bi + active_bi` 状态机与漏画笔诊断机制。

## 2026-05-12 DEMO 与真实股票画笔链路核查

本次暂停分型和笔算法修改，没有修改 `src/chan/fractal.py` 和 `src/chan/bi.py`，只核查 `engine/chart/app` 的数据流、字段读取和缓存链路。

本次修改过的文件：

- `src/chan/engine.py`
- `src/ui/chart.py`
- `DEV_LOG.md`

核查结论：

- DEMO 没有单独的旧画笔逻辑，也没有 `symbol == "DEMO"` 的特殊画法。
- 600497 没有单独的真实股票画笔逻辑。
- 两者都通过 `src/ui/chart.py -> src/chan/engine.py:analyze_chan_marks()` 进入同一条链路。
- `chart.py` 当前画笔只读取 `marks["confirmed_bis"]`，不再用旧字段 `bis` 画笔。
- `chart.py` 当前笔坐标使用 `inclusion_groups.source_end_index -> raw_index -> chart_x` 映射。
- Dash 数据缓存 key 已包含 `ANALYSIS_VERSION`、股票代码和周期，避免继续复用旧缓存键。

新增诊断输出：

- `engine.py` 打印 `[engine] symbol`、`is_demo`、`standard_bars count`、`candidate_fractals count`、`confirmed_bis count`。
- `chart.py` 打印 `[chart] using field = confirmed_bis`、`[chart] uses inclusion_groups mapping = True`。

验证结果：

- DEMO：`is_demo=True`、`standard_bars=167`、`candidate_fractals=105`、`confirmed_bis=9`、`bi_trace_count=9`。
- 600497：`is_demo=False`、`standard_bars=3795`、`candidate_fractals=1842`、`confirmed_bis=217`、默认 1000 根显示窗口内 `bi_trace_count=17`，窗口外跳过 `200`。
- `.\.venv\Scripts\python.exe -m pytest`：`79 passed`。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；检查命令超时是因为 Dash 开发服务器会持续运行。

## 2026-05-12 chart_x 坐标统一修正

本次暂停分型和笔算法修改，没有修改 `src/chan/fractal.py` 和 `src/chan/bi.py`，只修正图表显示坐标和缓存版本。

本次修改过的文件：

- `src/ui/app.py`
- `src/ui/chart.py`
- `tests/test_chart_bi_mapping.py`
- `DEV_LOG.md`

问题原因：

- 600497 的 K 线 trace 使用的是全量原始序号切片后的 `x`，例如最近 1000 根仍是 `4065..5064`。
- 笔 trace 使用的是 standard_bars / source index 映射出来的局部或全量 index，没有统一到当前 K 线图层的显示坐标。
- 因此笔会被画到没有 K 线的空白区域，或者当前有 K 线的可视区域看不到笔。

本次修正：

- `chart.py` 为当前实际绘制的 raw K 线切片统一生成 `chart_x = 0..len(raw_bars_displayed)-1`，并让 K 线、MA、MACD、包含关系、分型、笔全部使用同一套 `chart_x`。
- `raw_index` 只作为 standard_bars / inclusion_groups 映射回 raw_bars_displayed 的中间字段，不再直接作为图表 x。
- `chart.py` 画笔只读取 `marks["confirmed_bis"]`，不再通过旧字段 `bis` 画笔。
- 笔端点从 `start_center_index/end_center_index` 经 `inclusion_groups.source_end_index` 映射到 `raw_index`，再映射为 `chart_x`。
- 两端不在当前 raw_bars_displayed 窗口内的笔会跳过，并输出 `skipped_bi_not_in_visible_window`。
- `app.py` 增加 `ANALYSIS_VERSION = "standard_bars_v2_confirmed_bis_mapping"`，避免继续复用旧缓存键。

验证结果：

- 600497 当前默认绘图数据窗口：`raw_bars_total=5065`、`raw_bars_displayed=1000`、`standard_bars=3795`、`confirmed_bis=217`、`candle_x_min=0`、`candle_x_max=999`、`bi_trace_count=17`、`skipped_bi_not_in_visible_window=200`。
- DEMO：`raw_bars_total=260`、`raw_bars_displayed=260`、`standard_bars=167`、`confirmed_bis=9`、`bi_trace_count=9`、`skipped_bi_not_in_visible_window=0`。
- 所有已绘制 bi trace 的 x 坐标都在当前 K 线 `chart_x` 范围内。
- `.\.venv\Scripts\python.exe -m pytest`：`79 passed`。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；检查命令超时是因为 Dash 开发服务器会持续运行。

## 2026-05-12 图表默认显示笔与视口修正

本次仍未修改分型和笔算法，只继续修正前端显示链路。

本次修改过的文件：

- `src/ui/app.py`
- `src/ui/chart.py`
- `DEV_LOG.md`

问题原因：

- 终端截图中的 `[chart] show_bi = False` 表示“显示笔”没有传入图表，因此不会画笔。
- 600497 默认显示最近 300 根 K 线，但当前最后一笔 `confirmed_bis` 结束在 `2023-06-30`，而最近 300 根是 2025-2026 区间；即使 trace 已添加，默认视口也看不到任何笔。

本次修正：

- Dash 默认勾选“显示笔”，避免启动后误以为后端笔没有显示。
- `create_kline_figure()` 新增 `analysis_df`，图表可显示窗口数据，但缠论分析使用完整缓存数据。
- 当默认最近窗口里没有任何笔、但后端已有确认笔时，自动把初始 X 轴范围移动到最后一笔附近。

验证结果：

- DEMO 默认启动：`[chart] confirmed_bis count = 9`，`[chart] bi traces added = 9`。
- 600497 默认 300 根显示窗口：`[chart] confirmed_bis count = 217`，`[chart] bi traces added = 217`，X 轴自动移动到最后确认笔附近 `[4075, 4374]`。
- `.\.venv\Scripts\python.exe -m pytest`：`74 passed`。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；检查命令超时是因为 Dash 开发服务器会持续运行。

## 2026-05-12 图表笔显示链路修正

本次没有修改分型和笔算法，只排查并修正后端已生成 `confirmed_bis` 但前端图表不显示的问题。

本次修改过的文件：

- `src/ui/chart.py`
- `DEV_LOG.md`

排查结论：

- `main.py` 正常调用当前 `src.ui.app.create_app()`，图表入口来自 `src/ui/chart.py`。
- `src/chan/engine.py` 返回的笔字段名是 `confirmed_bis`，同时保留兼容字段 `bis`。
- `src/ui/chart.py` 原来读取兼容字段 `bis`，不是空字段问题；真正问题是绘图坐标映射。
- Dash 性能模式下传给图表的是最后 1000 根 K 线，显示用 `x` 仍是全局坐标，例如 `4065..5064`；但后端笔的 `start_x/end_x` 来自子集内部 source index，例如 `0..999`，导致笔 trace 被画到当前坐标轴左侧很远的位置，图上看起来一笔都没有。

本次修正：

- `chart.py` 绘图优先读取 `confirmed_bis`，保留 `bis` 作为兼容回退。
- 绘制分型和笔前，使用 `inclusion_groups` 将 standard_bars 的 `center_index/virtual_index` 映射回当前 raw K 线图上的真实 `x` 坐标。
- 增加临时控制台输出：`[chart] show_bi`、`[chart] marks keys`、`[chart] confirmed_bis count`、`[chart] bi traces added`。

验证结果：

- `.\.venv\Scripts\python.exe -m pytest`：`74 passed`。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；启动默认未勾选“显示笔”，因此打印 `[chart] show_bi = False`。
- 模拟 600497 勾选“显示笔”：
  - 最近 1000 根窗口：`[chart] confirmed_bis count = 17`，`[chart] bi traces added = 17`。
  - 全部数据模式：`[chart] confirmed_bis count = 217`，`[chart] bi traces added = 217`。

## 2026-05-12 分型与笔层按 standard_bars 重检

本次只处理分型和笔，没有修改 UI，也没有进入线段、中枢、背驰、买卖点。

本次修改过的文件：

- `src/chan/fractal.py`
- `src/chan/bi.py`
- `src/chan/engine.py`
- `tests/test_fractal.py`
- `tests/test_fractal_definition.py`
- `tests/test_bi_fractal_spacing.py`
- `tests/test_bi_from_fractals.py`
- `tests/test_chan_data_flow.py`
- `DEV_LOG.md`
- `NEXT_TASKS.md`

本次修正要点：

- `detect_candidate_fractals()` 改为只接受 `standard_bars`，禁止直接接收 `raw_bars` / `raw_df`。
- 候选分型记录补齐 `center_index`、`span_start`、`span_end`、`source_start_index`、`source_end_index` 等字段。
- `build_bis_incremental()` 按 `standard_bars + candidate_fractals` 从左到右生成笔，保留 `locked_bis + active_bi` 模型。
- 成笔检查明确使用 `MIN_CENTER_GAP_FOR_BI = 4`，并检查不共享 standard_bar、顶底之间至少 1 根中间 standard_bar、端点区间极值和笔序列连续。
- `validate_bi_extreme()` 继续只检查当前候选笔在 `standard_bars` 上的闭区间最高/最低。
- `analyze_chan_marks(raw_bars)` 统一返回 `raw_bars`、`standard_bars`、`inclusion_groups`、`candidate_fractals`、`valid_fractals_for_bi`、`confirmed_bis`，兼容旧的 `fractals` / `bis` 图表入口。

新增或更新测试：

- `tests/test_fractal_definition.py`：严格顶/底分型定义、相等不成分型、首尾不成分型、禁止 raw_df 直接识别分型。
- `tests/test_bi_fractal_spacing.py`：不共享 standard_bar、至少 1 根中间 standard_bar、`center_gap >= 4`。
- `tests/test_bi_from_fractals.py`：顶底成笔、底顶成笔、同类不成笔、连续性和端点极值。
- `tests/test_chan_data_flow.py`：raw_bars 禁止直接进入分型/笔，`analyze_chan_marks()` 必须先生成 `standard_bars + inclusion_groups`。
- `tests/test_fractal.py`：旧分型测试改为显式基于 `build_standard_bars(df)` 后再识别。

验证结果：

- `.\.venv\Scripts\python.exe -m pytest`：`74 passed`。
- `.\.venv\Scripts\python.exe main.py`：可启动到 `http://127.0.0.1:8050/`；命令检查时超时是因为 Dash 开发服务器会持续运行。

600497 日线诊断结果：

- `raw_bars_count=5065`
- `standard_bars_count=3795`
- `inclusion_groups_count=3795`
- `candidate_fractals_count=1842`
- `confirmed_bis_count=217`
- `alternating=True`
- `no_shared_standard_bar=True`
- `neutral_bar_between_fractals=True`
- `center_gap_at_least_4=True`
- `extreme_ok=True`
- `continuity_ok=True`
- 诊断文件：`output/bi_debug_report.txt`、`output/fractals_debug.csv`、`output/bi_attempts_debug.csv`、`output/suspected_missing_bis.csv`

## 2026-05-12 项目规则更新

本次只更新项目规则文档，没有修改核心代码、测试或 UI。

本次修改过的文件：

- `AGENTS.md`
- `PROJECT_PLAN.md`
- `DEV_LOG.md`
- `NEXT_TASKS.md`

今天新增了“`standard_bars` 作为缠论分析唯一基础数据”的长期规则。

长期强制数据流：

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

规则要点：

- 原始 K 线 `raw_bars` / `raw_df` 只用于主图显示、保留原始行情字段，以及通过 `inclusion_groups` 映射显示分析结果。
- 禁止直接用 `raw_bars` 做顶底分型、笔、线段、中枢、背驰、买卖点或任何缠论结构判断。
- `standard_bars` 是分型、笔、未来线段、中枢、背驰、买卖点、多级别分析的唯一基础数据。
- `inclusion_groups` 只负责把 `standard_bars` 上的分析结果映射回原始 K 线图，不直接负责结构分析。
- 后续修改 `src/chan/inclusion.py`、`src/chan/fractal.py`、`src/chan/bi.py`、`src/chan/engine.py`、`src/ui/chart.py` 前，必须先检查是否符合 `raw_bars -> standard_bars + inclusion_groups -> fractals -> bis -> chart mapping`。
- 下一步在继续修正漏画笔之前，先检查全项目是否严格遵守这条数据流。

## 2026-05-12 包含关系规范化

本次修改过的文件：

- `src/chan/inclusion.py`
- `src/chan/fractal.py`
- `tests/test_inclusion.py`
- `tests/test_fractal.py`
- `DEV_LOG.md`
- `NEXT_TASKS.md`

本次只处理包含关系与其对分型/笔层的输入影响，没有修改 UI，也没有进入线段、中枢、背驰、买卖点。

`src/chan/inclusion.py` 本次修正：

- 新增 `StandardKLine`、`InclusionResult`、`build_standard_bars()`、`build_inclusion_groups()`、`process_inclusions()`。
- 原始 K 线只读，不删除、不覆盖、不改写；内部另行生成 `standard_bars`。
- 包含判定使用相邻 K 线双向包含，包含等价边界，相等也视为包含。
- 方向判定使用 high 与 low 同时确认：`curr.high > prev.high and curr.low > prev.low` 为 upward，`curr.high < prev.high and curr.low < prev.low` 为 downward。
- 向上包含合并使用 `high=max(...)`、`low=max(...)`；向下包含合并使用 `high=min(...)`、`low=min(...)`。
- 连续包含从左到右递归合并：合并后的 `merged_bar` 会继续与下一根原始 K 线比较。
- 起始包含关系不再随意兜底方向；会向右寻找第一个明确方向，若整段无法形成明确方向，则不产出可用于分型确认的标准序列。
- 每根 `standard_bar` 记录 `virtual_index`、`source_start_index`、`source_end_index`、`source_indices`、`source_positions`、`date_start`、`date_end`、`open`、`high`、`low`、`close`、`volume`。

`src/chan/fractal.py` 本次修正：

- `detect_candidate_fractals()` 和 `build_virtual_klines()` 改为调用 `build_standard_bars()`。
- 分型识别基于 `standard_bars`，不再在 `fractal.py` 内部另起一套去包含合并逻辑。
- 分型点仍映射回原始 K 线坐标；顶/底价格使用标准 K 线处理后的 high/low，并在标准 K 线来源组中找到对应原始 K 线位置。

测试与诊断结果：

- 新增/更新包含关系测试，覆盖无包含、向上包含、向下包含、三根以上连续递归包含、原始 K 线不变、source 字段记录、起始包含等待明确方向。
- 新增分型测试，确认分型识别调用 `standard_bars`，而不是直接使用原始 K 线。
- `.\.venv\Scripts\python.exe -m pytest` 当前结果：`55 passed`。
- 600497 日线诊断：`original_kline_count=5065`、`standard_bars_count=3795`、`confirmed_bis=211`、`continuity=True`、`all_extreme=True`。
- 仍需继续排查：`suspected_missing_bis=True` 仍为 `130` 条，`reject_reverse_extreme_check_failed=278` 仍集中存在，下一步继续围绕 600497 日线 `2023-06-30` 附近 anchor index `4374` 做人工指定笔诊断。

## 2026-05-12 状态机修正

本次修改过的文件：

- `src/chan/bi.py`
- `tests/test_bi_continuity.py`
- `DEV_LOG.md`
- `NEXT_TASKS.md`

本次只继续笔层，没有进入线段、中枢、背驰、买卖点，也没有重构 UI。

`src/chan/bi.py` 本次修正：

- 将 `locked_bis` 语义收紧为真正不可回写的历史笔：代码不再从 `locked_bis` 中弹出最近一笔再改写。
- 新增内部 `pending_bi` 活动窗口：紧挨着最后一笔 `active_bi` 的上一笔先留在 pending 中，只有当后续新笔继续确认后才进入 `locked_bis`。
- 反向候选未能从当前 `active_bi` 终点合法成笔时，仍允许只回收 `pending_bi + active_bi` 这个未锁定窗口；诊断原因记录为 `reopen_active_window_with_more_extreme_reverse`。
- 保留 `validate_bi_extreme()`，没有降低 K 线数量、顶底交替、端点极值或连续性约束；绘图阶段仍只使用最终 `confirmed_bis`。
- 诊断统计新增 `pending_bi_count`，用于区分真正锁定历史和仍可调整的活动窗口。

测试与诊断结果：

- `tests/test_bi_continuity.py` 新增/调整 active 窗口回收与 locked 历史不可改写测试。
- `.\.venv\Scripts\python.exe -m pytest` 当前结果：`49 passed`。
- 600497 日线诊断：`original_kline_count=5065`、`virtual_kline_count=3795`、`raw_fractals=1842`、`cleaned_fractals=1842`、`confirmed_bis=211`、`locked_bis=209`、`pending_bi=1`、`active_bi=1`、`continuity=True`、`all_extreme=True`。
- 600497 日线活动窗口回收：`reopen_active_window_with_more_extreme_reverse=25`，这些回收不再改写真正的 `locked_bis`。
- 仍需继续排查：`suspected_missing_bis=True` 仍为 `130` 条，`reject_reverse_extreme_check_failed=278` 仍集中存在，尤其是 600497 日线 `2023-06-30` 附近 anchor index `4374`。

## 2026-05-12 进度保存

今天修改过的文件：

- `src/chan/bi.py`
- `tests/test_bi_active_leg.py`
- `tests/test_bi_continuity.py`
- `DEV_LOG.md`
- `NEXT_TASKS.md`
- `AGENTS.md`
- `PROJECT_PLAN.md`

`src/chan/bi.py` 当前笔生成逻辑进展：

- 分型和笔判断基于包含关系处理后的虚拟 K 线序列。
- 生成笔前保留 `validate_bi_extreme()`，只校验当前候选笔起终点闭区间，要求顶/底是该笔区间最高/最低。
- 最终笔序列保留 `validate_bi_sequence_continuity()`，要求 `bis[i].end == bis[i+1].start` 且方向交替。
- `build_bis_incremental()` 当前采用 `locked_bis + active_bi` 模型：已经被后一笔确认过的历史笔进入 `locked_bis`，禁止回头改写；最后一笔 `active_bi` 在后一笔确认前允许被同方向更极端分型动态延伸；必要时只回收最近 active 窗口，不改更早历史。
- 绘图阶段仍只使用最终 `confirmed_bis`，不画候选笔，也不在绘图阶段移动端点。

当前测试结果：

- `.\.venv\Scripts\python.exe -m pytest`
- 当前结果：`48 passed`

当前仍存在的漏画笔问题：

- 600497 日线当前诊断为 `raw_fractals=1842`、`cleaned_fractals=1842`、`confirmed_bis=211`、`continuity=True`、`all_extreme=True`。
- 仍存在疑似漏画笔或过度合并区间：`output/suspected_missing_bis.csv` 中 `suspected_missing=True` 为 `130` 条。
- `reject_reverse_extreme_check_failed=278` 仍然存在集中区间，其中 `2023-06-30` 附近 anchor index `4374` 集中 `117` 次，需要明天优先结合人工标记检查。

诊断文件输出位置：

- `output/bi_debug_report.txt`
- `output/fractals_debug.csv`
- `output/bi_attempts_debug.csv`
- `output/suspected_missing_bis.csv`

明天 Codex 应该从这里继续：

1. 先读 `AGENTS.md`、`PROJECT_PLAN.md`、`DEV_LOG.md`、`NEXT_TASKS.md`。
2. 不要进入线段、中枢、背驰、买卖点，不要重构 UI。
3. 优先从 `output/suspected_missing_bis.csv` 和 `output/bi_attempts_debug.csv` 中选择疑似漏画最集中的区间。
4. 用 `manual_expected_bis=[(start_index, end_index), ...]` 对人工认为应成笔的区间生成诊断报告。
5. 只在确认原因后小范围修正 `src/chan/bi.py` 的 `locked_bis + active_bi` 状态机。
6. 修完后运行 `.\.venv\Scripts\python.exe -m pytest`。

## 2026-05-12 更新

- `src/chan/bi.py`：将笔生成状态机调整为 `locked_bis + active_bi` 模型；最后一笔在下一笔确认前允许通过同向更极端分型延伸终点，必要时只回收最近 active 窗口，避免最后一笔过早锁死后导致后续反向笔集中 `extreme_check_failed`。
- `src/chan/bi.py`：诊断报告新增 `locked_bis_count`、`active_bi_count`、`final_confirmed_bis_count`、`active_bi_endpoint_extensions_count`、`reverse_reject_extreme_check_failed_count`、`reverse_reject_not_enough_bars_count`。
- `src/chan/bi.py`：新增 `output/suspected_missing_bis.csv` 输出，逐笔统计区间内部 raw 分型和交替分型数量，用于人工定位疑似过度合并区间。
- `tests/test_bi_active_leg.py`：新增 active_bi 延伸、延伸后下一笔起点、历史笔不被更远同向点吞并、连续多笔与硬约束测试。
- `tests/test_bi_continuity.py`：调整旧的局部回退测试，使其覆盖 active 窗口回收且不制造断层。
- 验证结果：`.\.venv\Scripts\python.exe -m pytest` 当前 `48 passed`。
- 600497 日线诊断：`raw_fractals=1842`、`cleaned_fractals=1842`、`confirmed_bis=211`、`locked_bis=210`、`active_bi=1`、`continuity=True`、`all_extreme=True`、`active_bi_endpoint_extensions_count=298`。
- 600497 日线拒绝原因仍显示 `reject_reverse_extreme_check_failed=278`，其中 `2023-06-30` 附近 anchor index `4374` 集中 `117` 次，后续应结合 `manual_expected_bis` 和 `suspected_missing_bis.csv` 继续精确诊断。

## 已完成内容

- `main.py`：Dash 程序入口，启动 `http://127.0.0.1:8050/`。
- `requirements.txt`：包含 dash、plotly、pandas、numpy、akshare、pytest。
- `src/data_source/csv_loader.py`：统一 DEMO、真实 CSV、本地真实数据缺失时下载入口。
- `src/data_source/akshare_loader.py`：下载 A 股日线、周线、月线并保存为标准字段 `date, open, high, low, close, volume`。
- `src/indicators/macd.py`：计算 DIF、DEA、MACD。
- `src/ui/app.py`：股票代码、周期、显示数量、显示选项、性能模式、数据缓存、拖动时只更新 Y 轴范围。
- `src/ui/chart.py`：黑底红网格 K 线、MA5/10/20/60、MACD、连续交易日 `x`、hover、包含关系/分型/笔图层。
- `src/chan/inclusion.py`：识别相邻原始 K 线包含关系，不删除、不隐藏、不合并原始 K 线。
- `src/chan/fractal.py`：构造内部虚拟去包含 K 线序列，在虚拟序列上识别候选顶/底分型，并映射回原始 K 线。
- `src/chan/bi.py`：有效分型确认与笔生成，包含顶底交替、不能共用 K 线、至少 5 根虚拟 K 线、同类分型替换；已新增 `validate_bi_extreme()`、`build_bis_incremental()`、`validate_bi_sequence_continuity()`；本次新增 `write_bi_debug_report()` 和 `debug_bi_generation()`，可输出 raw/cleaned 分型、每次 anchor/candidate 尝试、拒绝原因、人工指定笔检查结果。
- `src/ui/chart.py`：仍只绘制 `analyze_chan_marks()` 返回的最终 `bis`；本次在“显示笔”路径增加 confirmed_bis 控制台摘要，逐条打印最终确认笔。
- `src/chan/engine.py`：统一输出包含关系、有效分型、笔。
- `sync_github.bat`：Windows 菜单式 GitHub 同步脚本。

## 已验证内容

- `.venv` 已通过 `python -m venv --upgrade .venv` 修复，当前指向 `C:\Python311\python.exe`。
- `.\.venv\Scripts\python.exe -m pytest`
- 当前结果：`48 passed`
- Plotly 图表生成检查：绘图代码循环 `bis.iterrows()` 绘制所有最终笔，不是只画 `bis[0]`。
- 本轮修正前状态：`600497_daily raw_fractals=1842`、`confirmed_bis=270`、连续性断点 `158`；`DEMO_daily confirmed_bis=12`、连续性断点 `4`。
- 当前诊断状态：`600497_daily original_kline_count=5065`、`virtual_kline_count=3795`、`raw_fractals=1842`、`cleaned_fractals=1842`、`confirmed_bis=211`、`continuity=True`、`all_extreme=True`、笔图层 trace 数量 `211`；`DEMO_daily raw_fractals=105`、`cleaned_fractals=105`、`confirmed_bis=9`、`continuity=True`、`all_extreme=True`、笔图层 trace 数量 `9`。
- 已生成诊断文件：`output/bi_debug_report.txt`、`output/fractals_debug.csv`、`output/bi_attempts_debug.csv`、`output/suspected_missing_bis.csv`。
- 600497 日线尝试成笔原因分布：`reject_reverse_not_enough_bars=509`、`reject_extension_not_more_extreme=482`、`extend_active_bi_endpoint=298`、`reject_reverse_extreme_check_failed=278`、`lock_previous_and_start_new_active_bi=235`、`replace_anchor_same_type_more_extreme=25`、`reject_extreme_check_failed=9`、`reject_direction_error=2`。
- `fractals_debug.csv` 显示当前没有 clean 阶段误删分型：`in_cleaned=True` 共 `1842` 条。
- `.\.venv\Scripts\python.exe main.py` 可启动到 `http://127.0.0.1:8050/`；命令检查时超时是因为 Dash 开发服务器会持续运行。
- Python 语法检查：`syntax ok`
- Dash app 创建检查：`app ok`

## 测试文件

- `tests/test_inclusion.py`：无包含、当前被前一根包含、当前包含前一根、连续包含、原始 OHLC 不变。
- `tests/test_fractal.py`：顶分型、底分型、首尾不识别、同一 K 线不同时顶底、字段完整、原始数据不变。
- `tests/test_bi.py`：共用 K 线不能成笔、不足 5 根不能成笔、上升/下降笔、端点区间重叠但价格方向有效时可以成笔、顶底交替、同类分型替换、原始数据不变、候选分型未形成反向笔不确认。
- `tests/test_bi_active_leg.py`：active_bi 终点延伸、延伸后下一笔从新端点开始、后一笔确认后更远新高/新低不能吞并已锁定历史、连续多笔结构和最终硬约束。
- `tests/test_bi_extreme.py`：向下笔更高顶替换、向上笔更低底替换、向下/向上笔端点极值校验失败、最终生成笔必须满足虚拟 K 线区间最高/最低约束。
- `tests/test_bi_incremental.py`：top-bottom-top 生成 2 笔、五个交替分型生成 4 笔、首个反向分型 K 线数量不足时等待、未形成有效反向笔前可替换更极端同向 anchor、上一笔确认后后续更极端同向分型可作为下一笔 anchor 但不改写上一笔。
- `tests/test_bi_continuity.py`：交替分型生成连续笔、每笔起点接上一笔终点、拒绝候选不重置 anchor、同向更极端端点更新上一笔时不制造断层、失败反向候选可触发 active 窗口回收并保持连续。
- `tests/test_bi_no_missing.py`：构造 10 笔长交替结构，逐笔验证顶底交替、不共用 K 线、K 线数量、端点区间极值和前后连续。
- `tests/test_bi_debug.py`：验证诊断报告和 CSV 可生成、字段完整、人工指定笔缺失时能报告原因。

## 当前已知问题

- `src/chan/bi.py` 中“顶底不能连接成笔”的过度保守价格区间判断已修正。
- `src/chan/bi.py` 已在生成笔阶段加入虚拟 K 线区间极值校验，避免顶/底端点不是该笔区间最高/最低时仍被画成笔。
- `src/chan/bi.py` 已移除“回头改写上一笔终点”的逻辑；已确认笔不会因后续同向更极端分型被吞并，同时后续同向更极端分型仍可作为下一笔新 anchor，避免极值校验把后续有效笔全部拒掉。
- `src/chan/bi.py` 当前会维护最终 `bis` 的连续性：`bis[i].end == bis[i+1].start` 且方向交替。
- `build_bis_incremental(debug=True)` 可打印 raw/cleaned fractals 数量、confirmed bis 数量、anchor、candidate、同向判断、anchor 替换、K 线数量、极值校验、是否生成笔和拒绝原因。
- `write_bi_debug_report(df, output_dir="output", manual_expected_bis=[...])` 可生成文本报告和 CSV；人工指定笔会检查起终点是否在 raw/cleaned 分型中、K 线数量、共用 K 线、极值校验以及最终是否被确认。
- `src/ui/chart.py` 当前只绘制 `analyze_chan_marks()` 返回的最终 `bis`，没有候选笔绘图入口。
- `detect_fractals()` 返回候选分型；`engine.detect_fractal_marks()` 返回经过笔确认的有效分型。后续修改时不要混淆。
- Plotly 在大量 K 线和多图层时仍有性能边界，当前靠最近 1000 根、性能模式和 layout patch 缓解。
- 分钟线数据源尚未接入，只支持日线、周线、月线真实数据。
- 线段、中枢、多级别、背驰、买卖点、回测、选股尚未实现。
- 旧文件 `data/sample/600497_daily.csv` 仍存在，但不应作为真实行情或默认 DEMO 来源；默认 DEMO 应使用 `data/sample/sample_demo_daily.csv`。

## 不要重复做的内容

- 不要重写 Dash 页面骨架。
- 不要重写连续交易日 X 轴逻辑。
- 不要重新实现 MACD、MA、数据缓存、Y 轴自适应。
- 不要重新实现包含关系标记。
- 不要把候选分型直接全部连成笔。
- 不要为了显示效果强行连接不合格分型。
- 不要把虚拟合并 K 线显示成主图 K 线。

## 下一次 Codex 应该从哪里继续

1. 先读 `AGENTS.md`、`PROJECT_PLAN.md`、`DEV_LOG.md`、`NEXT_TASKS.md`。
2. 使用 `output/bi_debug_report.txt` 和 `output/bi_attempts_debug.csv` 对照图上疑似漏笔位置，先用 `manual_expected_bis` 输入人工认为应成笔的 index，再根据拒绝原因精确修正。
3. 若漏画原因集中在 raw 分型未识别，再修 `src/chan/fractal.py`；若集中在数量/极值/anchor 路径，再小范围修 `src/chan/bi.py`。
4. 后续修改后继续运行 `.\.venv\Scripts\python.exe -m pytest`。
5. 不要进入线段、中枢、背驰、买卖点。

## 建议给下一次 Codex 的提示词

```text
请先阅读 D:\chan-theory-project 下的 AGENTS.md、PROJECT_PLAN.md、DEV_LOG.md、NEXT_TASKS.md。
不要重构 UI，不要实现线段/中枢/买卖点。
当前优先在 Dash 中人工检查 600497 日线等真实走势的有效笔输出，重点确认不再漏掉中间有效笔，同时观察是否有过度成笔。
如需运行测试，使用 `.\.venv\Scripts\python.exe -m pytest`。
```
