# AGENTS.md

本文件是给 Codex/ChatGPT 的长期规则。新会话开始前必须先阅读：

1. `AGENTS.md`
2. `PROJECT_PLAN.md`
3. `DEV_LOG.md`
4. `NEXT_TASKS.md`

读完后先总结当前状态，再决定是否修改代码。

## 项目定位

本项目是缠论 Chan Theory 分析引擎与 K 线可视化界面。

当前技术栈：

- Python
- pandas
- plotly
- dash
- pytest
- akshare

当前目标不是换框架，而是在现有 Dash + Plotly 界面上逐步实现缠论结构分析。

## 已完成能力，后续不要重复实现

- `main.py` 作为程序入口。
- `src/data_source/csv_loader.py`：DEMO 示例数据、真实 CSV 读取、A 股代码数据加载入口。
- `src/data_source/akshare_loader.py`：通过 AkShare 下载日线、周线、月线真实行情并保存到 `data/real/`。
- `src/ui/app.py`：Dash 页面、股票代码、周期、显示选项、性能模式、数据缓存、拖动时 Y 轴自适应。
- `src/ui/chart.py`：黑底红网格 K 线、MA5/10/20/60、MACD、连续交易日 X 轴、包含关系/分型/笔显示。
- `src/chan/inclusion.py`：相邻原始 K 线包含关系可视化标记、`standard_bars` 标准化虚拟 K 线序列、`inclusion_groups` 映射关系。
- `src/chan/fractal.py`：基于 `standard_bars` 的候选顶/底分型识别、映射回原始 K 线。
- `src/chan/bi.py`：基于 `standard_bars` 的有效分型确认与笔生成，当前包含端点极值校验、连续笔序列校验、`locked_bis + pending_bi + active_bi` 状态机和漏画笔诊断输出。
- `tests/test_inclusion.py`、`tests/test_fractal.py`、`tests/test_bi.py`：当前算法测试。

修改代码前必须先检查现有函数，优先扩展或修正已有函数，不要另起一套同名逻辑。

## 编码规则

- 每次只做一个小任务。
- 优先小范围、高确定性修改。
- 不要无关重构。
- 不要把所有代码写进 `main.py`。
- 不要删除、隐藏、替换原始 K 线。
- 不要在界面上显示虚拟合并 K 线替代原始 K 线。
- 图上所有缠论结果必须映射回原始 K 线的 `x` 坐标。
- 修改后必须说明改了哪些文件。
- 每次完成任务后必须更新 `DEV_LOG.md`。
- 写算法前先补或更新测试。

## 缠论分析数据流强制规则

本项目中，原始 K 线 `raw_bars` / `raw_df` 不能直接作为缠论结构分析的数据基础。

所有缠论结构分析必须遵守以下数据流：

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

### 1. raw_bars / raw_df

`raw_bars` 是原始行情 K 线数据，只允许用于：

- 主图 K 线显示；
- 保留原始 `open`、`high`、`low`、`close`、`volume`；
- 通过 `inclusion_groups` 映射显示分型、笔、线段、中枢等结果。

禁止直接使用 `raw_bars` 做以下操作：

- 识别顶底分型；
- 生成笔；
- 判断笔端点极值；
- 判断线段；
- 判断中枢；
- 判断背驰；
- 判断买卖点；
- 做任何缠论结构判断。

### 2. standard_bars

`standard_bars` 是 `raw_bars` 经过包含关系处理后生成的标准化虚拟 K 线序列。

`standard_bars` 是本项目所有缠论分析的唯一基础数据。

以下操作必须基于 `standard_bars`：

- `detect_fractals`；
- `build_bis` / `build_bis_incremental`；
- `validate_bi_extreme`；
- `validate_bi_sequence_continuity`；
- 未来 segment 识别；
- 未来 zhongshu 识别；
- 未来 divergence / 背驰判断；
- 未来 buy_sell_points / 买卖点判断；
- 多级别缠论结构分析。

### 3. inclusion_groups

`inclusion_groups` 是 `standard_bars` 与 `raw_bars` 的映射关系。

`inclusion_groups` 的作用是：

- 记录每根 `standard_bar` 由哪些原始 K 线合并而来；
- 把 `standard_bars` 上识别出的分型、笔、线段、中枢等结果映射回原始 K 线图；
- 用于绘图定位、hover 信息、调试和诊断。

`inclusion_groups` 只负责映射，不直接负责缠论结构分析。

### 4. 禁止写法

以后禁止出现以下逻辑：

```text
detect_fractals(raw_bars)
build_bis(raw_bars, fractals)
build_bis_incremental(raw_bars, fractals)
validate_bi_extreme(raw_bars, ...)
直接在 raw_bars 上寻找顶分型或底分型
直接在 raw_bars 上生成笔
为了绘图方便绕过 standard_bars
```

### 5. 正确写法

以后所有缠论分析入口必须遵守：

```text
standard_bars, inclusion_groups = build_standard_bars_with_inclusion(raw_bars)

fractals = detect_fractals(standard_bars)

bis = build_bis_incremental(standard_bars, fractals)

chart 使用 inclusion_groups 把 standard_bars 上的分析结果映射回 raw_bars 显示。
```

当前代码可使用 `build_standard_bars()` / `process_inclusions()` 等现有函数承载上述数据流，但语义必须保持一致。

### 6. 修改代码前的检查要求

以后 Codex 修改以下文件时：

- `src/chan/inclusion.py`
- `src/chan/fractal.py`
- `src/chan/bi.py`
- `src/chan/engine.py`
- `src/ui/chart.py`

必须先检查数据流是否符合：

```text
raw_bars -> standard_bars + inclusion_groups -> fractals -> bis -> chart mapping
```

如果发现有函数直接使用 `raw_bars` 做分型、笔或其他缠论结构分析，必须优先修正为使用 `standard_bars`。

### 7. 测试要求

后续新增或更新测试，必须确保：

- `raw_bars` 不被修改；
- `standard_bars` 是分型识别输入；
- `standard_bars` 是笔生成输入；
- `fractals` 带有 `virtual_index`；
- `bis` 的 start/end 基于 `standard_bars` 的 `virtual_index`；
- 图表显示时通过 `inclusion_groups` 映射回 `raw_bars`；
- 不允许直接在 `raw_bars` 上识别分型或生成笔。

## Git 与文件规则

不要提交或上传：

- `.venv/`
- `venv/`
- `__pycache__/`
- `.pytest_cache/`
- `output/`
- `logs/`
- `*.log`
- `.vscode/`
- 临时调试文件

真实行情 CSV 属于本地缓存，提交前必须确认是否需要上传。旧的 `data/sample/600497_daily.csv` 不应被当作真实行情来源，真实 600497 日线应来自 `data/real/600497_daily.csv`。

## 成笔逻辑硬规则

后续修正 `src/chan/bi.py` 时必须遵守：

1. 原始 K 线必须先做包含关系处理，得到算法内部的无包含虚拟 K 线序列。
2. 分型必须来自处理后的无包含 K 线序列，不能直接粗暴使用原始 K 线。
3. 顶底不能共用同一根虚拟 K 线，也不能映射到同一根原始 K 线。
4. 顶底必须交替，不能出现顶顶底底直接连接。
5. 顶底之间必须满足 K 线数量要求，当前规则为 `abs(end.virtual_index - start.virtual_index) + 1 >= 5`。
6. 笔端点必须取对应虚拟 K 线来源原始 K 线中的最高点或最低点。
7. 不允许为了画线强行连接不合格分型。
8. 每一笔的顶/底必须是该笔闭区间内的最高价/最低价，不能删除或绕过 `validate_bi_extreme()`。
9. 最终笔序列必须连续：`bis[i].end == bis[i+1].start`，且方向交替。
10. 不要为了多画笔而降低全部过滤条件；必须先用诊断文件确认漏画原因。
11. 不要在绘图阶段强行补线、移动端点或画候选笔；绘图只能使用最终有效 `confirmed_bis`。
12. `locked_bis` 是已被后一笔确认过的历史笔，禁止被后续走势回头改写；最后一笔 `active_bi` 可在后一笔确认前动态延伸。

当前 `bi.py` 已具备较多测试覆盖，但仍存在真实走势中疑似漏画笔问题。下一步应先读 `DEV_LOG.md` 和 `NEXT_TASKS.md`，不要重复已完成工作，优先排查 `locked_bis + active_bi` 状态机和诊断文件中集中的拒绝原因。
