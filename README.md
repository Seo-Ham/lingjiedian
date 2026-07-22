# 零碳园区招标信息聚合平台

## 概述

自动采集、结构化提取、全量验证互联网上零碳园区相关的招标/采购/中标公告，输出可筛选可分析的 Excel 数据集，为投资决策提供信息支撑。

**核心理念**：纯客观信息提取 + 严格全量验证，不做主观评分。

---

## 数据架构

### 25 字段模型（`schemas.py`）

#### 基础信息
| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 自增主键 |
| `title` | string | 公告标题（原文照录） |
| `source_url` | string | 公告原始链接 |
| `source_platform` | string | 来源平台名称 |
| `crawled_at` | datetime | 抓取时间戳 ISO8601 |

#### 地区维度
| 字段 | 类型 | 说明 |
|------|------|------|
| `province` | string | 省/自治区/直辖市（标准化去后缀） |
| `city` | string | 地级市，可为 null |
| `district` | string | 区/县，可为 null |

#### 招标分类
| 字段 | 类型 | 枚举值 |
|------|------|--------|
| `bid_status` | enum | `预告/公告` / `招标中(含更正)` / `中标公示` |
| `content_tags` | list[enum] | 多选：`可再生能源` `储能与微电网` `节能改造` `数字化碳管理` `绿色建筑与基础设施` `水处理与循环利用` `CCUS` `规划咨询与认证` `综合类` |
| `project_stage` | enum | `规划设计` / `工程建设` / `设备采购` / `运营服务` / `咨询认证` |

#### 时间维度
| 字段 | 类型 | 说明 |
|------|------|------|
| `publish_date` | date | 发布日期 YYYY-MM-DD，可为 null |
| `deadline_date` | date | 投标截止日期，可为 null |
| `award_date` | date | 中标日期，可为 null |

#### 金额
| 字段 | 类型 | 说明 |
|------|------|------|
| `amount_raw` | string | 原始金额表述（原文照搬） |
| `amount_range` | enum | `<100万` / `100-500万` / `500-1000万` / `1000万-5000万` / `5000万-1亿` / `>1亿` / `未披露` |

#### 参与方
| 字段 | 类型 | 说明 |
|------|------|------|
| `bidder` | string | 招标方/业主 |
| `winner` | string | 中标方，可为 null |
| `winner_type` | string | 央企/国企/民企/外资/联合体，可为 null |
| `contact_info` | string | 联系方式（原文照录），可为 null |

#### 数据质量
| 字段 | 类型 | 说明 |
|------|------|------|
| `verified` | bool | 是否通过全量验证 |
| `verification_note` | string | 验证备注（具体不匹配字段及原因） |
| `data_quality` | enum | `完整` / `部分缺失` / `仅标题` |

#### 原始留存
| 字段 | 类型 | 说明 |
|------|------|------|
| `raw_snippets` | string | 该记录对应的搜索片段（每条独立，不共享） |
| `remarks` | string | 人工备注 |

---

## 管道架构

```
┌─────────────────────────────────────────────────────────────┐
│                      SEARCH LAYER                            │
│  12 个关键词 × 2 通道 (Tavily + Serper) = 24 路并行搜索       │
│  Tavily: search_depth="advanced", max_results=5, 中英文      │
│  Serper: Google Search API, gl=cn, hl=zh-cn, num=5           │
│  → 合并去重，返回 urls + snippets + url_to_snippet_map       │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                     EXTRACTION LAYER                         │
│  DeepSeek-Chat (deepseek-chat)                               │
│  response_format: json_object, temperature=0.3               │
│  max_tokens=12000                                            │
│  Prompt: 提取所有零碳园区招标信息 → {"bids": [...]}           │
│  → 每条提取记录包含 source_url（从 snippet 标注中获取）        │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    VERIFICATION LAYER                         │
│  对每条记录:                                                   │
│  1. HTTP GET source_url → 提取正文 (HTML→text, 前10000字符)   │
│  2. 如直接访问失败 → Tavily fallback (爬虫缓存)               │
│  3. DeepSeek 逐字段对比: title/amount/bidder/winner/date/     │
│     bid_status 是否在原文中有支撑                              │
│  4. 输出 verified=True/False + 具体不匹配说明                  │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    PERSISTENCE LAYER                          │
│  evaluated_bids.jsonl — 增量写入，支持断点续传                 │
│  bids_data.json — 全量导出                                    │
│  bids_data.xlsx — Excel (筛选+冻结+分类说明Sheet)             │
└─────────────────────────────────────────────────────────────┘
```

---

## 搜索关键词策略

```
"零碳园区" 招标公告
"零碳产业园" 招标
"零碳园区" 中标
"零碳产业园" 中标公告
"近零碳园区" 招标
"碳中和园区" 采购
"零碳园区" 采购公告
site:gov.cn "零碳园区" 招标
site:ccgp.gov.cn "零碳" "园区"
"零碳园区" 竞争性磋商
"零碳园区" EPC
"零碳园区" 规划设计
```

**已知可扩展方向**：
- 按 31 省轮询：`"零碳园区" 招标 site:{province}.gov.cn`
- 按工程内容细化：`"零碳园区" 光伏` `"零碳园区" 储能` `"零碳园区" 碳监测`
- 按招标类型：`"零碳园区" 竞争性谈判` `"零碳园区" 单一来源` `"零碳园区" 询价`
- 按时间窗口：加 `after:2024-01-01` 等时间过滤

---

## 项目文件

| 文件 | 作用 |
|------|------|
| `schemas.py` | Pydantic 数据模型 + 6个枚举 + Excel 元数据 |
| `bid_agent.py` | 主 Agent：搜索 + 抽取 + 验证 + 持久化 + Excel导出 |
| `repair_bids.py` | 修复脚本：对 verified=False 记录自动修复（直接访问+Tavily备用） |
| `manual_fix.py` | 人工修正脚本：批量应用手动核查的更正、去重、拆分 |
| `evaluated_bids.jsonl` | 增量评估记录（断点续传） |
| `bids_data.json` | 全量 JSON 数据 |
| `bids_data.xlsx` | Excel 输出（2个Sheet：招标信息 + 分类说明） |
| `.env` | API Keys: TAVILY_API_KEY, DEEPSEEK_API_KEY, SERPER_API_KEY |

---

## 运行方式

```bash
# 全量采集（默认 3 轮搜索，每轮 12 关键词 × 2 通道）
python bid_agent.py

# 限制条数（测试用）
python bid_agent.py --limit 5

# 自定义搜索轮数
python bid_agent.py --rounds 5

# 修复验证未通过的记录
python repair_bids.py

# 应用人工修正（去重、拆分、替换URL、补全字段）
python manual_fix.py
```

---

## 当前数据集概况

| 指标 | 数值 |
|------|------|
| 记录总数 | 35 条 |
| 验证通过率 | 83% (29/35) |
| 覆盖省份 | 20+ |
| 覆盖工程内容 | 8/9 类（缺 CCUS） |
| 招标状态 | 招标中 60% / 中标 40% |
| 金额披露率 | 约 20%（多数为咨询服务类，金额较小） |

---

## 数据质量保障流程

```
首轮搜索采集 → LLM提取 → 逐条URL验证 → 
    ↓
verified=True → 入库（绿色）
verified=False → repair_bids.py 自动修复 → 
    ↓
修复成功 → 入库（绿色）
修复失败 → 人工核查 →
    ├── 手动补全字段 + 替换可访问URL → verified=True
    ├── 拆分聚合页记录 → 新增独立记录
    ├── 删除重复 → 合并保留最权威来源
    └── URL彻底不可达 → 标记 verified=False 保留
```

---

## 经验教训（供扩大搜索参考）

1. **snippet 中必须嵌入 URL**：LLM 提取时需要 `URL: xxx` 标注才能正确填充 `source_url`
2. **max_tokens 要够大**：20+ 条候选记录时 6000 不够，建议 12000
3. **json_object 不支持顶层数组**：Prompt 输出格式必须用 `{"bids": [...]}` 包装
4. **直接用 requests 访问 .gov.cn 常失败**：Tavily 爬虫缓存可作为备用验证通道
5. **北极星等专题聚合页**：一个 URL 含多个项目，需单独拆分处理
6. **重复检测**：同一项目可能被多个来源报道（如 ID=20 vs ID=31），按标题相似度 + URL 去重
7. **权威来源优先级**：政府采购网 > 公共资源交易平台 > 商业招标网 > 新闻聚合
