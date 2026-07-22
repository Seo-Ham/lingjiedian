#!/usr/bin/env python3
"""零碳园区招标信息聚合 — Pydantic 数据模型与枚举定义"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field


# =============================================================================
# 枚举定义
# =============================================================================

class BidStatus(str, Enum):
    """招标状态"""
    PREVIEW = "预告/公告"
    ACTIVE = "招标中(含更正)"
    AWARDED = "中标公示"


class ContentTag(str, Enum):
    """工程内容标签（多选）"""
    RENEWABLE = "可再生能源"
    STORAGE_MICROGRID = "储能与微电网"
    ENERGY_EFFICIENCY = "节能改造"
    DIGITAL_CARBON = "数字化碳管理"
    GREEN_BUILDING = "绿色建筑与基础设施"
    WATER_CIRCULAR = "水处理与循环利用"
    CCUS = "CCUS"
    CONSULTING = "规划咨询与认证"
    COMPREHENSIVE = "综合类"


class ProjectStage(str, Enum):
    """项目阶段"""
    PLANNING = "规划设计"
    CONSTRUCTION = "工程建设"
    PROCUREMENT = "设备采购"
    OPERATION = "运营服务"
    CONSULTING = "咨询认证"


class AmountRange(str, Enum):
    """金额区间"""
    UNDER_1M = "<100万"
    M1_M5 = "100-500万"
    M5_M10 = "500-1000万"
    M10_M50 = "1000万-5000万"
    M50_M100 = "5000万-1亿"
    OVER_100M = ">1亿"
    UNDISCLOSED = "未披露"


class DataQuality(str, Enum):
    """数据质量"""
    COMPLETE = "完整"
    PARTIAL = "部分缺失"
    TITLE_ONLY = "仅标题"


# =============================================================================
# 主记录模型
# =============================================================================

class BidRecord(BaseModel):
    """零碳园区招标信息完整记录 — 25 字段"""

    # ---- 基础标识 ----
    id: int = 0
    title: str = Field(default="", description="公告标题（原文）")
    source_url: str = Field(default="", description="公告原始链接")
    source_platform: str = Field(default="", description="来源平台名称")
    crawled_at: str = Field(default="", description="抓取时间戳 ISO8601")

    # ---- 地区维度 ----
    province: str = Field(default="", description="省/自治区/直辖市")
    city: str | None = Field(default=None, description="地级市")
    district: str | None = Field(default=None, description="区/县")

    # ---- 招标分类 ----
    bid_status: str = Field(default="", description="招标状态: 预告/公告 | 招标中(含更正) | 中标公示")
    content_tags: list[str] = Field(default_factory=list, description="工程内容标签（多选）")
    project_stage: str = Field(default="", description="项目阶段: 规划设计 | 工程建设 | 设备采购 | 运营服务 | 咨询认证")

    # ---- 时间维度 ----
    publish_date: str | None = Field(default=None, description="发布日期 YYYY-MM-DD")
    deadline_date: str | None = Field(default=None, description="投标截止日期 YYYY-MM-DD")
    award_date: str | None = Field(default=None, description="中标日期 YYYY-MM-DD")

    # ---- 金额 ----
    amount_raw: str = Field(default="", description="原始金额表述（照搬原文）")
    amount_range: str = Field(default="", description="金额区间: <100万 | 100-500万 | 500-1000万 | 1000万-5000万 | 5000万-1亿 | >1亿 | 未披露")

    # ---- 参与方 ----
    bidder: str = Field(default="", description="招标方/业主")
    winner: str | None = Field(default=None, description="中标方")
    winner_type: str | None = Field(default=None, description="中标方类型: 央企/国企/民企/外资/联合体")
    contact_info: str | None = Field(default=None, description="联系方式（原文照录）")

    # ---- 园区级别 ----
    park_level: str | None = Field(default=None, description="园区级别: 国家级 | 省级 | null(未明确)")

    # ---- 数据质量 ----
    verified: bool = Field(default=False, description="是否通过全量验证")
    verification_note: str = Field(default="", description="验证备注（不匹配字段及原因）")
    data_quality: str = Field(default="", description="数据质量: 完整 | 部分缺失 | 仅标题")

    # ---- 原始留存 ----
    raw_snippets: str = Field(default="", description="搜索返回的原始文本片段")
    remarks: str = Field(default="", description="人工备注")


# =============================================================================
# 字段元数据（供前端和 Excel 使用）
# =============================================================================

FIELD_META: list[dict] = [
    # (序号, 字段名, 中文表头, 类型说明)
    {"col": 1,  "field": "id",              "header": "ID",            "type": "int"},
    {"col": 2,  "field": "title",           "header": "公告标题",       "type": "text"},
    {"col": 3,  "field": "source_url",      "header": "原文链接",       "type": "url"},
    {"col": 4,  "field": "source_platform", "header": "来源平台",       "type": "text"},
    {"col": 5,  "field": "crawled_at",      "header": "抓取时间",       "type": "datetime"},
    {"col": 6,  "field": "province",        "header": "省份",          "type": "text"},
    {"col": 7,  "field": "city",            "header": "城市",          "type": "text"},
    {"col": 8,  "field": "district",        "header": "区县",          "type": "text"},
    {"col": 9,  "field": "bid_status",      "header": "招标状态",       "type": "enum"},
    {"col": 10, "field": "content_tags",    "header": "工程内容",       "type": "multi-enum"},
    {"col": 11, "field": "project_stage",   "header": "项目阶段",       "type": "enum"},
    {"col": 12, "field": "publish_date",    "header": "发布日期",       "type": "date"},
    {"col": 13, "field": "deadline_date",   "header": "截止日期",       "type": "date"},
    {"col": 14, "field": "award_date",      "header": "中标日期",       "type": "date"},
    {"col": 15, "field": "amount_raw",      "header": "原始金额",       "type": "text"},
    {"col": 16, "field": "amount_range",    "header": "金额区间",       "type": "enum"},
    {"col": 17, "field": "bidder",          "header": "招标方/业主",    "type": "text"},
    {"col": 18, "field": "winner",          "header": "中标方",        "type": "text"},
    {"col": 19, "field": "winner_type",     "header": "中标方类型",     "type": "enum"},
    {"col": 20, "field": "contact_info",    "header": "联系方式",       "type": "text"},
    {"col": 21, "field": "verified",        "header": "验证通过",       "type": "bool"},
    {"col": 22, "field": "verification_note","header":"验证备注",        "type": "text"},
    {"col": 23, "field": "data_quality",    "header": "数据质量",       "type": "enum"},
    {"col": 24, "field": "park_level",      "header": "园区级别",       "type": "enum"},
    {"col": 25, "field": "raw_snippets",    "header": "原始片段",       "type": "text"},
    {"col": 26, "field": "remarks",         "header": "备注",          "type": "text"},
]

# 枚举字段 → 合法值映射（Excel 数据验证用）
ENUM_VALUES: dict[str, list[str]] = {
    "bid_status":    [e.value for e in BidStatus],
    "content_tags":  [e.value for e in ContentTag],
    "project_stage": [e.value for e in ProjectStage],
    "amount_range":  [e.value for e in AmountRange],
    "winner_type":   ["央企", "国企", "民企", "外资", "联合体"],
    "data_quality":  [e.value for e in DataQuality],
}

# 中国 31 省级行政区（数据验证用）
PROVINCES: list[str] = [
    "北京", "天津", "河北", "山西", "内蒙古",
    "辽宁", "吉林", "黑龙江",
    "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东",
    "河南", "湖北", "湖南", "广东", "广西", "海南",
    "重庆", "四川", "贵州", "云南", "西藏",
    "陕西", "甘肃", "青海", "宁夏", "新疆",
]
