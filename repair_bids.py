#!/usr/bin/env python3
"""零碳园区招标数据修复脚本 — 自动修正验证未通过的记录"""

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv
from openai import AsyncOpenAI, APIConnectionError, RateLimitError, InternalServerError, APIError
from tavily import AsyncTavilyClient
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
JSONL_FILE = BASE_DIR / "evaluated_bids.jsonl"
BACKUP_FILE = BASE_DIR / "evaluated_bids.jsonl.bak"

DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("repair")

# 省份名称标准化映射
PROVINCE_NORMALIZE = {
    "江苏省": "江苏", "湖北省": "湖北", "青海省": "青海", "海南省": "海南",
    "河南省": "河南", "吉林省": "吉林", "广东省": "广东", "陕西省": "陕西",
    "山东省": "山东", "新疆": "新疆", "辽宁省": "辽宁", "甘肃省": "甘肃",
    "北京市": "北京", "上海市": "上海", "天津市": "天津", "重庆市": "重庆",
    "内蒙古自治区": "内蒙古", "广西壮族自治区": "广西", "广西": "广西",
    "西藏自治区": "西藏", "宁夏回族自治区": "宁夏",
    "新疆维吾尔自治区": "新疆", "西藏": "西藏", "宁夏": "宁夏",
    "河北省": "河北", "山西省": "山西", "黑龙江省": "黑龙江",
    "浙江省": "浙江", "安徽省": "安徽", "福建省": "福建", "江西省": "江西",
    "湖南省": "湖南", "贵州省": "贵州", "云南省": "云南", "四川省": "四川",
}


def normalize_province(p: str) -> str:
    if not p:
        return ""
    return PROVINCE_NORMALIZE.get(p, p)


# ---------------------------------------------------------------------------
# Fix Prompt
# ---------------------------------------------------------------------------

FIX_SYSTEM = """你是一个招标信息数据修复助手。现在有一条招标记录的某些字段被验证为不匹配原文，请根据原文内容修正这些字段。

## 规则
1. 只修正 verification_note 中指出的问题字段
2. 其他字段保持不变
3. 所有值必须从原文中提取，严禁编造
4. 如果原文确实找不到该信息，保持原值为空/null

## 输出格式
只输出一个 JSON 对象，包含修正后的完整记录：
```json
{
  "title": "...",
  "bid_status": "...",
  "content_tags": ["..."],
  "project_stage": "...",
  "publish_date": "..." 或 null,
  "deadline_date": "..." 或 null,
  "award_date": "..." 或 null,
  "amount_raw": "...",
  "amount_range": "...",
  "bidder": "...",
  "winner": "..." 或 null,
  "winner_type": "..." 或 null,
  "contact_info": "..." 或 null,
  "data_quality": "..."
}
```

只输出 JSON，不要其他文字。"""


async def _call_llm(aclient, model, messages, max_tokens=3000):
    return await aclient.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=0.2,
        response_format={"type": "json_object"},
    )


_call_llm_retry = retry(
    retry=retry_if_exception_type((
        OSError, ConnectionError, TimeoutError,
        APIConnectionError, RateLimitError, InternalServerError,
    )),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)(_call_llm)


# ---------------------------------------------------------------------------
# Web Page Fetcher
# ---------------------------------------------------------------------------

def _html_to_text(html: str) -> str:
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&amp;', '&').replace('&quot;', '"').replace('&#39;', "'")
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n', text)
    return text.strip()


async def fetch_page_text(session, url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36"
        }
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return None
            html = await resp.text(encoding="utf-8", errors="replace")
            return _html_to_text(html)[:10000]
    except Exception:
        return None


async def fetch_page_via_tavily(tavily: AsyncTavilyClient, url: str) -> str | None:
    """备用通道：通过 Tavily 搜索该 URL，利用其爬虫缓存获取内容"""
    if not tavily:
        return None
    try:
        # 提取 URL 中的关键词作为搜索词
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        path_words = parsed.path.replace("/", " ").replace("-", " ").replace("_", " ")
        # 限制搜索词长度，避免 Tavily 400 错误
        search_query = path_words[:200] if path_words else url[:200]
        resp = await tavily.search(
            search_query, search_depth="basic", max_results=3,
            include_raw_content=True,
        )
        for r in resp.get("results", []):
            raw = r.get("raw_content") or ""
            if raw and len(raw) > 200:
                return raw[:10000]
        contents = [r.get("content", "") for r in resp.get("results", [])]
        combined = "\n".join(c for c in contents if c)
        return combined[:10000] if combined else None
    except Exception as e:
        logger.debug("  Tavily 备用通道异常: %s", e)
        return None


# ---------------------------------------------------------------------------
# Verification (简化版)
# ---------------------------------------------------------------------------

VERIFY_SYSTEM = """你是一个严格的数据审核员。请逐字段核对以下招标记录是否与原文一致。

输出格式（只输出 JSON）：
```json
{
  "verified": true/false,
  "verification_note": "如果 verified=true，写'修正后全部关键字段有原文支撑'；如果 verified=false，列出仍不匹配的字段"
}
```
只输出 JSON，不要其他文字。"""


async def verify_record(aclient, model, record, page_text):
    fields = {
        "title": record.get("title", ""),
        "amount_raw": record.get("amount_raw", ""),
        "bidder": record.get("bidder", ""),
        "winner": record.get("winner", ""),
        "publish_date": record.get("publish_date", ""),
        "deadline_date": record.get("deadline_date", ""),
        "bid_status": record.get("bid_status", ""),
    }
    check = json.dumps(fields, ensure_ascii=False, indent=2)
    messages = [
        {"role": "system", "content": VERIFY_SYSTEM},
        {"role": "user", "content": f"## 记录\n```json\n{check}\n```\n\n## 原文\n{page_text[:8000]}"},
    ]
    try:
        resp = await _call_llm_retry(aclient, model, messages, max_tokens=500)
        raw = resp.choices[0].message.content or "{}"
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw)
        return result.get("verified", False), result.get("verification_note", "")
    except Exception as e:
        return False, f"验证失败: {e}"


# ---------------------------------------------------------------------------
# Main Repair Logic
# ---------------------------------------------------------------------------

async def repair_all(aclient, model, session, tavily, records):
    new_records = []
    fixed_count = 0
    skip_count = 0

    for i, rec in enumerate(records):
        # 先标准化省份
        rec["province"] = normalize_province(rec.get("province", ""))

        if rec.get("verified"):
            new_records.append(rec)
            continue

        source_url = rec.get("source_url", "")
        if not source_url:
            logger.info("[%d/%d] 跳过: %s — 无 source_url", i + 1, len(records),
                        rec.get("title", "")[:50])
            new_records.append(rec)
            skip_count += 1
            continue

        logger.info("[%d/%d] 修复: %s", i + 1, len(records), (rec.get("title") or "")[:50])
        page_text = await fetch_page_text(session, source_url)
        if not page_text:
            logger.info("  直接访问失败，尝试 Tavily 备用通道...")
            page_text = await fetch_page_via_tavily(tavily, source_url)
        if not page_text:
            logger.info("  Tavily 也无法获取，跳过")
            new_records.append(rec)
            skip_count += 1
            continue

        # 构建修复请求
        current = {
            "title": rec.get("title", ""),
            "bid_status": rec.get("bid_status", ""),
            "content_tags": rec.get("content_tags", []),
            "project_stage": rec.get("project_stage", ""),
            "publish_date": rec.get("publish_date"),
            "deadline_date": rec.get("deadline_date"),
            "award_date": rec.get("award_date"),
            "amount_raw": rec.get("amount_raw", ""),
            "amount_range": rec.get("amount_range", ""),
            "bidder": rec.get("bidder", ""),
            "winner": rec.get("winner"),
            "winner_type": rec.get("winner_type"),
            "contact_info": rec.get("contact_info"),
            "data_quality": rec.get("data_quality", ""),
        }
        vnote = rec.get("verification_note", "")

        messages = [
            {"role": "system", "content": FIX_SYSTEM},
            {"role": "user", "content": (
                f"## 问题字段\n{vnote}\n\n"
                f"## 当前记录\n```json\n{json.dumps(current, ensure_ascii=False, indent=2)}\n```\n\n"
                f"## 原始网页内容\n{page_text[:8000]}"
            )},
        ]

        try:
            resp = await _call_llm_retry(aclient, model, messages, max_tokens=3000)
            raw_content = resp.choices[0].message.content or "{}"
            raw_content = raw_content.strip()
            if raw_content.startswith("```"):
                raw_content = re.sub(r"^```(?:json)?\s*", "", raw_content)
                raw_content = re.sub(r"\s*```$", "", raw_content)
            fixed = json.loads(raw_content)
        except Exception as e:
            logger.warning("  修复 LLM 调用失败: %s", e)
            new_records.append(rec)
            skip_count += 1
            continue

        # 应用修正（只更新业务字段，保留元数据）
        for key in ["title", "bid_status", "content_tags", "project_stage",
                     "publish_date", "deadline_date", "award_date",
                     "amount_raw", "amount_range", "bidder", "winner",
                     "winner_type", "contact_info", "data_quality"]:
            if key in fixed:
                rec[key] = fixed[key]

        # 重新验证
        verified, vnote_new = await verify_record(aclient, model, rec, page_text)
        rec["verified"] = verified
        rec["verification_note"] = vnote_new
        logger.info("  → verified=%s — %s", verified, (vnote_new or "")[:80])

        if verified:
            fixed_count += 1
        new_records.append(rec)

    logger.info("修复完成: %d/%d 条通过验证 (修复了 %d 条)",
                sum(1 for r in new_records if r.get("verified")),
                len(new_records), fixed_count)
    return new_records


async def main_async():
    if not JSONL_FILE.exists():
        logger.error("JSONL 文件不存在")
        return

    aclient = AsyncOpenAI(api_key=DEEPSEEK_KEY, base_url="https://api.deepseek.com")
    model = "deepseek-chat"
    tavily_key = os.getenv("TAVILY_API_KEY", "")
    tavily = AsyncTavilyClient(api_key=tavily_key) if tavily_key else None

    # 备份
    import shutil
    shutil.copy(JSONL_FILE, BACKUP_FILE)
    logger.info("已备份: %s", BACKUP_FILE)

    # 读取
    records = []
    with open(JSONL_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    red_before = sum(1 for r in records if not r.get("verified"))
    logger.info("读取 %d 条记录，其中 %d 条需修复", len(records), red_before)

    async with aiohttp.ClientSession() as session:
        new_records = await repair_all(aclient, model, session, tavily, records)

    # 写回 JSONL
    with open(JSONL_FILE, "w", encoding="utf-8") as f:
        for rec in new_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    green_after = sum(1 for r in new_records if r.get("verified"))
    logger.info("已写入 %d 条记录 → %s (验证通过: %d → %d)",
                len(new_records), JSONL_FILE, len(records) - red_before, green_after)

    # 重新导出 JSON 和 Excel
    records_sorted = sorted(new_records, key=lambda r: r["id"])
    json_path = BASE_DIR / "bids_data.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records_sorted, f, ensure_ascii=False, indent=2)
    logger.info("JSON 已更新: %s", json_path)

    # Excel
    from bid_agent import export_to_xlsx
    export_to_xlsx()
    logger.info("Excel 已更新")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
