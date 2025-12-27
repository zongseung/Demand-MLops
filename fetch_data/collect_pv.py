# koenergy_async_downloader_all.py
# - 발전소/호기 index 선택 제거 (코드 직접 입력 or 엔터=전체)
# - 기간은 월 단위로 자동 분할(1개월 제한 대응)
# - 월별 다운로드 사이 5초 대기
# - 파일명에 전체/발전소코드 포함
#
# 실행:
#   pip install aiohttp
#   python3 koenergy_async_downloader_all.py
#
# 팁:
# - "전체"로 받으려면 strOrgNo/strHokiS/strHokiE 모두 엔터
# - 특정 발전소만 받으려면 strOrgNo에 코드(예: 84S1) 입력, 호기는 엔터(전체) 또는 값 입력

import asyncio
import re
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp

BASE = "https://www.koenergy.kr"
MENU_CD = "FN0912020217"

OUTPUT_DIR = Path.cwd()  # 원하는 저장 경로로 변경 가능


def _sanitize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-.가-힣 ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:180]


def _prompt(msg: str, default: Optional[str] = None) -> str:
    if default is not None:
        s = input(f"{msg} [{default}]: ").strip()
        return s or default
    return input(f"{msg}: ").strip()


def _validate_yyyymmdd(date_str: str) -> str:
    if not re.fullmatch(r"\d{8}", date_str):
        raise ValueError("YYYYMMDD 형식(예: 20251101)이어야 합니다.")
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def _to_date_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _month_end(d: date) -> date:
    if d.month == 12:
        next_month = date(d.year + 1, 1, 1)
    else:
        next_month = date(d.year, d.month + 1, 1)
    return next_month - timedelta(days=1)


def split_by_month(date_s: str, date_e: str) -> List[Tuple[str, str]]:
    start = _to_date_yyyymmdd(date_s)
    end = _to_date_yyyymmdd(date_e)
    if end < start:
        raise ValueError("종료일이 시작일보다 빠릅니다.")

    ranges: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        me = _month_end(cur)
        chunk_end = me if me <= end else end
        ranges.append((_to_yyyymmdd(cur), _to_yyyymmdd(chunk_end)))
        cur = chunk_end + timedelta(days=1)
    return ranges


def _build_main_url(
    page_index: str,
    org_no: str,
    hoki_s: str,
    hoki_e: str,
    date_s: str,
    date_e: str,
) -> str:
    # 캡처 Referer 형태 유지 (빈 값이면 전체)
    return (
        f"{BASE}/kosep/gv/nf/dt/nfdt21/main.do"
        f"?pageIndex={page_index}&menuCd={MENU_CD}&xmlText="
        f"&strOrgNo={org_no}&strHokiS={hoki_s}&strHokiE={hoki_e}"
        f"&strDateS={date_s}&strDateE={date_e}"
    )


def _tag_for_filename(org_no: str, hoki_s: str, hoki_e: str) -> str:
    # 파일명 태그: 전체/발전소코드/호기 범위 표시(비어있으면 전체로 간주)
    if not org_no and not hoki_s and not hoki_e:
        return "전체"
    parts = []
    parts.append(org_no if org_no else "ALLORG")
    if hoki_s or hoki_e:
        hs = hoki_s if hoki_s else "ALL"
        he = hoki_e if hoki_e else "ALL"
        parts.append(f"H{hs}-{he}")
    return "_".join(parts)


async def download_all_by_months() -> None:
    csv_url = f"{BASE}/kosep/gv/nf/dt/nfdt21/csvDown.do"

    headers = {
        "Origin": BASE,
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0",
    }

    # 캡처 기준: pageIndex=1 (그대로 유지 권장)
    page_index = _prompt("pageIndex", "1")

    # 엔터면 '전체'(빈 값 전송)
    org_no = _prompt("발전소 코드(strOrgNo) - 전체는 엔터", "").strip()
    hoki_s = _prompt("호기 시작(strHokiS) - 전체는 엔터", "").strip()
    hoki_e = _prompt("호기 종료(strHokiE) - 전체는 엔터", "").strip()

    # 기간 입력(전체 기간 -> 월별 자동 분할)
    while True:
        try:
            date_s = _validate_yyyymmdd(_prompt("시작일(YYYYMMDD)", "20251101"))
            date_e = _validate_yyyymmdd(_prompt("종료일(YYYYMMDD)", "20251201"))
            if _to_date_yyyymmdd(date_e) < _to_date_yyyymmdd(date_s):
                raise ValueError("종료일이 시작일보다 빠릅니다.")
            break
        except Exception as e:
            print(f"입력 오류: {e}")

    month_ranges = split_by_month(date_s, date_e)
    print(f"\n총 {len(month_ranges)}개 구간(월 단위)으로 분할합니다.")
    for i, (ds, de) in enumerate(month_ranges, start=1):
        print(f"  {i:>2}. {ds} ~ {de}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = _tag_for_filename(org_no, hoki_s, hoki_e)

    async with aiohttp.ClientSession(headers={"User-Agent": headers["User-Agent"]}) as session:
        for idx, (ds, de) in enumerate(month_ranges, start=1):
            main_url = _build_main_url(page_index, org_no, hoki_s, hoki_e, ds, de)

            # 세션 쿠키 확보(필요 케이스 대비)
            async with session.get(main_url, timeout=30) as r1:
                r1.raise_for_status()

            data = {
                "pageIndex": page_index,
                "menuCd": MENU_CD,
                "xmlText": "",
                "strOrgNo": org_no,     # 빈값이면 전체
                "strHokiS": hoki_s,     # 빈값이면 전체
                "strHokiE": hoki_e,     # 빈값이면 전체
                "strDateS": ds,
                "strDateE": de,
                "ptSignature": "",      # 캡처처럼 비워둠
            }

            post_headers = dict(headers)
            post_headers["Referer"] = main_url

            async with session.post(csv_url, data=data, headers=post_headers, timeout=120) as r2:
                r2.raise_for_status()
                content_type = (r2.headers.get("Content-Type", "") or "").lower()
                body = await r2.read()

            if "csv" not in content_type:
                print(f"\n[FAIL] ({idx}/{len(month_ranges)}) {ds}~{de} Content-Type={content_type}")
                print("응답 앞부분(300바이트):", body[:300])
            else:
                out_name = _sanitize_filename(f"south_pv_{tag}_{ds}-{de}.csv")
                out_path = OUTPUT_DIR / out_name
                out_path.write_bytes(body)
                print(f"[OK] ({idx}/{len(month_ranges)}) Saved: {out_path} ({len(body)} bytes)")

            if idx < len(month_ranges):
                print("...5초 대기 후 다음 구간 수집")
                await asyncio.sleep(5)


def main():
    asyncio.run(download_all_by_months())


if __name__ == "__main__":
    main()
