"""
Heat Demand 데이터 마이그레이션 스크립트

CSV 파일과 JSON 파일에서 PostgreSQL로 데이터 적재
- heat_demand.csv → heat_demand 테이블 (~50만건)
- heat_demand_location.json → heat_demand_location 테이블 (19건)
"""

import asyncio
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import text, func
from sqlalchemy.dialects.postgresql import insert

from fetch_data.common.database import (
    get_async_session,
    init_db,
    HeatDemand,
    HeatDemandLocation,
)

# 프로젝트 루트 경로
PROJECT_ROOT = Path(__file__).parent.parent.parent

# CSV 컬럼명 매핑 (원본 → DB)
COLUMN_MAPPING = {
    "date": "timestamp",
    "branch": "branch",
    "wind direction": "wind_direction",
    "wind speed": "wind_speed",
    "rain_daily": "rain_daily",
    "rain_1_Hour": "rain_1h",
    "hummiditiy": "humidity",  # 원본 CSV의 오타
    "temperature_chill": "temperature_chill",
    "year": "year",
    "month": "month",
    "day": "day",
    "hour": "hour",
    "temperature": "temperature",
    "heat_demand": "heat_demand",
}

BATCH_SIZE = 2000  # asyncpg has 32767 param limit, 15 columns * 2000 = 30000


async def migrate_locations() -> int:
    """heat_demand_location.json → heat_demand_location 테이블"""
    json_path = PROJECT_ROOT / "heat_demand_location.json"

    if not json_path.exists():
        print(f"[ERROR] Location JSON not found: {json_path}")
        return 0

    with open(json_path, "r", encoding="utf-8") as f:
        locations = json.load(f)

    async with get_async_session() as session:
        for loc in locations:
            stmt = insert(HeatDemandLocation).values(
                name=loc["name"],
                address=loc.get("address"),
                latitude=loc["lat"],
                longitude=loc["lng"],
            ).on_conflict_do_update(
                index_elements=["name"],
                set_={
                    "address": loc.get("address"),
                    "latitude": loc["lat"],
                    "longitude": loc["lng"],
                }
            )
            await session.execute(stmt)
        await session.commit()

    print(f"[OK] Migrated {len(locations)} locations")
    return len(locations)


async def migrate_heat_demand() -> int:
    """heat_demand.csv → heat_demand 테이블"""
    csv_path = PROJECT_ROOT / "heat_demand.csv"

    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 0

    print(f"[INFO] Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path, index_col=0)

    # 컬럼명 매핑
    df = df.rename(columns=COLUMN_MAPPING)

    # timestamp 변환
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 필요한 컬럼만 선택
    columns = [
        "timestamp", "branch", "wind_direction", "wind_speed",
        "rain_daily", "rain_1h", "humidity", "temperature_chill",
        "year", "month", "day", "hour", "temperature", "heat_demand"
    ]
    df = df[columns]

    # NaN → None 변환
    df = df.where(pd.notnull(df), None)

    total_rows = len(df)
    print(f"[INFO] Total rows: {total_rows:,}")

    inserted = 0
    async with get_async_session() as session:
        for start in range(0, total_rows, BATCH_SIZE):
            end = min(start + BATCH_SIZE, total_rows)
            batch = df.iloc[start:end]

            records = batch.to_dict(orient="records")

            stmt = insert(HeatDemand).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["timestamp", "branch"],
                set_={
                    "wind_direction": stmt.excluded.wind_direction,
                    "wind_speed": stmt.excluded.wind_speed,
                    "rain_daily": stmt.excluded.rain_daily,
                    "rain_1h": stmt.excluded.rain_1h,
                    "humidity": stmt.excluded.humidity,
                    "temperature_chill": stmt.excluded.temperature_chill,
                    "temperature": stmt.excluded.temperature,
                    "year": stmt.excluded.year,
                    "month": stmt.excluded.month,
                    "day": stmt.excluded.day,
                    "hour": stmt.excluded.hour,
                    "heat_demand": stmt.excluded.heat_demand,
                    "updated_at": func.now(),
                }
            )
            await session.execute(stmt)
            await session.commit()

            inserted += len(records)
            if inserted % 50000 == 0 or inserted == total_rows:
                print(f"[PROGRESS] {inserted:,} / {total_rows:,} ({100*inserted/total_rows:.1f}%)")

    print(f"[OK] Migrated {inserted:,} heat_demand records")
    return inserted


async def verify_migration():
    """마이그레이션 결과 검증"""
    async with get_async_session() as session:
        # 위치 데이터 확인
        result = await session.execute(
            text("SELECT COUNT(*) FROM heat_demand_location")
        )
        loc_count = result.scalar()

        # 열수요 데이터 확인
        result = await session.execute(
            text("SELECT COUNT(*) FROM heat_demand")
        )
        hd_count = result.scalar()

        # 지역별 데이터 건수
        result = await session.execute(
            text("SELECT branch, COUNT(*) as cnt FROM heat_demand GROUP BY branch ORDER BY branch")
        )
        branch_stats = result.fetchall()

        print("\n=== Migration Verification ===")
        print(f"heat_demand_location: {loc_count} rows")
        print(f"heat_demand: {hd_count:,} rows")
        print("\nBranch distribution:")
        for branch, cnt in branch_stats:
            print(f"  {branch}: {cnt:,}")


async def main():
    print("=== Heat Demand Migration ===\n")

    # 1. 테이블 생성
    print("[Step 1] Creating tables...")
    await init_db()

    # 2. 위치 데이터 적재
    print("\n[Step 2] Migrating locations...")
    await migrate_locations()

    # 3. 열수요 데이터 적재
    print("\n[Step 3] Migrating heat_demand data...")
    await migrate_heat_demand()

    # 4. 검증
    await verify_migration()

    print("\n=== Migration Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
