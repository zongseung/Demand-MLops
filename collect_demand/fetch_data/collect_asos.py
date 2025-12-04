import pandas as pd
import aiohttp
import asyncio
import sys
from pathlib import Path

# 상위 디렉토리를 sys.path에 추가하여 fetch_data 모듈 임포트 가능하게 함

sys.path.insert(0, str(Path(__file__).parent.parent))

# 해당 fetch 데이터 모듈을 임포트
from fetch_data.impute_missing import impute_missing_values
from dotenv import load_dotenv

import os

##  api 키를 .env 파일에 저장 -> gitignore는 비공개
load_dotenv()
SERVICE_KEY = os.getenv("SERVICE_KEY")

API_URL = "https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

# CSV 파일 읽기
CSV_PATH = "/mnt/nvme/open-stef/collect_demand/station_list.csv"
stations = pd.read_csv(CSV_PATH)
station_ids = stations["지점"].astype(str).tolist()  # 문자열 리스트로 변환

async def fetch_city(session, city_id, start, end, max_retries=3):
    """단일 지점 데이터를 비동기 수집 (실패 시 5초 후 재시도)"""
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": "1",
        "numOfRows": "999",
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "HR",
        "startDt": start,
        "startHh": "00",
        "endDt": end,
        "endHh": "23",
        "stnIds": city_id
    }

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(API_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
                    if items:
                        print(f"{city_id}: 데이터 {len(items)}건 수집 완료")
                        return pd.DataFrame(items)
                    else:
                        print(f"{city_id}: 데이터 없음")
                        return pd.DataFrame()
                else:
                    print(f"{city_id}: 요청 실패 (시도 {attempt}/{max_retries}) 상태코드={response.status}")
        except Exception as e:
            print(f"{city_id}: 예외 발생 (시도 {attempt}/{max_retries}) → {e}")

        # 재시도 전 5초 대기
        if attempt < max_retries:
            print(f"{city_id}: 5초 후 재시도...")
            await asyncio.sleep(5)

    print(f"{city_id}: 3회 실패 — 포기")
    return pd.DataFrame()

async def select_data_async(city_list, start, end):
    """모든 지점 데이터를 비동기로 수집"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city(session, city, start, end) for city in city_list]
        results = await asyncio.gather(*tasks)
    return pd.concat(results, ignore_index=True)

if __name__ == "__main__":
    #
    start, end = input("날짜를 입력하세요, start, end = 'YYYYMMDD' 'YYYYMMDD' : ").split(',') # 데이터 넣는방법 -> 20250101, 20250131 이런식으로 대입하면 됨. 
    start = start.strip()
    end = end.strip()

    # 메인 비동기 루프 실행
    df = asyncio.run(select_data_async(station_ids, start, end))
    
    # 원하는 데이터 정보 저장
    df = df[["tm", "hm", "ta", "stnNm"]]
    
    # 결측치 처리 (원본 컬럼명 사용: ta, hm, tm, stnNm)
    print("\n결측치 처리 중...")
    result = impute_missing_values(df, columns=['ta', 'hm'], date_col='tm', station_col='stnNm', debug=True)
    if isinstance(result, tuple):
        df, debug_info = result
    else:
        df = result
    
    # 컬럼 이름 변경
    df.rename(columns={"tm": "date", 
                        "hm": "humidity", 
                        "ta": "temperature", 
                        "stnNm": "station_name"}, inplace=True)
    
    # 저장
    output_path = f"/mnt/nvme/open-stef/collect_demand/asos_{start}_{end}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n완료: {output_path}")
    print(df.head())
