import sys
from pathlib import Path
from datetime import datetime, timedelta

from prefect import task, flow
import pandas as pd

# 상위 디렉토리를 sys.path에 추가하여 fetch_data 모듈 임포트 가능하게 함
# 프로젝트를 패키지로 설치해두었다면 이 부분 없이
# from fetch_data.collect_asos import ... 형태로 바로 임포트할 수도 있음
sys.path.insert(0, str(Path(__file__).parent.parent))

from fetch_data.collect_asos import select_data_async, station_ids
from fetch_data.impute_missing import impute_missing_values
from prefect_flows.merge_to_all import merge_to_all_csv


@task(name="데이터 수집", retries=3, retry_delay_seconds=300)
async def collect_weather_data(date_str: str) -> pd.DataFrame:
    """
    특정 날짜의 기상 데이터를 수집합니다.

    Parameters
    ----------
    date_str : str
        수집할 날짜 (YYYYMMDD 형식)
    """
    print("\n" + "=" * 80)
    print(f"날짜 {date_str} 데이터 수집 시작")
    print("=" * 80 + "\n")

    # 비동기 데이터 수집
    df = await select_data_async(station_ids, date_str, date_str)

    # 필요한 컬럼만 선택 (없는 경우 KeyError 방지용 체크)
    needed_cols = ["tm", "hm", "ta", "stnNm"]
    missing_cols = [c for c in needed_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"수집 데이터에 필요한 컬럼이 없습니다: {missing_cols}")

    df = df[needed_cols]

    print(f"\n수집된 데이터: {len(df)}건")

    return df


@task(name="결측치 처리", retries=2)
def process_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    결측치를 처리합니다.

    - 연속 3개 이하: 스플라인 보간
    - 연속 4개 이상: 같은 지역의 다른 연도 동일 월-일-시 평균값
    """
    print("\n결측치 처리 시작...")

    result = impute_missing_values(
        df,
        columns=["ta", "hm"],
        date_col="tm",
        station_col="stnNm",
        debug=True,  # 로그가 너무 많다면 False로 변경 가능
    )

    if isinstance(result, tuple):
        df_imputed, debug_info = result
    else:
        df_imputed = result

    # 컬럼 이름 변경
    df_imputed = df_imputed.rename(
        columns={
            "tm": "date",
            "hm": "humidity",
            "ta": "temperature",
            "stnNm": "station_name",
        }
    )

    return df_imputed


@task(name="데이터 저장", retries=2)
def save_data(
    df: pd.DataFrame,
    date_str: str,
    output_dir: str = "/mnt/nvme/open-stef/collect_demand",
) -> str:
    """
    처리된 데이터를 CSV 파일로 저장합니다.

    Parameters
    ----------
    df : pd.DataFrame
        저장할 데이터프레임
    date_str : str
        기준 날짜 (YYYYMMDD)
    output_dir : str
        저장 디렉토리

    Returns
    -------
    output_path : str
        저장된 CSV 경로
    """
    output_path = f"{output_dir}/asos_{date_str}_{date_str}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 80)
    print(f"저장 완료: {output_path}")
    print(f"데이터 shape: {df.shape}")
    print("=" * 80 + "\n")

    return output_path


@task(name="통합 파일에 추가", retries=2)
def merge_to_all(output_path: str) -> str:
    """
    새로 저장된 CSV 파일을 asos_all_merged.csv에 추가합니다.
    """
    merged_path = merge_to_all_csv(output_path)
    return merged_path


@flow(name="daily-weather-collection-flow")
def daily_weather_collection_flow(target_date=None):
    """
    매일 전날 기상 데이터를 수집, 처리, 저장하는 플로우

    Parameters
    ----------
    target_date : str, optional
        수집할 날짜 (YYYYMMDD 형식).
        None이면 "실행 시점 기준 전날" 데이터를 수집합니다.

    Returns
    -------
    output_path : str
        새로 생성된 개별 CSV 파일 경로
    """
    # 수집 대상 날짜 설정
    if target_date is None:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")

    print("\n" + "=" * 80)
    print("일일 기상 데이터 수집 플로우 시작")
    print(f"수집 대상 날짜: {target_date}")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")

    # 1. 데이터 수집 (비동기 task 이므로 submit 사용)
    df_future = collect_weather_data.submit(target_date)

    # 2. 결측치 처리
    df_processed_future = process_missing_values.submit(df_future)

    # 3. 데이터 저장
    output_path_future = save_data.submit(df_processed_future, target_date)

    # 4. 통합 파일에 추가
    merged_path_future = merge_to_all.submit(output_path_future)

    # Prefect UI에서는 Future로 관리되지만, 로컬 디버깅 시에는 .result()로 값 꺼낼 수 있음
    output_path = output_path_future.result()
    merged_path = merged_path_future.result()

    print("\n플로우 완료!")
    print(f"개별 파일: {output_path}")
    print(f"통합 파일: {merged_path}")

    return output_path
