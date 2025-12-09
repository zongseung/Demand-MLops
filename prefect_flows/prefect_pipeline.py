import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

from prefect import task, flow
import pandas as pd
import requests

# 상위 디렉토리를 sys.path에 추가하여 fetch_data 모듈 임포트 가능하게 함
# 프로젝트를 패키지로 설치해두었다면 이 부분 없이
# from fetch_data.collect_asos import ... 형태로 바로 임포트할 수도 있음
sys.path.insert(0, str(Path(__file__).parent.parent))

from fetch_data.collect_asos import select_data_async, station_ids
from fetch_data.impute_missing import impute_missing_values
from prefect_flows.merge_to_all import merge_to_all_csv


# ==============================
# Slack 알림 유틸리티
# ==============================

def send_slack_message(text: str, webhook_url: str | None = None):
    """
    Slack Incoming Webhook으로 단순 텍스트 메시지 전송
    """
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다. Slack 전송 스킵.")
        return

    payload = {"text": text}

    try:
        resp = requests.post(
            webhook_url,
            json=payload,
            timeout=5,
        )
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}, {resp.text}")
    except Exception as e:
        print(f"Slack 전송 중 예외 발생: {e}")


@task(name="Slack 성공 알림", retries=0)
def notify_slack_success(date_str: str, output_path: str, merged_path: str):
    msg = (
        f"[Weather ETL 완료]\n"
        f"- 대상 날짜: {date_str}\n"
        f"- 개별 파일: {output_path}\n"
        f"- 통합 파일: {merged_path}"
    )
    send_slack_message(msg)


@task(name="Slack 실패 알림", retries=0)
def notify_slack_failure(date_str: str, error_msg: str):
    msg = (
        f"[Weather ETL 실패]\n"
        f"- 대상 날짜: {date_str}\n"
        f"- 에러: {error_msg}"
    )
    send_slack_message(msg)


# ==============================
# ETL Tasks
# ==============================

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

    # 수집된 데이터가 없는 경우 처리
    if df.empty:
        raise ValueError(
            f"날짜 {date_str}에 대해 수집된 데이터가 없습니다. "
            "API 키, 네트워크 연결 상태, 또는 해당 날짜의 데이터 제공 여부를 확인해주세요."
        )

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
    output_dir: str = "/app/data",
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


# ==============================
# Prefect Flow
# ==============================

def normalize_date_format(date_str: str) -> str:
    """
    날짜 문자열을 YYYYMMDD 형식으로 변환합니다.

    지원 형식:
    - YYYYMMDD (그대로 반환)
    - YYYY-MM-DD (하이픈 제거)
    - YYYY/MM/DD (슬래시 제거)
    """
    # 하이픈, 슬래시 제거
    normalized = date_str.replace("-", "").replace("/", "")

    # 8자리 숫자인지 확인
    if len(normalized) != 8 or not normalized.isdigit():
        raise ValueError(f"날짜 형식이 올바르지 않습니다: {date_str}. YYYYMMDD 또는 YYYY-MM-DD 형식이어야 합니다.")

    return normalized


@flow(name="daily-weather-collection-flow", log_prints=True)
def daily_weather_collection_flow(target_date: str | None = None):
    """
    매일 전날 기상 데이터를 수집, 처리, 저장하는 플로우

    Parameters
    ----------
    target_date : str, optional
        수집할 날짜 (YYYYMMDD 또는 YYYY-MM-DD 형식).
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
    else:
        # 날짜 형식 정규화 (YYYY-MM-DD → YYYYMMDD)
        target_date = normalize_date_format(target_date)

    print("\n" + "=" * 80)
    print("일일 기상 데이터 수집 플로우 시작")
    print(f"수집 대상 날짜: {target_date}")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")

    try:
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

        # 5. Slack 성공 알림
        notify_slack_success.submit(target_date, output_path, merged_path)

        return output_path

    except Exception as e:
        # 에러 메시지 정리
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n플로우 실행 중 에러 발생: {error_msg}")

        # Slack 실패 알림
        notify_slack_failure.submit(target_date, error_msg)

        # Prefect 상에서는 실패로 남기기 위해 예외 재발생
        raise
