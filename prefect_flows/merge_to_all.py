"""
매일 수집된 데이터를 asos_all_merged.csv에 추가하는 스크립트
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 & data 디렉토리 기준 경로들
BASE_DIR = Path(__file__).resolve().parents[1]   # /app/fetch_data/merge_to_all.py -> /app
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_MERGED_CSV = DATA_DIR / "asos_all_merged.csv"


def merge_to_all_csv(new_csv_path: str | Path,
                     merged_csv_path: str | Path = DEFAULT_MERGED_CSV):
    """
    새로 수집된 CSV 파일을 asos_all_merged.csv에 추가합니다.
    
    Parameters
    ----------
    new_csv_path : str | Path
        새로 수집된 CSV 파일 경로
    merged_csv_path : str | Path
        통합 CSV 파일 경로 (기본값: data/asos_all_merged.csv)
    """
    new_csv_path = Path(new_csv_path)
    merged_csv_path = Path(merged_csv_path)

    print(f"\n{'='*80}")
    print(f"데이터 통합 시작")
    print(f"{'='*80}")
    print(f"새 파일: {new_csv_path}")
    print(f"통합 파일: {merged_csv_path}")
    
    # 새 파일 읽기
    if not new_csv_path.exists():
        raise FileNotFoundError(f"새 파일을 찾을 수 없습니다: {new_csv_path}")
    
    print(f"\n새 파일 읽는 중...")
    df_new = pd.read_csv(new_csv_path, encoding="utf-8-sig")
    print(f"새 데이터: {len(df_new)}건")
    
    merged_file = merged_csv_path
    
    if merged_file.exists():
        print(f"\n기존 통합 파일 읽는 중...")
        df_merged = pd.read_csv(merged_csv_path, encoding="utf-8-sig")
        print(f"기존 데이터: {len(df_merged)}건")
        
        # 날짜 컬럼을 datetime으로 변환
        if "date" in df_merged.columns:
            if df_merged["date"].dtype == "object":
                df_merged["date"] = pd.to_datetime(df_merged["date"], format="mixed")
        if "date" in df_new.columns:
            if df_new["date"].dtype == "object":
                df_new["date"] = pd.to_datetime(df_new["date"], format="mixed")
        
        # 중복 체크: 새 데이터에서 기존에 없는 것만 필터링
        print(f"\n중복 체크 중...")
        if "hour" in df_new.columns and "date" in df_new.columns and "station_name" in df_new.columns:
            # hour 컬럼이 있으면 date + station_name + hour 기준
            subset_cols = ["date", "station_name", "hour"]
            print(f"중복 기준: {subset_cols}")
            
            # 기존 데이터의 키 조합 생성
            df_merged_keys = df_merged[subset_cols].drop_duplicates()
            
            # 새 데이터에서 기존에 없는 것만 필터링
            df_new_merged = df_new.merge(
                df_merged_keys,
                on=subset_cols,
                how="left",
                indicator=True
            )
            df_new_unique = df_new_merged[df_new_merged["_merge"] == "left_only"].drop(columns=["_merge"])
            
        elif "date" in df_new.columns and "station_name" in df_new.columns:
            # hour 없으면 date + station_name 기준
            subset_cols = ["date", "station_name"]
            print(f"중복 기준: {subset_cols}")
            
            # 기존 데이터의 키 조합 생성
            df_merged_keys = df_merged[subset_cols].drop_duplicates()
            
            # 새 데이터에서 기존에 없는 것만 필터링
            df_new_merged = df_new.merge(
                df_merged_keys,
                on=subset_cols,
                how="left",
                indicator=True
            )
            df_new_unique = df_new_merged[df_new_merged["_merge"] == "left_only"].drop(columns=["_merge"])
        else:
            print("경고: 'date' 또는 'station_name' 컬럼이 없습니다. 중복 체크를 건너뜁니다.")
            df_new_unique = df_new.copy()
        
        skipped_count = len(df_new) - len(df_new_unique)
        print(f"새 데이터: {len(df_new)}건 → 중복 스킵: {skipped_count}건 → 추가할 데이터: {len(df_new_unique)}건")
        
        if len(df_new_unique) > 0:
            # 중복 없는 새 데이터만 추가
            df_combined = pd.concat([df_merged, df_new_unique], ignore_index=True)
            print(f"통합 데이터: {len(df_combined)}건 (기존 {len(df_merged)}건 + 신규 {len(df_new_unique)}건)")
        else:
            print("추가할 새 데이터가 없습니다. 기존 데이터를 유지합니다.")
            df_combined = df_merged.copy()
    else:
        print(f"\n통합 파일이 없습니다. 새로 생성합니다.")
        df_combined = df_new.copy()
    
    # 날짜 순 정렬
    if "date" in df_combined.columns:
        df_combined["date"] = pd.to_datetime(df_combined["date"], format="mixed")

    
    print(f"\n통합 파일 저장 중...")
    df_combined.to_csv(merged_csv_path, index=False, encoding="utf-8-sig")
    
    print(f"\n{'='*80}")
    print(f"통합 완료!")
    print(f"저장 경로: {merged_csv_path}")
    print(f"총 데이터: {len(df_combined)}건")
    print(f"저장 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    return str(merged_csv_path)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python merge_to_all.py <새_CSV_파일_경로> [통합_CSV_파일_경로]")
        print("\n예시:")
        print("  python merge_to_all.py data/asos_20241203_20241203.csv")
        sys.exit(1)
    
    new_csv = sys.argv[1]
    merged_csv = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_MERGED_CSV
    merge_to_all_csv(new_csv, merged_csv)
