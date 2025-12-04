"""
매일 수집된 데이터를 asos_all_merged.csv에 추가하는 스크립트
"""
import pandas as pd
from pathlib import Path
from datetime import datetime


def merge_to_all_csv(new_csv_path: str, merged_csv_path: str = "/mnt/nvme/open-stef/collect_demand/asos_all_merged.csv"):
    """
    새로 수집된 CSV 파일을 asos_all_merged.csv에 추가합니다.
    
    Parameters:
    -----------
    new_csv_path : str
        새로 수집된 CSV 파일 경로
    merged_csv_path : str
        통합 CSV 파일 경로 (기본값: asos_all_merged.csv)
    
    Returns:
    --------
    str
        저장된 파일 경로
    """
    print(f"\n{'='*80}")
    print(f"데이터 통합 시작")
    print(f"{'='*80}")
    print(f"새 파일: {new_csv_path}")
    print(f"통합 파일: {merged_csv_path}")
    
    # 새 파일 읽기
    if not Path(new_csv_path).exists():
        raise FileNotFoundError(f"새 파일을 찾을 수 없습니다: {new_csv_path}")
    
    print(f"\n새 파일 읽는 중...")
    df_new = pd.read_csv(new_csv_path, encoding='utf-8-sig')
    print(f"새 데이터: {len(df_new)}건")
    
    # 통합 파일이 존재하는지 확인
    merged_file = Path(merged_csv_path)
    
    if merged_file.exists():
        print(f"\n기존 통합 파일 읽는 중...")
        df_merged = pd.read_csv(merged_csv_path, encoding='utf-8-sig')
        print(f"기존 데이터: {len(df_merged)}건")
        
        # 두 데이터프레임 합치기
        print(f"\n데이터 합치는 중...")
        df_combined = pd.concat([df_merged, df_new], ignore_index=True)
        print(f"합친 데이터: {len(df_combined)}건")
        
        # 중복 제거 (date, station_name, hour가 모두 같은 경우)
        # hour 컬럼이 있는지 확인
        if 'hour' in df_combined.columns:
            # date와 hour를 합쳐서 datetime으로 변환
            if 'date' in df_combined.columns:
                # date 컬럼이 datetime 형식인지 확인
                if df_combined['date'].dtype == 'object':
                    df_combined['date'] = pd.to_datetime(df_combined['date'])
                
                # 중복 제거: date, station_name, hour 기준
                print(f"\n중복 제거 중... (date, station_name, hour 기준)")
                before_dedup = len(df_combined)
                df_combined = df_combined.drop_duplicates(
                    subset=['date', 'station_name', 'hour'],
                    keep='last'  # 최신 데이터 유지
                )
                after_dedup = len(df_combined)
                print(f"중복 제거 전: {before_dedup}건 → 중복 제거 후: {after_dedup}건 (제거: {before_dedup - after_dedup}건)")
            else:
                print("경고: 'date' 컬럼이 없습니다. 중복 제거를 건너뜁니다.")
        else:
            # hour 컬럼이 없으면 date와 station_name만으로 중복 제거
            if 'date' in df_combined.columns:
                print(f"\n중복 제거 중... (date, station_name 기준)")
                before_dedup = len(df_combined)
                df_combined = df_combined.drop_duplicates(
                    subset=['date', 'station_name'],
                    keep='last'
                )
                after_dedup = len(df_combined)
                print(f"중복 제거 전: {before_dedup}건 → 중복 제거 후: {after_dedup}건 (제거: {before_dedup - after_dedup}건)")
            else:
                print("경고: 'date' 컬럼이 없습니다. 중복 제거를 건너뜁니다.")
        
        # 날짜순으로 정렬
        if 'date' in df_combined.columns:
            print(f"\n날짜순 정렬 중...")
            df_combined['date'] = pd.to_datetime(df_combined['date'])
            df_combined = df_combined.sort_values(by=['date', 'station_name']).reset_index(drop=True)
        
    else:
        print(f"\n통합 파일이 없습니다. 새로 생성합니다.")
        df_combined = df_new.copy()
        
        # 날짜순으로 정렬
        if 'date' in df_combined.columns:
            df_combined['date'] = pd.to_datetime(df_combined['date'])
            df_combined = df_combined.sort_values(by=['date', 'station_name']).reset_index(drop=True)
    
    # 저장
    print(f"\n통합 파일 저장 중...")
    df_combined.to_csv(merged_csv_path, index=False, encoding='utf-8-sig')
    
    print(f"\n{'='*80}")
    print(f"통합 완료!")
    print(f"저장 경로: {merged_csv_path}")
    print(f"총 데이터: {len(df_combined)}건")
    print(f"저장 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    return merged_csv_path


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python merge_to_all.py <새_CSV_파일_경로> [통합_CSV_파일_경로]")
        print("\n예시:")
        print("  python merge_to_all.py /mnt/nvme/open-stef/collect_demand/asos_20241203_20241203.csv")
        sys.exit(1)
    
    new_csv = sys.argv[1]
    merged_csv = sys.argv[2] if len(sys.argv) > 2 else "/mnt/nvme/open-stef/collect_demand/asos_all_merged.csv"
    
    merge_to_all_csv(new_csv, merged_csv)


