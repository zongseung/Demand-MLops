# 기상 데이터 수집 시스템 (Collect Demand)

기상청 ASOS(종관기상관측) 데이터를 자동으로 수집, 처리, 저장하는 Prefect 기반 자동화 시스템입니다.

## 📋 목차

- [개요](#개요)
- [주요 기능](#주요-기능)
- [프로젝트 구조](#프로젝트-구조)
- [시작하기](#시작하기)
- [사용 방법](#사용-방법)
- [데이터 구조](#데이터-구조)
- [설정](#설정)

## 개요

이 프로젝트는 기상청 공공데이터 API를 활용하여 전국 지점의 시간별 기상 데이터를 자동으로 수집하고, 결측치를 처리한 후 CSV 파일로 저장하는 시스템입니다.

- **자동 스케줄링**: Prefect를 사용하여 매일 오전 9시에 전날 데이터를 자동 수집
- **결측치 처리**: 스플라인 보간 및 통계적 대체 방법으로 결측치 처리
- **데이터 통합**: 수집된 데이터를 자동으로 통합 파일에 병합
- **Docker 기반**: Docker Compose를 사용한 간편한 배포 및 관리

## 주요 기능

### 1. 데이터 수집 (`fetch_data/collect_asos.py`)
- 기상청 ASOS 시간별 데이터 API를 사용한 비동기 데이터 수집
- 전국 지점의 시간별 온도, 습도 데이터 수집
- 재시도 로직을 통한 안정적인 데이터 수집

### 2. 결측치 처리 (`fetch_data/impute_missing.py`)
- 연속 3개 이하 결측치: 스플라인 보간
- 연속 4개 이상 결측치: 같은 지역의 다른 연도 동일 월-일-시 평균값 사용

### 3. 자동화 플로우 (`prefect_flows/`)
- Prefect를 사용한 워크플로우 관리
- 매일 오전 9시 자동 실행
- 데이터 수집 → 결측치 처리 → 저장 → 통합 파일 병합

## 프로젝트 구조

```
collect_demand/
├── fetch_data/              # 데이터 수집 및 처리 모듈
│   ├── __init__.py
│   ├── collect_asos.py      # 기상청 API 데이터 수집
│   └── impute_missing.py    # 결측치 처리 로직
│
├── prefect_flows/           # Prefect 워크플로우
│   ├── __init__.py
│   ├── prefect_pipeline.py  # 메인 플로우 정의
│   ├── deploy.py            # Prefect 배포 스크립트
│   ├── merge_to_all.py      # 데이터 통합 함수
│   └── README.md            # Prefect 관련 문서
│
├── docker/                  # Docker 설정
│   ├── Dockerfile           # Weather Collector 이미지 정의
│   ├── docker-compose.yml   # 전체 시스템 구성
│   └── README.md            # Docker 관련 문서
│
├── station_list.csv         # 관측 지점 목록
├── asos_all_merged.csv      # 통합 데이터 파일 (자동 생성)
└── README.md                # 이 파일
```

## 시작하기

### 사전 요구사항

- Docker 및 Docker Compose
- 기상청 공공데이터 API 키 (`.env` 파일에 `SERVICE_KEY`로 저장)

### 1. 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 API 키를 설정합니다:

```bash
SERVICE_KEY=your_api_key_here
```

### 2. Docker Compose로 시스템 시작

```bash
cd /mnt/nvme/open-stef/collect_demand/docker
docker-compose up -d
```

이 명령어는 다음 서비스들을 시작합니다:
- **prefect-server-weather**: Prefect 서버 (포트 4204)
- **postgres-weather**: PostgreSQL 데이터베이스
- **weather-collector**: 배포 스크립트 실행 컨테이너
- **weather-worker**: Prefect 워커 (Docker work pool 사용)

### 3. Prefect UI 접속

브라우저에서 `http://localhost:4204` 접속하여 플로우 실행 상태를 확인할 수 있습니다.

### 4. 로그 확인

```bash
# 전체 로그 확인
docker-compose logs -f

# 특정 서비스 로그 확인
docker-compose logs -f weather-collector
docker-compose logs -f weather-worker
docker-compose logs -f prefect-server-weather
```

## 사용 방법

### 자동 실행

시스템이 시작되면 자동으로:
1. Prefect 배포가 생성됩니다 (`daily-weather-collection`)
2. 매일 오전 9시(Asia/Seoul)에 전날 데이터를 수집합니다
3. 수집된 데이터는 `asos_YYYYMMDD_YYYYMMDD.csv` 형식으로 저장됩니다
4. 통합 파일 `asos_all_merged.csv`에 자동으로 병합됩니다

### 수동 실행

특정 날짜의 데이터를 수동으로 수집하려면:

```bash
# 컨테이너 내부에서 실행
docker exec -it weather-collector python -c "
import asyncio
from prefect_flows.prefect_pipeline import daily_weather_collection_flow
asyncio.run(daily_weather_collection_flow('20241203'))
"
```

또는 로컬에서 직접 실행:

```bash
# 프로젝트 루트에서
cd /mnt/nvme/open-stef/collect_demand
python -m prefect_flows.prefect_pipeline
```

### 스크립트 직접 실행

`collect_asos.py`를 직접 실행하여 데이터를 수집할 수 있습니다:

```bash
cd /mnt/nvme/open-stef
uv run collect_demand/fetch_data/collect_asos.py
# 날짜 입력: 20241203, 20241203
```

## 데이터 구조

### 수집 데이터 컬럼

- `date`: 날짜 및 시간 (YYYY-MM-DD HH:MM:SS)
- `station_name`: 관측 지점명
- `temperature`: 기온 (°C)
- `humidity`: 상대습도 (%)
- `hour`: 시간 (0-23)

### 출력 파일

- **개별 파일**: `asos_YYYYMMDD_YYYYMMDD.csv`
  - 특정 날짜의 수집 데이터
- **통합 파일**: `asos_all_merged.csv`
  - 모든 수집 데이터를 통합한 파일
  - 중복 제거 및 날짜순 정렬

## 설정

### 스케줄 변경

`prefect_flows/deploy.py` 파일에서 스케줄을 변경할 수 있습니다:

```python
CronSchedule(
    cron="0 9 * * *",  # 매일 오전 9시
    timezone="Asia/Seoul",
)
```

### 관측 지점 변경

`station_list.csv` 파일을 수정하여 수집할 지점을 변경할 수 있습니다.

### 포트 변경

`docker/docker-compose.yml`에서 Prefect 서버 포트를 변경할 수 있습니다:

```yaml
ports:
  - "4204:4200"  # 호스트:컨테이너
```

## 중단 및 재시작

### 시스템 중단

```bash
cd /mnt/nvme/open-stef/collect_demand/docker
docker-compose stop
```

### 완전 중단 (컨테이너 제거)

```bash
docker-compose down
```

### 데이터 삭제 포함 중단

```bash
docker-compose down -v
```

⚠️ **주의**: 이 명령어는 PostgreSQL 볼륨까지 삭제하므로 Prefect 메타데이터가 모두 삭제됩니다.

### 재시작

```bash
docker-compose up -d
```

## 문제 해결

### Prefect 워커가 실행되지 않는 경우

```bash
# 워커 로그 확인
docker logs weather-worker

# 워커 재시작
docker restart weather-worker
```

### 배포가 생성되지 않는 경우

```bash
# 배포 스크립트 로그 확인
docker logs weather-collector

# 수동 배포 실행
docker exec -it weather-collector python -m prefect_flows.deploy
```

### API 연결 오류

- `.env` 파일에 `SERVICE_KEY`가 올바르게 설정되어 있는지 확인
- API 키가 유효한지 확인
- 네트워크 연결 상태 확인

## 참고 자료

- [Prefect 문서](https://docs.prefect.io/)
- [기상청 공공데이터 API](https://www.data.go.kr/)
- [Docker Compose 문서](https://docs.docker.com/compose/)

## 라이선스 

이 프로젝트는 개인/내부 사용 목적으로 개발되었습니다.