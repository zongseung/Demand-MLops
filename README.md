# Weather Pipeline
Prefect 2 기반으로 기상청 ASOS 시간을 수집하고 결측치를 보정해 CSV로 적재·머지하는 파이프라인입니다. Prefect Docker 워커가 flow 컨테이너를 띄워 실행합니다.

## 구조
- `fetch_data/collect_asos.py` 비동기 ASOS 수집 (직접 실행 가능)
- `fetch_data/impute_missing.py` 결측치 처리 유틸
- `prefect_flows/prefect_pipeline.py` 일일 ETL Flow (수집 → 보정 → 저장 → 통합 → Slack)
- `prefect_flows/deploy.py` Prefect 배포/워크풀 생성 스크립트
- `prefect_flows/merge_to_all.py` 새 CSV를 `data/asos_all_merged.csv`에 병합
- `docker/` Prefect 서버/워커/배포용 compose 및 Dockerfile
- `data/` 일별·통합 CSV 저장 위치

## 환경변수 (.env 루트에 배치)
- `SERVICE_KEY` 기상청 API 키
- `SLACK_WEBHOOK_URL` Slack Incoming Webhook (옵션)
- `PREFECT_API_URL` (기본 `http://prefect-server-new:4200/api` — compose와 동일)
- `PREFECT_DOCKER_NETWORK` (옵션, 기본 `weather-pipeline_prefect-new`)

## 로컬 실행 (Docker Compose)
```bash
cd docker
# 이미지 빌드 및 서버/워커/배포 컨테이너 기동
docker compose up -d --build

# Prefect 서버가 healthy 되면 배포 스크립트 재실행(필요 시)
docker compose run --rm weather-deployer
```

## 배포/워크풀 리셋이 필요할 때
```bash
docker compose run --rm weather-deployer prefect work-pool delete weather-new-pool || true
docker compose run --rm weather-deployer
```

## 수동 Flow 실행 (UI 대신 CLI)
```bash
# Prefect UI( http://localhost:4300 )에서 수동 트리거하거나,
# CLI로도 가능 (Prefect CLI가 호스트에 있다면)
prefect deployment run 'daily-weather-collection-new' -p target_date=20250101
```

## 개별 스크립트로 수집만 테스트
```bash
python fetch_data/collect_asos.py
# 입력 예: 20250101,20250101
```

## 생성물
- 일별 파일: `data/asos_YYYYMMDD_YYYYMMDD.csv`
- 통합 파일: `data/asos_all_merged.csv`
