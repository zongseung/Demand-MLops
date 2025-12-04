# 일일 기상 데이터 수집 자동화

매일 오전 9시에 전날 기상 데이터를 자동으로 수집, 처리, 저장하는 Prefect 플로우입니다.

## 구성

- **Prefect Server**: 플로우 관리 및 스케줄링
- **PostgreSQL**: Prefect 메타데이터 저장
- **Weather Collector**: 기상 데이터 수집 워커

## 실행 방법

### 1. Docker Compose로 시작

```bash
cd /mnt/nvme/open-stef/collect_demand/docker
docker-compose up -d
```

### 2. Prefect UI 접속

브라우저에서 `http://localhost:4200` 접속

### 3. 배포 확인

```bash
# 배포 상태 확인
docker logs weather-collector

# Prefect 워커 시작 (필요 시)
docker exec -it weather-collector bash
prefect worker start --pool default-agent-pool --work-queue weather-queue
```

## 수동 실행

특정 날짜 데이터를 수동으로 수집하려면:

```bash
docker exec -it weather-collector python -c "
import asyncio
from prefect_flows import daily_weather_collection_flow
asyncio.run(daily_weather_collection_flow('20241203'))
"
```

## 스케줄

- **시간**: 매일 오전 9시 (Asia/Seoul)
- **동작**: 전날(실행일 - 1일) 데이터를 수집
- **예시**: 12월 4일 09:00에 실행 → 12월 3일 데이터 수집

## 중단

```bash
docker-compose down
```

## 데이터 삭제 포함 중단

```bash
docker-compose down -v
```

