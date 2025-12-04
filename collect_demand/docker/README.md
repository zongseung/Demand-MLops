# Docker 설정

Prefect 기반 기상 데이터 수집 시스템의 Docker 설정입니다.

## 파일 구조

- `Dockerfile`: Weather Collector 컨테이너 이미지 정의
- `docker-compose.yml`: 전체 시스템 구성 (Prefect Server, PostgreSQL, Weather Collector)

## 실행 방법

### 1. Docker Compose로 시작

```bash
cd /mnt/nvme/open-stef/collect_demand/docker
docker-compose up -d
```

### 2. 로그 확인

```bash
# 전체 로그
docker-compose logs -f

# 특정 서비스 로그
docker-compose logs -f weather-collector
docker-compose logs -f prefect-server
```

### 3. Prefect UI 접속

브라우저에서 `http://localhost:4204` 접속

### 4. 컨테이너 상태 확인

```bash
docker-compose ps
```

## 중단

```bash
# 컨테이너 중단
docker-compose stop

# 컨테이너 중단 및 제거
docker-compose down

# 볼륨까지 삭제 (데이터 삭제)
docker-compose down -v
```

## 재빌드

코드 변경 후 이미지를 재빌드하려면:

```bash
docker-compose build weather-collector
docker-compose up -d
```

또는

```bash
docker-compose up -d --build
```

## 컨테이너 내부 접속

```bash
# Weather Collector 컨테이너 접속
docker exec -it weather-collector bash

# Prefect Server 컨테이너 접속
docker exec -it prefect-server bash
```

