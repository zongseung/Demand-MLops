# Prefect 서버와 동일한 2.x 이미지 사용
FROM prefecthq/prefect:2-python3.10

# uv 설치 (추가 의존성 빠르게 설치용)
RUN pip install uv

# 작업 디렉토리
WORKDIR /mnt/nvme/open-stef

# pyproject.toml 복사
# ⚠️ pyproject.toml 안에서는 prefect / pydantic 의존성은 제거해두는 게 좋음
COPY pyproject.toml ./

# uv로 프로젝트 의존성 설치
# (--system: 현재 컨테이너의 전역 site-packages에 설치)
RUN uv pip install --system -e .

# 👉 Prefect + pydantic 2.11.x 버그 회피:
#    최종적으로 pydantic을 2.11 미만 버전으로 명시적으로 덮어쓰기
RUN pip install "pydantic<2.11"

# collect_demand 코드 복사
COPY collect_demand/ /mnt/nvme/open-stef/collect_demand/

# Prefect API URL 설정 (docker-compose에서 override 가능)
ENV PREFECT_API_URL=http://prefect-server-weather:4200/api

# 타임존 설정
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 기본 작업 디렉토리
WORKDIR /mnt/nvme/open-stef/collect_demand

# 기본 CMD: 배포 스크립트 실행
CMD ["python", "-m", "prefect_flows.deploy"]
