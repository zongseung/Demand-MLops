import asyncio
import os
import time

from prefect.client.orchestration import get_client
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.utilities.importtools import import_object


# =======================================================================
# Prefect API / Docker 네트워크 설정
# =======================================================================

# Prefect 서버 URL (서버 컨테이너 이름 + 포트)
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server-new:4200/api")

# docker network 이름
#   docker compose를 리포지토리 루트(예: /mnt/nvme/weather-pipeline)에서 올리면
#   생성되는 네트워크 이름은 보통 `weather-pipeline_prefect-new` 입니다.
#   다를 경우 PREFECT_DOCKER_NETWORK 환경변수로 덮어써 주세요.
DOCKER_NETWORK = os.getenv("PREFECT_DOCKER_NETWORK", "weather-pipeline_prefect-new")

# 데이터 포털 API 키
SERVICE_KEY = os.getenv("SERVICE_KEY")
if not SERVICE_KEY:
    raise ValueError("SERVICE_KEY 환경변수를 찾을 수 없습니다.")


# 이 스크립트가 실행되는 컨테이너(weather-deployer)에서도 API URL 기본값으로 사용
os.environ.setdefault("PREFECT_API_URL", PREFECT_API_URL)


async def wait_for_api(timeout: int = 120) -> None:
    """Prefect API 살아날 때까지 대기"""
    start = time.time()
    while True:
        try:
            async with get_client() as client:
                await client.api_healthcheck()
            print("✅ Prefect API 연결 성공")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("❌ Prefect API 연결 시간 초과") from e
            print(f"Prefect API 대기 중... ({e!r})")
            await asyncio.sleep(5)


async def ensure_work_pool(pool_name: str = "weather-new-pool") -> None:
    """docker 타입 work pool 없으면 생성"""
    async with get_client() as client:
        try:
            pool = await client.read_work_pool(work_pool_name=pool_name)
            print(f"Work pool '{pool_name}'이 이미 존재합니다. (타입: {pool.type})")
        except Exception:
            base_job_template = {
                "job_configuration": {
                    "image": "weather-pipeline:latest",
                    "env": {
                        "PREFECT_API_URL": PREFECT_API_URL,
                        "SERVICE_KEY": SERVICE_KEY,
                        "TZ": "Asia/Seoul",
                    },
                    "networks": [DOCKER_NETWORK],
                },
                "variables": {
                    "required": ["image"],
                    "properties": {
                        "image": {
                            "title": "Image",
                            "description": "Docker image to use",
                            "type": "string",
                            "default": "weather-pipeline:latest",
                        }
                    },
                },
            }

            await client.create_work_pool(
                WorkPoolCreate(
                    name=pool_name,
                    type="docker",
                    description="새 일일 기상 수집용 Docker 워크 풀",
                    base_job_template=base_job_template,
                )
            )
            print(f"✅ Work pool '{pool_name}' 생성 완료 (image=weather-pipeline:latest)")


async def create_deployment() -> None:
    """배포 생성 + 스케줄 등록"""
    await wait_for_api()
    await ensure_work_pool("weather-new-pool")

    # flow 객체 임포트
    flow = import_object(
        "prefect_flows.prefect_pipeline.daily_weather_collection_flow"
    )

    # Docker 인프라 설정
    infra_overrides = {
        # flow 컨테이너 이미지
        "image": "weather-pipeline:latest",
        "image_pull_policy": "Never",  # 항상 로컬 이미지 사용
        "auto_remove": True,
        # 🔥 flow 컨테이너 환경변수 주입
        "env": {
            "PREFECT_API_URL": PREFECT_API_URL,
            "SERVICE_KEY": SERVICE_KEY,
            "TZ": "Asia/Seoul",
        },
        # 🔥 flow 컨테이너를 compose 네트워크에 붙임
        "networks": [DOCKER_NETWORK],
        # 🔥 호스트 데이터 디렉토리를 컨테이너에 마운트
        "volumes": ["/mnt/nvme/weather-pipeline/data:/app/data"],
    }

    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="daily-weather-collection-new",
        work_pool_name="weather-new-pool",
        path="/app",  # 💾 소스 코드가 이미지 안에 있는 경로
        entrypoint="prefect_flows/prefect_pipeline.py:daily_weather_collection_flow",  # 🚀 실행 경로
        parameters={"target_date": None},  # 기본은 전날
        schedules=[
            CronSchedule(
                cron="0 9 * * *",  # 매일 오전 9시
                timezone="Asia/Seoul",
            )
        ],
        tags=["weather", "daily", "new"],
        description="(NEW) 매일 오전 9시에 전날 기상 데이터를 수집, 처리, 저장",
        infra_overrides=infra_overrides,
    )

    print("--- 배포될 설정값 ---")
    print(deployment.dict())
    print("--------------------")

    await deployment.apply()
    print("✅ Deployment 생성 / 업데이트 완료: 'daily-weather-collection-new'")


if __name__ == "__main__":
    asyncio.run(create_deployment())
