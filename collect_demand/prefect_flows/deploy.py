import asyncio
import os
import time

from prefect.client.orchestration import get_client
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from prefect.utilities.importtools import import_object
from prefect.server.schemas.actions import WorkPoolCreate

# Prefect API URL 환경변수 기본값 세팅
os.environ.setdefault("PREFECT_API_URL", "http://prefect-server-weather:4200/api")


async def wait_for_api(timeout: int = 120) -> None:
    """
    Prefect API가 살아있는지 헬스 체크.
    예전 코드에서 쓰던 `read_server_settings` 대신
    이제는 `api_healthcheck()`를 사용.
    """
    start = time.time()

    while True:
        try:
            async with get_client() as client:
                # ✅ 여기서 API 헬스 체크
                await client.api_healthcheck()

            print("✅ Prefect API 연결 성공")
            return

        except Exception as e:
            elapsed = time.time() - start
            if elapsed > timeout:
                raise RuntimeError("❌ Prefect API 연결 시간 초과") from e

            print(f"Prefect API 대기 중... ({e!r})")
            await asyncio.sleep(5)


async def ensure_work_pool(pool_name: str = "weather-pool") -> None:
    """
    work pool 이 없으면 docker 타입으로 생성,
    이미 있으면 '이미 존재' 메세지만 출력.
    """
    # ✅ 여기서 PrefectClient()가 아니라 get_client() 사용
    async with get_client() as client:
        try:
            pool = await client.read_work_pool(work_pool_name=pool_name)
            print(f"Work pool '{pool_name}'이 이미 존재합니다. (타입: {pool.type})")
        except Exception:
            print(f"Work pool '{pool_name}'이 없어 새로 생성합니다...")
            await client.create_work_pool(
                work_pool=WorkPoolCreate(
                    name=pool_name,
                    type="docker",
                    base_job_template={},
                )
            )
            print(f"✅ Work pool '{pool_name}' 생성 완료")


async def create_deployment() -> None:
    """
    - Prefect API 대기
    - work pool 확인/생성
    - flow import 해서 Deployment 생성/업데이트
    """
    # 1) API 준비될 때까지 대기
    await wait_for_api()

    # 2) work pool 준비
    await ensure_work_pool("weather-pool")

    # 3) flow 가져오기
    flow = import_object(
        "prefect_flows.prefect_pipeline.daily_weather_collection_flow"
    )

    # 4) Deployment 정의
    deployment = await Deployment.build_from_flow(
        flow=flow,
        name="daily-weather-collection",
        work_pool_name="weather-pool",
        parameters={
            "target_date": None,
        },
        schedules=[
            CronSchedule(
                cron="0 9 * * *",  # 매일 오전 9시 (Asia/Seoul)
                timezone="Asia/Seoul",
            )
        ],
    )

    # 5) 서버에 적용
    await deployment.apply()
    print("✅ Deployment 생성 / 업데이트 완료: 'daily-weather-collection'")


if __name__ == "__main__":
    asyncio.run(create_deployment())
