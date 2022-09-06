from typing import List

from dagster import (
    In,
    Out,
    Nothing,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context) -> [Stock]:
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        print(row)
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg_max": Out(dagster_type=Aggregation)},
    description="Find the highest stock price and date",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    return Aggregation(
        date=(highest_stock := max(stocks, key=lambda stock: stock.high)).date,
        high=highest_stock.high,
    )


@op(
    required_resource_keys={"redis"},
    ins={"agg_max": In(dagster_type=Aggregation)},
    out=None,
    description="Post aggregate result to Redis",
)
def put_redis_data(context, agg_max: Aggregation) -> None:
    context.log.debug(f"Putting {agg_max} to Redis")
    context.resources.redis.put_data(
        name=f"{agg_max.date}",
        value=agg_max.high,
    )


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=[str(n) for n in range(1, 11)])  # Each month
def docker_config(partition_key: int) -> dict:
    return docker | {
        "ops": {
            "get_s3_data": {
                "config": {
                    "s3_key": f"prefix/stock_{partition_key}.csv"
                }
            }
        }
    }


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(
        max_retries=10,
        delay=1
    ),
)


local_week_3_schedule = ScheduleDefinition(
    job=local_week_3_pipeline,
    cron_schedule="*/15 * * * *",  # every 15 minutes
)

docker_week_3_schedule = ScheduleDefinition(
    job=docker_week_3_pipeline,
    cron_schedule="0 * * * *",  # every hour
)


@sensor(
    job=docker_week_3_pipeline,
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    new_s3_keys = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566",
        since_key=context.last_run_key,
    )
    if not new_s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
    else:
        for new_s3_key in new_s3_keys:
            yield RunRequest(
                run_key=new_s3_key,
                run_config=docker | {
                    "ops": {
                        "get_s3_data": {
                            "config": {
                                "s3_key": new_s3_key
                            }
                        }
                    }
                }
            )