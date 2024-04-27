
import argparse
from asyncio import Queue
import asyncio
import motor.motor_asyncio
import motor
import os
from tqdm import tqdm
from datetime import UTC, datetime, timedelta

STATUS_FAIL = "FAIL"
DRY_RUN = False

def arg_parser():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("db", help="")
    parser.add_argument("--items-co", help="")
    parser.add_argument("--queue-co", help="")
    parser.add_argument("--hours", help="Hours", type=float, default=1.0)
    parser.add_argument("--id_name", help="id name", default="id")
    return parser.parse_args()

async def worker(jobs: Queue):
    while True:
        job = await jobs.get()
        await job
        jobs.task_done()

async def _main():
    args = arg_parser()
    db_name = args.db
    items_co_name = args.items_co
    queue_co_name = args.queue_co
    hours: float = args.hours
    id_name = args.id_name

    client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGODB_URI"])
    dbs = await client.list_database_names()
    print(dbs)
    assert db_name in dbs, "Database not found"
    db = client[db_name]
    colls = await db.list_collection_names()
    print(colls)
    assert queue_co_name.endswith("_queue"), "Collection name must end with '_queue'"
    assert queue_co_name in colls and items_co_name in colls, "Collection not found"
    items_co = db[items_co_name]
    queue_co = db[queue_co_name]

    jobs = Queue(maxsize=100)

    for _ in range(100):
        asyncio.create_task(worker(jobs))

    _filter = {
        "status": STATUS_FAIL,
        "updated_at": {"$lt": datetime.now(tz=UTC) - timedelta(hours=hours)},
    }  # 一定要设置 tz=UTC ！！！！
    jobs_total = await queue_co.count_documents(_filter)
    global tqd
    tqd = tqdm(total=jobs_total)

    async for fail_task in queue_co.find(_filter):
        async def process(fail_task):
            try:
                if await items_co.count_documents({id_name: fail_task[id_name]}):
                    # queue 中状态为 STATUS_TO_SCAN， items 中存在，说明 task 的状态是错误的，需要改成 DONE
                    print("--->DONE", fail_task)
                    if DRY_RUN:
                        return
                    await queue_co.update_one(
                        {id_name: fail_task[id_name]},
                        {"$set": {"status": "DONE"}},
                    )
                else:
                    # queue 中状态为 STATUS_TO_SCAN， items 中不存在，说明 task 是真的失败了，需要改成 TODO 重新排队
                    print("--->TODO", fail_task)
                    if DRY_RUN:
                        return
                    await queue_co.update_one(
                        {id_name: fail_task[id_name]},
                        {"$set": {"status": "TODO"}},
                    )
            finally:
                tqd.update(1)
        job = process(fail_task)
        await jobs.put(job)

    await jobs.join()

def main():
    asyncio.run(_main())

if __name__ == "__main__":
    main()