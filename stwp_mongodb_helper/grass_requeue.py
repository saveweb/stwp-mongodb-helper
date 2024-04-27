
import argparse
from asyncio import Queue
import asyncio
import motor.motor_asyncio
import motor
import os
from datetime import UTC, datetime, timedelta


# STATUS_FROM = "PROCESSING"
STATUS_TO = "TODO"

def arg_parser():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("db", help="Database name")
    parser.add_argument("co", help="Collection name")
    parser.add_argument("--hours", help="Hours", type=float, default=1.0)
    parser.add_argument("--status-from", help="From status", default="FAIL")
    return parser.parse_args()

async def worker(jobs: Queue):
    while True:
        job = await jobs.get()
        await job
        jobs.task_done()

async def _main():
    args = arg_parser()
    db_name = args.db
    c_queue_name = args.co
    hours: float = args.hours
    STATUS_FROM = args.status_from

    client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGODB_URI"])
    dbs = await client.list_database_names()
    print(dbs)
    assert db_name in dbs, "Database not found"
    db = client[db_name]
    colls = await db.list_collection_names()
    print(colls)
    assert c_queue_name.endswith("_queue"), "Collection name must end with '_queue'"
    assert c_queue_name in colls, "Collection not found"
    coll = db[c_queue_name]

    _filter = {
        "status": STATUS_FROM,
        "updated_at": {"$lt": datetime.now(tz=UTC) - timedelta(hours=hours)},
    }

    jobs = Queue(maxsize=40)

    for _ in range(30):
        asyncio.create_task(worker(jobs))

    async for doc in coll.find(_filter):
        async def process(doc):
            await coll.update_one(
                {"_id": doc["_id"]},
                {"$set": {"status": STATUS_TO}},
            )
            # print(doc)
        job = process(doc)
        await jobs.put(job)

    await jobs.join()

def main():
    asyncio.run(_main())

if __name__ == "__main__":
    main()