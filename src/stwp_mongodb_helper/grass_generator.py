import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
import signal
import multiprocessing
import sys
import tomllib
import time
import concurrent.futures

import pymongo
import pymongo.collection
from pymongo.synchronous.client_session import ClientSession

ID = "id"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def find_max_id(c: pymongo.collection.Collection, key: str, session: ClientSession):
    r = c.find_one(
        filter={},
        sort=[(key, pymongo.DESCENDING)],
        session=session
    )
    return r[key] if r else None


def arg_parser():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("db", help="Database name")
    parser.add_argument("co", help="Collection name")
    parser.add_argument("--chunk-size", help="Chunk size", type=int, required=True)
    parser.add_argument("--end", help="End ID", type=int, required=True)
    parser.add_argument("--delay", help="Delay in seconds", type=float, required=False, default=1.0)
    return parser.parse_args()

def qos(delay: float):
    while True:
        yield
        for _ in range(int(delay * 10)):
            if event.is_set():
                return
            time.sleep(0.1)
@dataclass
class Args:
    db: str
    co: str
    chunk_size: int
    end: int
    delay: float

class GID(str): ...

def claim_owner(client: pymongo.MongoClient, session_id: GID, db: str, co: str):
    meta_col = client['tracker']['generator_registry']
    meta_col.update_one(
        {"_id": f"{db}.{co}"},
        {
            "$set": {
                "owner": session_id,
                "claimed_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )

def check_owner(client: pymongo.MongoClient, gid: GID, a: Args, session: ClientSession):
    meta_col = client['tracker']['generator_registry']
    r = meta_col.find_one({"_id": f"{a.db}.{a.co}"}, session=session)
    assert r is not None, "Generator not registered"
    assert r["owner"] == gid, "Lost ownership of the generator"
    meta_col.update_one(
        {"_id": f"{a.db}.{a.co}"},
        {
            "$set": {
                "heartbeat_at": datetime.now(timezone.utc),
            }
        },
        session=session,
    )

def run(a: Args):
    gid = GID(f"{os.getpid()}-{int(time.time())}-{os.urandom(4).hex()}")
    def log(msg: str, *args):
        logger.info(f"{a.db}.{a.co}: {msg}", *args)
    with pymongo.MongoClient(os.environ['MONGODB_URI']) as client:
        dbs = client.list_database_names()
        log("Databases: %s", dbs)
        assert a.db in dbs, "Database not found"
        db = client[a.db]
        colls = db.list_collection_names()
        log("Collections: %s", colls)
        assert a.co in colls, "Collection not found"
        assert a.co.endswith("_queue"), "Collection name must end with '_queue'"
        coll = db[a.co]

        claim_owner(client, gid, a.db, a.co)

        for _ in qos(a.delay):
            if event.is_set():
                log("runner exiting...")
                return

            with client.start_session() as session:
                with session.start_transaction():
                    check_owner(client, gid, a, session)

                    todos_now = coll.count_documents({ "status": "TODO" }, session=session)
                    log("%d TODO documents in queue", todos_now)
                    if todos_now > a.chunk_size * 5:
                        continue
                    max_id_now = find_max_id(coll, ID, session=session)  or 0

                    if max_id_now >= a.end:
                        log("Max id reached %d", max_id_now)
                        break
                    log("Max id: %d", max_id_now)
                    docs = []
                    for i in range(1, a.chunk_size + 1):
                        docs.append({
                            ID: max_id_now + i,
                            "status": "TODO"
                        })
                    log("Inserting %d documents", a.chunk_size)
                    log("Document IDs: %s", [doc[ID] for doc in docs])
                    coll.insert_many(docs, session=session)

                    check_owner(client, gid, a, session)

def loadargs_list_from_toml():
    data = tomllib.load(open("generator_tasks.toml", "rb"))

    return [
        Args(
            db=service["database"],
            co=service["collection"],
            chunk_size=service["chunk_size"],
            end=service["end"],
            delay=service.get("delay", 1.0),
        )
        for service in data["task"]
    ]

def toml_changes_watcher():
    last_mtime = os.path.getmtime("generator_tasks.toml")
    while True:
        if event.is_set():
            return

        time.sleep(1)
        mtime = os.path.getmtime("generator_tasks.toml")
        if mtime != last_mtime:
            last_mtime = mtime
            logger.info("TOML file changed, forcing exit...")
            to_exit()
            return

event = multiprocessing.Event()
err_event = multiprocessing.Event()

def to_exit(err: bool = False):
    print("setting exit event...", "err =", err)
    event.set()
    if err:
        err_event.set()

def main():
    signal.signal(signal.SIGINT, lambda s, f: to_exit())
    signal.signal(signal.SIGTERM, lambda s, f: to_exit())

    logger.addHandler(logging.StreamHandler())
    if os.environ.get("DAEMON_MODE"):
        import threading
        watcher = threading.Thread(target=toml_changes_watcher)
        watcher.daemon = True
        watcher.start()

        args_list = loadargs_list_from_toml()
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(run, args) for args in args_list]
            while futures:
                done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    if future.exception():
                        logger.error("Task failed: %s", future.exception())
                        print("Force exiting...")
                        to_exit(err=True)
                    else:
                        logger.info("Task completed")
                print("Futures remaining:", len(futures))

        print("Waiting for watcher to join...")
        watcher.join()
        print("Watcher joined")

        if err_event.is_set():
            sys.exit(1)
        else:
            sys.exit(0)
    else:
        args = arg_parser()
        run(Args(
            db=args.db,
            co=args.co,
            chunk_size=args.chunk_size,
            end=args.end,
            delay=args.delay,
        ))


if __name__ == "__main__":
    main()