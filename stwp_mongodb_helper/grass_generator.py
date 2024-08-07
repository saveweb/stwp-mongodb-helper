import argparse
import os
import time

import tqdm
import pymongo
import pymongo.collection

def find_max_id(c: pymongo.collection.Collection, key: str):
    r = c.find_one(
        filter={},
        sort=[(key, pymongo.DESCENDING)]
    )
    return r[key] if r else None


def arg_parser():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("db", help="Database name")
    parser.add_argument("co", help="Collection name")
    parser.add_argument("--chunk-size", help="Chunk size", type=int, required=True)
    parser.add_argument("--end", help="End ID", type=int, required=True)
    parser.add_argument("--delay", help="Delay in seconds", type=float, required=False, default=1.0)
    parser.add_argument("--id-name", help="ID name", default="id", choices=["id", "feed_id"])
    return parser.parse_args()

def qos(delay: float):
    while True:
        yield
        time.sleep(delay)

def main():
    args = arg_parser()
    db_name = args.db
    c_queue_name = args.co
    chunk_size: int = args.chunk_size
    delay: float = args.delay
    ID_NAME = args.id_name

    client = pymongo.MongoClient(os.environ['MONGODB_URI'])
    dbs = client.list_database_names()
    print(dbs)
    assert db_name in dbs, "Database not found"
    db = client[db_name]
    colls = db.list_collection_names()
    print(colls)
    assert c_queue_name in colls, "Collection not found"
    assert c_queue_name.endswith("_queue"), "Collection name must end with '_queue'"
    coll = db[c_queue_name]

    input("Press Enter to start generating documents")

    tqd = None

    for _ in qos(delay):
        todos_now = coll.count_documents({ "status": "TODO" })
        print(todos_now, "TODO documents in queue")
        if todos_now > chunk_size * 5:
            continue
        max_id_now = find_max_id(coll, ID_NAME)  or 0
        if tqd is None:
            tqd = tqdm.tqdm(total=args.end, initial=max_id_now)
        tqd.n = max_id_now
        tqd.refresh()

        if max_id_now >= args.end:
            print("Max id reached", max_id_now)
            break
        print("Max id:", max_id_now)
        docs = []
        for i in range(1,chunk_size+1):
            docs.append({
                ID_NAME: max_id_now + i,
                "status": "TODO"
            })
        print("Inserting", chunk_size, "documents")
        print([doc[ID_NAME] for doc in docs])
        coll.insert_many(docs)


if __name__ == "__main__":
    main()