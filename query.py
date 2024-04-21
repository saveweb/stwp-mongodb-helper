import pymongo
import os
from datetime import UTC, datetime, timedelta

client = pymongo.MongoClient(os.environ['MONGODB_URI'])
feed_queue = client.coolapk.feed_queue
_filter = { "status": "FAIL", "update_at": { "$lt": datetime.now(tz=UTC) - timedelta(seconds=3600) } } # 一定要设置 tz=UTC ！！！！
while doc_c := feed_queue.count_documents(
    _filter   
    ):
    print(doc_c)

    r = feed_queue.find_one_and_update(
        filter=_filter,
        update={ "$set": { "status": "TODO" } }
    )
    print(r)