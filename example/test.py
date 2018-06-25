import asyncio
import time
from async_sql_cache.client.common import SimpleClient
mysql_conf = {
    'host': '127.0.0.1',
    'port': 3306,
    'db': 'blob_ai',
    'user': 'root',
    'password': '123'
}

redis_conf = {
    'redis_host': '127.0.0.1',
    'redis_port': 1234,
    'redis_secret': '',
}


client = SimpleClient(mysql_conf=mysql_conf,
                      redis_conf=redis_conf,
                      update_interval=5)


async def test():
    await client.connect()
    expire_at = int(time.time()) + 3
    result = await client.get(
        database='sku',
        key='select sku_id,description,price,status from sku',
        expire_at=expire_at)
    print(result)

loop = asyncio.get_event_loop()

# loop.run_until_complete(test())

loop.create_task(test())
loop.run_forever()
