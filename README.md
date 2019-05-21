# Cache-Client

[![Downloads](https://pepy.tech/badge/cache-client)](https://pepy.tech/project/cache-client)

## Cache-Client

most of the time, you need cache to some kind of backend to speed up your app.

this framework add these tools to free you from write these.

it also has auto update function to free you from manually update something.


## Usage

```python
from logging import StreamHandler
import asyncio
import time
from cache_client.client.common import SimpleClient
import logging
logger = logging.getLogger(__package__)
handler = StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


mysql_conf = {
    'host': '127.0.0.1',
    'port': 3306,
    'db': 'test',
    'user': 'test',
    'password': 'test'
}

redis_conf = {
    'redis_host': '127.0.0.1',
    'redis_port': 6379,
    'redis_secret': '',
}


client = SimpleClient(backend_conf=mysql_conf,
                      cache_conf=redis_conf,
                      update_interval=30)


async def test():
    await client.connect()
    expire_at = int(time.time()) + 100
    for i in range(10):
        start = int(time.time()*1000)
        result = await client.get(
            database='sku',
            key='select sku_id,description,price,status from sku',
            expire_at=expire_at,
            update_interval=100)
        end = int(time.time()*1000)
        print('running time', end-start)
        print(result, 'result', i)

loop = asyncio.get_event_loop()

# loop.run_until_complete(test())

loop.create_task(test())
loop.run_forever()
```

## Coding

* **like this project, star it**
* **any suggestion is welcome**
* **this project is under MIT lisense**

more docs are writing.
