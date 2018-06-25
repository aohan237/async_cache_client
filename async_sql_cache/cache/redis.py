import aioredis
from aioredis.commands.sorted_set import SortedSetCommandsMixin
import json
import time
import logging
from .base import CacheBase


class RedisCache(CacheBase):
    def __init__(
            self, conf=None, cached_set_postfix=None,
            cached_key=None, **kwargs):
        self._create_pool = aioredis.create_redis_pool(
            (conf.get('redis_host'), conf.get('redis_port')),
            password=conf.get('redis_password'),
            minsize=5,
            maxsize=10,
        )
        self.cached_set_postfix = cached_set_postfix or ':zset'
        self._cached_key = cached_key or 'RedisCache'

    async def connect(self):
        self.pool = await self._create_pool

    async def add_to_regulate_set(
            self, database=None,
            key=None, expire_at=None):
        database = database + self.cached_set_postfix
        # 添加记录的数据库名称
        await self.pool.sadd(self._cached_key, database)
        # 记录每一个数据对应的缓存数据，如果过期则删除
        exist = await self.pool.zscore(database, key)
        if exist:
            if expire_at:
                tmp = await self.pool.zadd(database, expire_at, key)
                print('update expire', database, expire_at, key, tmp)
            return True
        else:
            expire_at = expire_at or int(time.time()) + 300
            await self.pool.zadd(database, expire_at, key)
            return True

    async def set(
            self, database=None, key=None, value=None,
            expire_at=None):
        await self.add_to_regulate_set(
            database=database, key=key,
            expire_at=expire_at)
        return await self.pool.hset(database, key, value)

    async def exist(self, database=None, key=None):
        return await self.pool.hexists(database, key)

    async def get(self, database=None, key=None):
        result_byte = await self.pool.hget(database, key)
        try:
            result = json.loads(result_byte)
        except Exception as tmp:
            result = result_byte.decode() if result_byte else {}
        return result

    async def get_update_data(self):
        now = int(time.time())
        all_databases = await self.pool.smembers(self._cached_key)
        all_databases = {i.decode() for i in all_databases}
        result = {}
        for database in all_databases:
            expire_list = await self.pool.zrangebyscore(
                database, float('-inf'), now,
                exclude=SortedSetCommandsMixin.ZSET_EXCLUDE_MAX)
            expire_list = [i.decode() for i in expire_list]
            await self.pool.zrem(database, '', *expire_list)
            update_list = await self.pool.zrangebyscore(
                database, now, float('+inf'))
            update_list = [i.decode() for i in update_list]
            database, _ = database.split(':')
            await self.pool.hdel(database, '', *expire_list)
            if update_list:
                result[database] = update_list
        return result
