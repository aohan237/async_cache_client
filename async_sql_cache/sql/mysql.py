import aiomysql
from .base import SqlBase


class MysqlClient(SqlBase):
    def __init__(self, conf=None, **kwargs):
        self._mysql_pool = aiomysql.create_pool(
            host=conf['host'], port=conf['port'],
            user=conf['user'], password=conf['password'],
            db=conf['db'], charset='utf8mb4', pool_recycle=kwargs.get(
                'pool_recycle', 3600
            ))

    async def connect(self):
        self.pool = await self._mysql_pool

    async def get(self, sql=None):
        sql = self.format_sql(sql=sql)
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql)
                result = await cur.fetchall()
        return result

    def format_sql(self, sql=None):
        sql = sql.strip()
        return sql

    async def set(self, sql=None):
        sql = self.format_sql(sql=sql)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(sql)
                    await conn.commit()
                    result = True
        except Exception as tmp:
            await conn.rollback()
            result = False
        return result
