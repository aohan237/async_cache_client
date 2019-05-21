import aiomysql
from .base import BackendBase


class MysqlBackend(BackendBase):
    def __init__(self, conf=None, **kwargs):
        """
        @param conf: database conf in dict:
            self.host :host address
            self.port :port number
            self.password :password
            self.user :username
            self.db :db name
            self.charset :charset
            self.pool_recycle :pool recycle interval     
        """
        self._host = conf['host']
        self._port = conf['port']
        self._password = conf['password']
        self._user = conf['user']
        self._db = conf['db']
        self._charset = conf.get('charset', 'utf8mb4')
        self._pool_recycle = conf.get('pool_recycle', 3600)

    async def connect(self, loop=None):
        mysql_pool = aiomysql.create_pool(
            host=self._host, port=self._port,
            user=self._user, password=self._password,
            db=self._db, charset=self._charset,
            pool_recycle=self._pool_recycle, loop=loop)
        self.pool = await mysql_pool

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
