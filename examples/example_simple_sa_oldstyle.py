import asyncio
import sqlalchemy as sa

from aiomysql.sa import create_engine


metadata = sa.MetaData()

tbl = sa.Table('tbl', metadata,
               sa.Column('id', sa.Integer, primary_key=True),
               sa.Column('val', sa.String(255)))


@asyncio.coroutine
def create_table(engine):
    with (yield from engine) as conn:
        yield from conn.execute('DROP TABLE IF EXISTS tbl')
        yield from conn.execute('''CREATE TABLE tbl (
                                            id serial PRIMARY KEY,
                                            val varchar(255))''')


@asyncio.coroutine
def go():
    engine = yield from create_engine(user='root',
                                      db='test_pymysql',
                                      host='127.0.0.1',
                                      password='')

    yield from create_table(engine)
    with (yield from engine) as conn:
        yield from conn.execute(tbl.insert().values(val='abc'))

        res = yield from conn.execute(tbl.select())
        for row in res:
            print(row.id, row.val)

        yield from conn.commit()

    engine.close()
    yield from engine.wait_closed()


asyncio.get_event_loop().run_until_complete(go())
