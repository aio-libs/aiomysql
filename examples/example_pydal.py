"""
pyDAL is a powerful Database Abstract Layer from web2py.
@see https://github.com/web2py/pydal
@see https://github.com/web2py/web2py

Data structure

create table user
(
    id   int auto_increment
        primary key,
    name varchar(512) null
);

"""

import asyncio

from pydal import Field

from aiomysql.pydal import AsyncDAL


async def bootstrap():
    dal = await AsyncDAL.create("127.0.0.1", "root", "mypass", "test_pymysql")
    dal.define_table(
        "user",
        Field("name")
    )

    conn = await dal.acquire()
    db = await conn.cursor()

    # insert
    effected = await db.user.insert(name="pydal")
    print(f"insert effected rows: {effected}")
    await conn.commit()

    # query
    result = await db(db.user.name == "pydal").select(db.user.ALL, orderby=~db.user.id)
    print(result)

    # update
    effected = await db(db.user.name == "pydal").update(name="aiomysql")
    print(f"update effected rows: {effected}")
    await conn.commit()

    # delete
    effected = await db(db.user.name == "aiomysql").delete()
    print(f"delete effected rows: {effected}")
    await conn.commit()

    # count
    count = await db(db.user.id > 0).count()
    print(f"count: {count}")

    await dal.release(conn)
    dal.terminate()
    await dal.wait_closed()


asyncio.run(bootstrap())
