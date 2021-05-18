from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.schema import DropTable, CreateTable
from sqlalchemy.sql.expression import bindparam

import aiomysql
from aiomysql import sa, Cursor

meta = MetaData()
tbl = Table('sa_tbl', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('name', String(255)))


@pytest.fixture()
def sa_connect(connection_creator):
    async def connect(**kwargs):
        conn = await connection_creator(**kwargs)
        await conn.autocommit(True)
        cur = await conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS sa_tbl")
        await cur.execute("CREATE TABLE sa_tbl "
                          "(id serial, name varchar(255))")
        await cur.execute("INSERT INTO sa_tbl (name)"
                          "VALUES ('first')")

        await cur._connection.commit()
        # yield from cur.close()
        engine = mock.Mock()
        engine.dialect = sa.engine._dialect
        return sa.SAConnection(conn, engine)
    return connect


@pytest.mark.run_loop
async def test_execute_text_select(sa_connect):
    conn = await sa_connect()
    res = await conn.execute("SELECT * FROM sa_tbl;")
    assert isinstance(res.cursor, Cursor)
    assert ('id', 'name') == res.keys()
    rows = await res.fetchall()
    assert res.closed
    assert res.cursor is None
    assert 1 == len(rows)
    row = rows[0]
    assert 1 == row[0]
    assert 1 == row['id']
    assert 1 == row.id
    assert 'first' == row[1]
    assert 'first' == row['name']
    assert 'first' == row.name
    # TODO: fix this
    await conn._connection.commit()


@pytest.mark.run_loop
async def test_execute_sa_select(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(tbl.select())
    assert isinstance(res.cursor, Cursor)
    assert ('id', 'name') == res.keys()
    rows = await res.fetchall()
    assert res.closed
    assert res.cursor is None
    assert res.returns_rows

    assert 1 == len(rows)
    row = rows[0]
    assert 1 == row[0]
    assert 1 == row['id']
    assert 1 == row.id
    assert 'first' == row[1]
    assert 'first' == row['name']
    assert 'first' == row.name
    # TODO: fix this
    await conn._connection.commit()


@pytest.mark.run_loop
async def test_execute_sa_insert_with_dict(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert(), {"id": 2, "name": "second"})

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@pytest.mark.run_loop
async def test_execute_sa_insert_with_tuple(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert(), (2, "second"))

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@pytest.mark.run_loop
async def test_execute_sa_insert_named_params(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert(), id=2, name="second")

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@pytest.mark.run_loop
async def test_execute_sa_insert_positional_params(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert(), 2, "second")

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert (1, 'first') == rows[0]
    assert (2, 'second') == rows[1]


@pytest.mark.run_loop
async def test_scalar(sa_connect):
    conn = await sa_connect()
    res = await conn.scalar(tbl.count())
    assert 1 == res


@pytest.mark.run_loop
async def test_scalar_None(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.delete())
    res = await conn.scalar(tbl.select())
    assert res is None
    # TODO: fix this
    await conn._connection.commit()


@pytest.mark.run_loop
async def test_row_proxy(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    row = rows[0]
    row2 = await (await conn.execute(tbl.select())).first()
    assert 2 == len(row)
    assert ['id', 'name'] == list(row)
    assert 'id' in row
    assert 'unknown' not in row
    assert 'first' == row.name
    assert 'first' == row[tbl.c.name]
    with pytest.raises(AttributeError):
        row.unknown
    assert "(1, 'first')" == repr(row)
    assert (1, 'first') == row.as_tuple()
    assert (555, 'other') != row.as_tuple()
    assert row2 == row
    assert 5 != row
    # TODO: fix this
    await conn._connection.commit()


@pytest.mark.run_loop
async def test_insert(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(tbl.insert().values(name='second'))
    assert 1 == res.rowcount
    assert 2 == res.lastrowid


@pytest.mark.run_loop
async def test_raw_insert(sa_connect):
    conn = await sa_connect()
    await conn.execute(
        "INSERT INTO sa_tbl (name) VALUES ('third')")
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


@pytest.mark.run_loop
async def test_raw_insert_with_params(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%s, %s)",
        2, 'third')
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


@pytest.mark.run_loop
async def test_raw_insert_with_params_dict(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        {'id': 2, 'name': 'third'})
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


@pytest.mark.run_loop
async def test_raw_insert_with_named_params(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        id=2, name='third')
    res = await conn.execute(tbl.select())
    assert 2 == res.rowcount
    assert ('id', 'name') == res.keys()
    assert res.returns_rows

    rows = await res.fetchall()
    assert 2 == len(rows)
    assert 2 == rows[1].id


@pytest.mark.run_loop
async def test_raw_insert_with_executemany(sa_connect):
    conn = await sa_connect()
    # with pytest.raises(sa.ArgumentError):
    await conn.execute(
        "INSERT INTO sa_tbl (id, name) VALUES (%(id)s, %(name)s)",
        [{"id": 2, "name": 'third'}, {"id": 3, "name": 'forth'}])
    await conn.execute(
        tbl.update().where(
            tbl.c.id == bindparam("id")
        ).values(
            {"name": bindparam("name")}
        ),
        [
            {"id": 2, "name": "t2"},
            {"id": 3, "name": "t3"}
        ]
    )
    with pytest.raises(sa.ArgumentError):
        await conn.execute(
            DropTable(tbl),
            [{}, {}]
        )
    with pytest.raises(sa.ArgumentError):
        await conn.execute(
            {},
            [{}, {}]
        )


@pytest.mark.run_loop
async def test_raw_select_with_wildcard(sa_connect):
    conn = await sa_connect()
    await conn.execute(
        'SELECT * FROM sa_tbl WHERE name LIKE "%test%"')


@pytest.mark.run_loop
async def test_delete(sa_connect):
    conn = await sa_connect()

    res = await conn.execute(tbl.delete().where(tbl.c.id == 1))

    assert () == res.keys()
    assert 1 == res.rowcount
    assert not res.returns_rows
    assert res.closed
    assert res.cursor is None


@pytest.mark.run_loop
async def test_double_close(sa_connect):
    conn = await sa_connect()
    res = await conn.execute("SELECT 1")
    await res.close()
    assert res.closed
    assert res.cursor is None
    await res.close()
    assert res.closed
    assert res.cursor is None


@pytest.mark.run_loop
@pytest.mark.skip("Find out how to close cursor on __del__ method")
async def test_weakrefs(sa_connect):
    conn = await sa_connect()
    assert 0 == len(conn._weak_results)
    res = await conn.execute("SELECT 1")
    assert 1 == len(conn._weak_results)
    cur = res.cursor
    assert not cur.closed
    # TODO: fix this, how close cursor if result was deleted
    # yield from cur.close()
    del res
    assert cur.closed
    assert 0 == len(conn._weak_results)


@pytest.mark.run_loop
async def test_fetchall(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    rows = await res.fetchall()
    assert 2 == len(rows)
    assert res.closed
    assert res.returns_rows
    assert [(1, 'first'), (2, 'second')] == rows


@pytest.mark.run_loop
async def test_fetchall_closed(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    await res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchall()


@pytest.mark.run_loop
async def test_fetchall_not_returns_rows(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchall()


@pytest.mark.run_loop
async def test_fetchone_closed(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    await res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchone()


@pytest.mark.run_loop
async def test_first_not_returns_rows(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        await res.first()


@pytest.mark.run_loop
async def test_fetchmany(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    rows = await res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first')] == rows


@pytest.mark.run_loop
async def test_fetchmany_with_size(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    rows = await res.fetchmany(100)
    assert 2 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first'), (2, 'second')] == rows


@pytest.mark.run_loop
async def test_fetchmany_closed(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    await res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()


@pytest.mark.run_loop
async def test_fetchmany_with_size_closed(sa_connect):
    conn = await sa_connect()
    await conn.execute(tbl.insert().values(name='second'))

    res = await conn.execute(tbl.select())
    await res.close()
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany(5555)


@pytest.mark.run_loop
async def test_fetchmany_not_returns_rows(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(tbl.delete())
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()


@pytest.mark.run_loop
async def test_fetchmany_close_after_last_read(sa_connect):
    conn = await sa_connect()

    res = await conn.execute(tbl.select())
    rows = await res.fetchmany()
    assert 1 == len(rows)
    assert not res.closed
    assert res.returns_rows
    assert [(1, 'first')] == rows
    rows2 = await res.fetchmany()
    assert 0 == len(rows2)
    assert res.closed


@pytest.mark.run_loop
async def test_create_table(sa_connect):
    conn = await sa_connect()
    res = await conn.execute(DropTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()

    with pytest.raises(aiomysql.ProgrammingError):
        await conn.execute("SELECT * FROM sa_tbl")

    res = await conn.execute(CreateTable(tbl))
    with pytest.raises(sa.ResourceClosedError):
        await res.fetchmany()

    res = await conn.execute("SELECT * FROM sa_tbl")
    assert 0 == len(await res.fetchall())
