import pytest


@pytest.mark.run_loop
async def test_nextset(cursor):
    await cursor.execute("SELECT 1; SELECT 2;")
    r = await cursor.fetchall()
    assert [(1,)] == list(r)

    r = await cursor.nextset()
    assert r

    r = await cursor.fetchall()
    assert [(2,)] == list(r)
    res = await cursor.nextset()
    assert res is None


@pytest.mark.run_loop
async def test_skip_nextset(cursor):
    await cursor.execute("SELECT 1; SELECT 2;")
    r = await cursor.fetchall()
    assert [(1,)] == list(r)

    await cursor.execute("SELECT 42")
    r = await cursor.fetchall()
    assert [(42,)] == list(r)


@pytest.mark.run_loop
async def test_ok_and_next(cursor):
    await cursor.execute("SELECT 1; commit; SELECT 2;")
    r = await cursor.fetchall()
    assert [(1,)] == list(r)

    res = await cursor.nextset()
    assert res

    res = await cursor.nextset()
    assert res

    r = await cursor.fetchall()
    assert [(2,)] == list(r)

    res = await cursor.nextset()
    assert res is None


@pytest.mark.xfail
@pytest.mark.run_loop
async def test_multi_cursorxx(connection):
    cur1 = await connection.cursor()
    cur2 = await connection.cursor()

    await cur1.execute("SELECT 1; SELECT 2;")
    await cur2.execute("SELECT 42")

    r1 = await cur1.fetchall()
    r2 = await cur2.fetchall()

    assert [(1,)] == list(r1)
    assert [(42,)] == list(r2)

    res = await cur1.nextset()
    assert res

    assert [(2,)] == list(r1)
    res = await cur1.nextset()
    assert res is None

    # TODO: How about SSCursor and nextset?
    # It's very hard to implement correctly...
