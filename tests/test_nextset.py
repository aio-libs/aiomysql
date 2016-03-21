import pytest


@pytest.mark.run_loop
def test_nextset(cursor):
    yield from cursor.execute("SELECT 1; SELECT 2;")
    r = yield from cursor.fetchall()
    assert [(1,)] == list(r)

    r = yield from cursor.nextset()
    assert r

    r = yield from cursor.fetchall()
    assert [(2,)] == list(r)
    res = yield from cursor.nextset()
    assert res is None


@pytest.mark.run_loop
def test_skip_nextset(cursor):
    yield from cursor.execute("SELECT 1; SELECT 2;")
    r = yield from cursor.fetchall()
    assert [(1,)] == list(r)

    yield from cursor.execute("SELECT 42")
    r = yield from cursor.fetchall()
    assert [(42,)] == list(r)


@pytest.mark.run_loop
def test_ok_and_next(cursor):
    yield from cursor.execute("SELECT 1; commit; SELECT 2;")
    r = yield from cursor.fetchall()
    assert [(1,)] == list(r)

    res = yield from cursor.nextset()
    assert res

    res = yield from cursor.nextset()
    assert res

    r = yield from cursor.fetchall()
    assert [(2,)] == list(r)

    res = yield from cursor.nextset()
    assert res is None


@pytest.mark.xfail
@pytest.mark.run_loop
def test_multi_cursorxx(connection):
    cur1 = yield from connection.cursor()
    cur2 = yield from connection.cursor()

    yield from cur1.execute("SELECT 1; SELECT 2;")
    yield from cur2.execute("SELECT 42")

    r1 = yield from cur1.fetchall()
    r2 = yield from cur2.fetchall()

    assert [(1,)] == list(r1)
    assert [(42,)] == list(r2)

    res = yield from cur1.nextset()
    assert res

    assert [(2,)] == list(r1)
    res = yield from cur1.nextset()
    assert res is None

    # TODO: How about SSCursor and nextset?
    # It's very hard to implement correctly...
