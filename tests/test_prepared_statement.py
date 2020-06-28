from decimal import Decimal
import datetime

import pytest


async def prepare(conn):
    c = await conn.cursor()

    # create a table ane some data to query
    await c.execute("drop table if exists everytype")
    await c.execute(
        """CREATE TABLE everytype (
            colchar char(20),
            colset set('a', 'b'),
            colblob blob,
            colenum enum('a', 'b'),
            colgeometry geometry,
            colbit bit,
            coldecimal decimal,
            coljson json,
            collong bigint,
            colint integer,
            colshort smallint,
            coltiny tinyint,
            coldouble double,
            colfloat float,
            coldate date,
            coldatetime datetime,
            coltime time,
            colint2 integer
        )"""
    )


@pytest.mark.run_loop
async def test_simple(connection):
    prepared = await connection.prepare("SELECT 42")
    await prepared.execute()
    r = await prepared.fetchone()
    assert r[0] == 42, "expecting 42"


@pytest.mark.run_loop
async def test(connection):
    await prepare(connection)
    prepared = await connection.prepare(
        """
        INSERT INTO everytype (
            colchar,
            colset,
            colblob,
            colenum,
            colgeometry,
            colbit,
            coldecimal,
            coljson,
            collong,
            colint,
            colshort,
            coltiny,
            coldouble,
            colfloat,
            coldate,
            coldatetime,
            coltime,
            colint2
        ) VALUES
        (?,?,?,?,ST_GeomFromText('POINT(1 1)'),?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
    )
    await prepared.execute(
        "123", "a", b"\x01\x02", "b", 1, Decimal("123"), '{"a":"b"}', 123, 123,
        123, 123, 123.45, 123.45, datetime.date(2020, 6, 1),
        datetime.datetime(2020, 6, 1, 1, 23, 45, 678),
        datetime.datetime(1, 1, 1, 1, 23, 45), None)

    prepared = await connection.prepare("SELECT * FROM everytype")
    await prepared.execute()
    r = await prepared.fetchone()
    assert r[0] == "123"
    assert r[1] == "a"
    assert r[2] == b"\x01\x02"
    assert r[3] == "b"
    # geometry
    assert isinstance(r[4], bytes)
    with pytest.raises(UnicodeDecodeError):
        r[4].decode('utf-8')
    assert r[5] == b'\x01'
    assert r[6] == Decimal("123")
    assert r[7] == '{"a":"b"}'
    assert r[8] == 123
    assert r[9] == 123
    assert r[10] == 123
    assert r[11] == 123
    assert pytest.approx(r[12]) == 123.45
    assert pytest.approx(r[13]) == 123.45
    assert r[14] == datetime.date(2020, 6, 1)
    assert r[15] == datetime.datetime(2020, 6, 1, 1, 23, 45)
    assert r[16] == datetime.timedelta(hours=1, minutes=23, seconds=45)
    assert r[17] is None
