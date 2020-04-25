import datetime

import pytest
from pymysql.err import Warning

import aiomysql


@pytest.mark.run_loop
async def test_issue_3(connection):
    """ undefined methods datetime_or_None, date_or_None """
    conn = connection
    c = await conn.cursor()
    await c.execute("drop table if exists issue3")
    await c.execute(
        "create table issue3 (d date, t time, dt datetime, ts timestamp)")
    try:
        await c.execute(
            "insert into issue3 (d, t, dt, ts) values (%s,%s,%s,%s)",
            (None, None, None, None))
        await c.execute("select d from issue3")
        r = await c.fetchone()
        assert r[0] is None
        await c.execute("select t from issue3")
        r = await c.fetchone()
        assert r[0] is None
        await c.execute("select dt from issue3")
        r = await c.fetchone()
        assert r[0] is None
        await c.execute("select ts from issue3")
        r = await c.fetchone()
        assert isinstance(r[0], datetime.datetime)
    finally:
        await c.execute("drop table issue3")


@pytest.mark.run_loop
async def test_issue_4(connection):
    """ can't retrieve TIMESTAMP fields """
    conn = connection
    c = await conn.cursor()
    await c.execute("drop table if exists issue4")
    await c.execute("create table issue4 (ts timestamp)")
    try:
        await c.execute("insert into issue4 (ts) values (now())")
        await c.execute("select ts from issue4")
        r = await c.fetchone()
        assert isinstance(r[0], datetime.datetime)
    finally:
        await c.execute("drop table issue4")


@pytest.mark.run_loop
async def test_issue_5(connection):
    """ query on information_schema.tables fails """
    conn = connection
    cur = await conn.cursor()
    await cur.execute("select * from information_schema.tables")


@pytest.mark.run_loop
async def test_issue_6(connection_creator):
    # test for exception: TypeError: ord() expected a character,
    # but string of length 0 found
    conn = await connection_creator(db='mysql')
    c = await conn.cursor()
    assert conn.db == 'mysql'
    await c.execute("select * from user")
    await conn.ensure_closed()


@pytest.mark.run_loop
async def test_issue_8(connection):
    """ Primary Key and Index error when selecting data """
    conn = connection
    c = await conn.cursor()
    await c.execute("drop table if exists test")
    await c.execute("""CREATE TABLE `test` (
        `station` int(10) NOT NULL DEFAULT '0',
        `dh` datetime NOT NULL DEFAULT '2020-04-25 22:39:12',
        `echeance` int(1) NOT NULL DEFAULT '0', `me` double DEFAULT NULL,
        `mo` double DEFAULT NULL, PRIMARY
        KEY (`station`,`dh`,`echeance`)) ENGINE=MyISAM DEFAULT
        CHARSET=latin1;""")
    try:
        await c.execute("SELECT * FROM test")
        assert 0 == c.rowcount
        await c.execute(
            "ALTER TABLE `test` ADD INDEX `idx_station` (`station`)")
        await c.execute("SELECT * FROM test")
        assert 0 == c.rowcount
    finally:
        await c.execute("drop table test")


@pytest.mark.run_loop
async def test_issue_13(connection):
    """ can't handle large result fields """
    conn = connection
    cur = await conn.cursor()
    await cur.execute("drop table if exists issue13")
    try:
        await cur.execute("create table issue13 (t text)")
        # ticket says 18k
        size = 18 * 1024
        await cur.execute("insert into issue13 (t) values (%s)",
                          ("x" * size,))
        await cur.execute("select t from issue13")
        # use assertTrue so that obscenely huge error messages don't print
        r = await cur.fetchone()
        assert "x" * size == r[0]
    finally:
        await cur.execute("drop table issue13")


@pytest.mark.run_loop
async def test_issue_15(connection):
    """ query should be expanded before perform character encoding """
    conn = connection
    c = await conn.cursor()
    await c.execute("drop table if exists issue15")
    await c.execute("create table issue15 (t varchar(32))")
    try:
        await c.execute("insert into issue15 (t) values (%s)",
                        (u'\xe4\xf6\xfc',))
        await c.execute("select t from issue15")
        r = await c.fetchone()
        assert u'\xe4\xf6\xfc' == r[0]
    finally:
        await c.execute("drop table issue15")


@pytest.mark.run_loop
async def test_issue_16(connection):
    """ Patch for string and tuple escaping """
    conn = connection
    c = await conn.cursor()
    await c.execute("drop table if exists issue16")
    await c.execute("create table issue16 (name varchar(32) "
                    "primary key, email varchar(32))")
    try:
        await c.execute("insert into issue16 (name, email) values "
                        "('pete', 'floydophone')")
        await c.execute("select email from issue16 where name=%s",
                        ("pete",))
        r = await c.fetchone()
        assert "floydophone" == r[0]
    finally:
        await c.execute("drop table issue16")


@pytest.mark.skip(
    "test_issue_17() requires a custom, legacy MySQL configuration and "
    "will not be run.")
@pytest.mark.run_loop
async def test_issue_17(connection, connection_creator, mysql_params):
    """ could not connect mysql use passwod """
    conn = connection
    c = await conn.cursor()
    db = mysql_params['db']
    # grant access to a table to a user with a password
    try:
        await c.execute("drop table if exists issue17")
        await c.execute(
            "create table issue17 (x varchar(32) primary key)")
        await c.execute(
            "insert into issue17 (x) values ('hello, world!')")
        await c.execute("grant all privileges on %s.issue17 to "
                        "'issue17user'@'%%' identified by '1234'"
                        % db)
        await conn.commit()

        conn2 = await connection_creator(user="issue17user",
                                         passwd="1234")
        c2 = await conn2.cursor()
        await c2.execute("select x from issue17")
        r = await c2.fetchone()
        assert "hello == world!", r[0]
    finally:
        await c.execute("drop table issue17")


@pytest.mark.run_loop
async def test_issue_34(connection_creator):
    try:
        await connection_creator(host="localhost", port=1237,
                                 user="root")
        pytest.fail()
    except aiomysql.OperationalError as e:
        assert 2003 == e.args[0]
    except Exception:
        pytest.fail()


@pytest.mark.run_loop
async def test_issue_33(connection_creator):
    conn = await connection_creator(charset='utf8')
    c = await conn.cursor()
    try:
        await c.execute(
            b"drop table if exists hei\xc3\x9fe".decode("utf8"))
        await c.execute(
            b"create table hei\xc3\x9fe (name varchar(32))".decode("utf8"))
        await c.execute(b"insert into hei\xc3\x9fe (name) "
                        b"values ('Pi\xc3\xb1ata')".
                        decode("utf8"))
        await c.execute(
            b"select name from hei\xc3\x9fe".decode("utf8"))
        r = await c.fetchone()
        assert b"Pi\xc3\xb1ata".decode("utf8") == r[0]
    finally:
        await c.execute(b"drop table hei\xc3\x9fe".decode("utf8"))


@pytest.mark.skip("This test requires manual intervention")
@pytest.mark.run_loop
async def test_issue_35(connection):
    conn = connection
    c = await conn.cursor()
    print("sudo killall -9 mysqld within the next 10 seconds")
    try:
        await c.execute("select sleep(10)")
        pytest.fail()
    except aiomysql.OperationalError as e:
        assert 2013 == e.args[0]


@pytest.mark.run_loop
async def test_issue_36(connection_creator):
    conn = await connection_creator()
    c = await conn.cursor()
    # kill connections[0]
    await c.execute("show processlist")
    kill_id = None
    rows = await c.fetchall()
    for row in rows:
        id = row[0]
        info = row[7]
        if info == "show processlist":
            kill_id = id
            break
    try:
        # now nuke the connection
        await conn.kill(kill_id)
        # make sure this connection has broken
        await c.execute("show tables")
        pytest.fail()
    except Exception:
        pass

    # check the process list from the other connection
    conn2 = await connection_creator()
    c = await conn2.cursor()
    await c.execute("show processlist")
    rows = await c.fetchall()
    ids = [row[0] for row in rows]

    assert kill_id not in ids


@pytest.mark.run_loop
async def test_issue_37(connection):
    conn = connection
    c = await conn.cursor()
    assert 1 == (await c.execute("SELECT @foo"))

    r = await c.fetchone()
    assert (None,) == r
    assert 0 == (await c.execute("SET @foo = 'bar'"))
    await c.execute("set @foo = 'bar'")


@pytest.mark.run_loop
async def test_issue_38(connection):
    conn = connection
    c = await conn.cursor()
    # reduced size for most default mysql installs
    datum = "a" * 1024 * 1023

    try:
        await c.execute("drop table if exists issue38")
        await c.execute(
            "create table issue38 (id integer, data mediumblob)")
        await c.execute("insert into issue38 values (1, %s)",
                        (datum,))
    finally:
        await c.execute("drop table issue38")


@pytest.mark.run_loop
async def disabled_test_issue_54(connection):
    conn = connection
    c = await conn.cursor()
    await c.execute("drop table if exists issue54")
    big_sql = "select * from issue54 where "
    big_sql += " and ".join("%d=%d" % (i, i) for i in range(0, 100000))

    try:
        await c.execute(
            "create table issue54 (id integer primary key)")
        await c.execute("insert into issue54 (id) values (7)")
        await c.execute(big_sql)

        r = await c.fetchone()
        assert 7 == r[0]
    finally:
        await c.execute("drop table issue54")


@pytest.mark.run_loop
async def test_issue_66(connection):
    """ 'Connection' object has no attribute 'insert_id' """
    conn = connection
    c = await conn.cursor()
    assert 0 == conn.insert_id()
    try:
        await c.execute("drop table if exists issue66")
        await c.execute("create table issue66 (id integer primary "
                        "key auto_increment, x integer)")
        await c.execute("insert into issue66 (x) values (1)")
        await c.execute("insert into issue66 (x) values (1)")
        assert 2 == conn.insert_id()
    finally:
        await c.execute("drop table issue66")


@pytest.mark.run_loop
async def test_issue_79(connection):
    """ Duplicate field overwrites the previous one in the result
    of DictCursor """
    conn = connection
    c = await conn.cursor(aiomysql.cursors.DictCursor)

    await c.execute("drop table if exists a")
    await c.execute("drop table if exists b")
    await c.execute("""CREATE TABLE a (id int, value int)""")
    await c.execute("""CREATE TABLE b (id int, value int)""")

    a = (1, 11)
    b = (1, 22)
    try:
        await c.execute("insert into a values (%s, %s)", a)
        await c.execute("insert into b values (%s, %s)", b)

        await c.execute("SELECT * FROM a inner join b on a.id = b.id")
        r, *_ = await c.fetchall()
        assert r['id'] == 1
        assert r['value'] == 11
        assert r['b.value'] == 22
    finally:
        await c.execute("drop table a")
        await c.execute("drop table b")


@pytest.mark.run_loop
async def test_issue_95(connection):
    """ Leftover trailing OK packet for "CALL my_sp" queries """
    conn = connection
    cur = await conn.cursor()
    await cur.execute("DROP PROCEDURE IF EXISTS `foo`")
    await cur.execute("""CREATE PROCEDURE `foo` ()
    BEGIN
        SELECT 1;
    END""")
    try:
        await cur.execute("""CALL foo()""")
        await cur.execute("""SELECT 1""")
        r = await cur.fetchone()
        assert r[0] == 1
    finally:
        await cur.execute("DROP PROCEDURE IF EXISTS `foo`")


@pytest.mark.run_loop
async def test_issue_114(connection_creator):
    """ autocommit is not set after reconnecting with ping() """
    conn = await connection_creator(charset="utf8")
    await conn.autocommit(False)
    c = await conn.cursor()
    await c.execute("""select @@autocommit;""")
    r = await c.fetchone()
    assert not r[0]
    await conn.ensure_closed()
    await conn.ping()
    await c.execute("""select @@autocommit;""")
    r = await c.fetchone()
    assert not r[0]
    await conn.ensure_closed()

    # Ensure autocommit() is still working
    conn = await connection_creator(charset="utf8")
    c = await conn.cursor()
    await c.execute("""select @@autocommit;""")
    r = await c.fetchone()
    assert not r[0]
    await conn.ensure_closed()
    await conn.ping()
    await conn.autocommit(True)
    await c.execute("""select @@autocommit;""")
    r = await c.fetchone()
    assert r[0]
    await conn.ensure_closed()


@pytest.mark.run_loop
async def test_issue_175(connection):
    """ The number of fields returned by server is read in wrong way """
    conn = connection
    cur = await conn.cursor()
    for length in (200, 300):
        cols = ', '.join('c{0} integer'.format(i) for i in range(length))
        sql = 'create table test_field_count ({0})'.format(cols)
        try:
            await cur.execute(sql)
            await cur.execute('select * from test_field_count')
            assert len(cur.description) == length
        finally:
            await cur.execute('drop table if exists test_field_count')


# MySQL will get you to renegotiate if sent a cleartext password
@pytest.mark.run_loop
async def test_issue_323(mysql_server, loop, recwarn):
    async with aiomysql.create_pool(**mysql_server['conn_params'],
                                    loop=loop) as pool:
        async with pool.get() as conn:
            async with conn.cursor() as cur:
                drop_db = "DROP DATABASE IF EXISTS bugtest;"
                await cur.execute(drop_db)

                create_db = "CREATE DATABASE bugtest;"
                await cur.execute(create_db)

                create_table = """CREATE TABLE IF NOT EXISTS `bugtest`.`testtable` (
                `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
                `bindata` VARBINARY(200) NOT NULL,
                PRIMARY KEY (`id`)
                );"""

                await cur.execute(create_table)

            try:
                recwarn.clear()

                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO `bugtest`.`testtable` "
                                      "(bindata) VALUES (%s);",
                                      (b'\xB0\x17',))

                    warnings = [warn for warn in recwarn.list
                                if warn.category is Warning]
                    assert len(warnings) == 0, \
                        "Got unexpected MySQL warning {}".\
                        format(' '.join(str(x) for x in warnings))

                    await cur.execute("SELECT * FROM `bugtest`.`testtable`;")
                    rows = await cur.fetchall()

                    assert len(rows) == 1, "Table should have 1 row"

            finally:
                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM `bugtest`.`testtable`;")
