import string
import random
import asyncio
from collections import namedtuple

import pytest
import aiomysql

from ._testutils import mysql_server_is


TempUser = namedtuple('TempUser', 'name,password,db')


@pytest.yield_fixture
def temp_user_creator(cursor, loop):
    users = []

    @asyncio.coroutine
    def _create_user(*, user, db, auth_plugin=None, generate_password=False):
        if generate_password:
            password = ''.join(
                random.choice(string.ascii_uppercase + string.digits)
                for _ in range(6)
            )
        else:
            password = None
        create_sql = "CREATE USER " + user
        if password is not None:
            create_sql += " IDENTIFIED BY '%s'" % password
        elif auth_plugin is not None:
            create_sql += " IDENTIFIED WITH %s" % auth_plugin

        yield from cursor.execute(create_sql)

        grant_sql = "GRANT SELECT ON %s.* TO %s" % (db, user)
        yield from cursor.execute(grant_sql)

        temp_user = TempUser(user, password, db)

        users.append(temp_user)
        return temp_user

    yield _create_user

    for _user in users:
        loop.run_until_complete(cursor.execute("DROP USER %s" % _user.name))


@pytest.mark.run_loop
def test_mysql_old_password_plugin(cursor, temp_user_creator,
                                   mysql_params, loop):
    server_info = cursor.connection.server_version
    if mysql_server_is(server_info, (5, 6, 0)):
        pytest.skip(
            "Old passwords aren't supported in version higher than 5.5"
        )

    password = "test password"
    yield from cursor.execute("SELECT OLD_PASSWORD(%s)", password)
    resp = yield from cursor.fetchone()
    assert resp[0] == '34464d3918c0793c'

    yield from cursor.execute("SELECT @@secure_auth")
    resp = yield from cursor.fetchone()
    secure_auth_setting = resp[0]

    try:
        yield from cursor.execute('set old_passwords=1')
        yield from cursor.execute('set global secure_auth=0')

        user = yield from temp_user_creator(
            user='old_pass_user', db=mysql_params['db'], generate_password=True
        )
        params = mysql_params.copy()
        params.update({
            'user': user.name,
            'password': user.password
        })

        conn = yield from aiomysql.connect(loop=loop, **params)
        cur = yield from conn.cursor()
        yield from cur.execute("SELECT VERSION()")
        yield from cur.close()
        conn.close()

    finally:
        yield from cursor.execute(
            'set global secure_auth=%s', secure_auth_setting)


@pytest.mark.run_loop
def test_mysql_native_password(temp_user_creator, mysql_params, loop):
    user = yield from temp_user_creator(user='test', db=mysql_params['db'],
                                        auth_plugin='mysql_native_password')
    params = mysql_params.copy()
    params.update({
        'user': user.name,
        'password': user.password
    })
    conn = yield from aiomysql.connect(loop=loop, **params)
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT VERSION()")
    yield from cur.close()
    conn.close()
