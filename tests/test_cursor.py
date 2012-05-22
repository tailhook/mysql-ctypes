# -*- coding: utf-8 -*-

import contextlib
import datetime
import warnings

import py

from MySQLdb.cursors import DictCursor
from MySQLdb.constants import CLIENT

from .base import BaseMySQLTests


class TestCursor(BaseMySQLTests):
    def assert_roundtrips(self, connection, obj, check_type=True):
        with contextlib.closing(connection.cursor()) as cur:
            cur.execute("SELECT %s", (obj,))
            row, = cur.fetchall()
            val, = row
            if check_type:
                assert type(val) is type(obj)
            assert val == obj

    def test_basic_execute(self, connection):
        with self.create_table(connection, "things", name="VARCHAR(20)"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.execute("INSERT INTO things (name) VALUES ('website')")
                cur.execute("SELECT name FROM things")
                results = cur.fetchall()
                assert results == [("website",)]

    def test_fetchmany_insert(self, connection):
        with self.create_table(connection, "things", name="VARCHAR(20)"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.execute("INSERT INTO things (name) VALUES ('website')")
                with py.test.raises(connection.ProgrammingError):
                    cur.fetchmany()

    def test_iterable(self, connection):
        with self.create_table(connection, "users", uid="INT"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.executemany("INSERT INTO users (uid) VALUES (%s)", [(i,) for i in range(5)])
                cur.execute("SELECT * FROM users")
                x = iter(cur)
                row = cur.fetchone()
                assert row == (0,)
                row = next(x)
                assert row == (1,)
                rows = cur.fetchall()
                assert rows == [(2,), (3,), (4,)]
                with py.test.raises(StopIteration):
                    next(x)

    def test_lastrowid(self, connection):
        with self.create_table(connection, "users", uid="INT NOT NULL AUTO_INCREMENT", primary_key="uid"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.execute("INSERT INTO users () VALUES ()")
                assert cur.lastrowid

    def test_autocommit(self, connection):
        with self.create_table(connection, "users", uid="INT"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.executemany("INSERT INTO users (uid) VALUES (%s)", [(i,) for i in range(5)])
                connection.rollback()
                cur.execute("SELECT COUNT(*) FROM users")
                c, = cur.fetchall()
                assert c == (0,)

    def test_none(self, connection):
        with contextlib.closing(connection.cursor()) as cur:
            cur.execute("SELECT %s", (None,))
            row, = cur.fetchall()
            assert row == (None,)

    def test_bool(self, connection):
        self.assert_roundtrips(connection, True, check_type=False)
        self.assert_roundtrips(connection, False, check_type=False)

    def test_longlong(self, connection):
        with contextlib.closing(connection.cursor()) as cur:
            cur.execute("SHOW COLLATION")
            cur.fetchone()

    def test_datetime(self, connection):
        with self.create_table(connection, "events", dt="TIMESTAMP"):
            with contextlib.closing(connection.cursor()) as cur:
                t = datetime.datetime.combine(datetime.datetime.today(), datetime.time(12, 20, 2))
                cur.execute("INSERT INTO events (dt) VALUES (%s)", (t,))
                cur.execute("SELECT dt FROM events")
                r, = cur.fetchall()
                assert r == (t,)

    def test_date(self, connection):
        with contextlib.closing(connection.cursor()) as cur:
            cur.execute("SELECT CURDATE()")
            row, = cur.fetchall()
            assert row == (datetime.date.today(),)

    def test_time(self, connection):
        with self.create_table(connection, "events", t="TIME"):
            with contextlib.closing(connection.cursor()) as cur:
                t = datetime.time(12, 20, 2)
                cur.execute("INSERT INTO events (t) VALUES (%s)", (t,))
                cur.execute("SELECT t FROM events")
                r, = cur.fetchall()
                # Against all rationality, MySQLdb returns a timedelta here
                # immitate this idiotic behavior.
                assert r == (datetime.timedelta(hours=12, minutes=20, seconds=2),)

    def test_binary(self, connection):
        self.assert_roundtrips(connection, "".join(chr(x) for x in range(255)))
        self.assert_roundtrips(connection, 'm\xf2\r\n')

    def test_blob(self, connection):
        with self.create_table(connection, "people", name="BLOB"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.execute("INSERT INTO people (name) VALUES (%s)", (bytes(x for x in range(255)),))
                cur.execute("SELECT * FROM people")
                row, = cur.fetchall()
                val, = row
                assert val == bytes(x for x in range(255)), val
                assert type(val) is bytes

    def test_nonexistant_table(self, connection):
        with contextlib.closing(connection.cursor()) as cur:
            with py.test.raises(connection.ProgrammingError) as cm:
                cur.execute("DESCRIBE t")
            assert cm.value.args[0] == 1146

    def test_rowcount(self, connection):
        with self.create_table(connection, "people", age="INT"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.execute("DESCRIBE people")
                assert cur.rowcount == 1

                cur.execute("DELETE FROM people WHERE age = %s", (10,))
                assert cur.rowcount == 0

                cur.executemany("INSERT INTO people (age) VALUES (%s)", [(i,) for i in range(5)])
                cur.execute("DELETE FROM people WHERE age < %s", (3,))
                assert cur.rowcount == 3

    def test_limit(self, connection):
        with self.create_table(connection, "people", age="INT"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.executemany("INSERT INTO people (age) VALUES (%s)", [(10,), (11,)])
                cur.execute("SELECT * FROM people ORDER BY age LIMIT %s", (1,))
                rows = cur.fetchall()
                assert rows == [(10,)]

    @py.test.mark.connect_opts(client_flag=CLIENT.FOUND_ROWS)
    def test_found_rows_client_flag(self, connection):
        with self.create_table(connection, "people", age="INT"):
            with contextlib.closing(connection.cursor()) as cur:
                cur.execute("INSERT INTO people (age) VALUES (20)")
                cur.execute("UPDATE people SET age = 20 WHERE age = 20")
                assert cur.rowcount == 1

    def test_broken_execute(self, connection):
        with contextlib.closing(connection.cursor()) as cursor:
            cursor.execute("SELECT %s", "hello")
            row, = cursor.fetchall()
            assert row == ("hello",)

            cursor.execute("SELECT %s, %s", ["Hello", "World"])
            row, = cursor.fetchall()
            assert row == ("Hello", "World")

    def test_integrity_error(self, connection):
        with self.create_table(connection, "people", uid="INT", primary_key="uid"):
            with contextlib.closing(connection.cursor()) as cursor:
                cursor.execute("INSERT INTO people (uid) VALUES (1)")
                with py.test.raises(connection.IntegrityError):
                    cursor.execute("INSERT INTO people (uid) VALUES (1)")

    def test_description(self, connection):
        with self.create_table(connection, "people", uid="INT"):
            with contextlib.closing(connection.cursor()) as cursor:
                cursor.execute("DELETE FROM people WHERE uid = %s", (1,))
                assert cursor.description is None

                cursor.execute("SELECT uid FROM people")
                assert len(cursor.description) == 1
                assert cursor.description[0][0] == b"uid"

    def test_executemany_return(self, connection):
        with self.create_table(connection, "people", uid="INT"):
            with contextlib.closing(connection.cursor()) as cursor:
                r = cursor.executemany("INSERT INTO people (uid) VALUES (%s)", [(1,), (2,)])
                assert r == 2

    def test_unicode(self, connection):
        with self.create_table(connection, "snippets", content="TEXT"):
            with contextlib.closing(connection.cursor()) as cursor:
                unicodedata = ("Alors vous imaginez ma surprise, au lever du "
                    "jour, quand une drôle de petite voix m’a réveillé. Elle "
                    "disait: « S’il vous plaît… dessine-moi un mouton! »")
                cursor.executemany("INSERT INTO snippets (content) VALUES (%s)", [
                    (unicodedata.encode("utf-8"),),
                    (unicodedata.encode("utf-8"),),
                ])
                cursor.execute("SELECT content FROM snippets LIMIT 1")
                r, = cursor.fetchall()
                v, = r
                v = v.decode("utf-8")
                assert isinstance(v, str)
                assert v == unicodedata


class TestDictCursor(BaseMySQLTests):
    def test_fetchall(self, connection):
        with self.create_table(connection, "people", name="VARCHAR(20)", age="INT"):
            with contextlib.closing(connection.cursor(DictCursor)) as cur:
                cur.execute("INSERT INTO people (name, age) VALUES ('guido', 50)")
                cur.execute("SELECT * FROM people")
                rows = cur.fetchall()
                assert rows == [{"name": "guido", "age": 50}]

    def test_fetchmany(self, connection):
        with self.create_table(connection, "users", uid="INT"):
            with contextlib.closing(connection.cursor(DictCursor)) as cur:
                cur.executemany("INSERT INTO users (uid) VALUES (%s)", [(i,) for i in range(10)])
                cur.execute("SELECT * FROM users")
                rows = cur.fetchmany()
                assert rows == [{"uid": 0}]
                rows = cur.fetchmany(2)
                assert rows == [{"uid": 1}, {"uid": 2}]

    def test_fetchone(self, connection):
        with self.create_table(connection, "salads", country="VARCHAR(20)"):
            with contextlib.closing(connection.cursor(DictCursor)) as cur:
                cur.execute("INSERT INTO salads (country) VALUES ('Italy')")
                cur.execute("SELECT * FROM salads")
                row = cur.fetchone()
                assert row == {"country": "Italy"}
                row = cur.fetchone()
                assert row is None
