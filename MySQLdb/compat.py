from ctypes import string_at, create_string_buffer

from MySQLdb import libmysql


def string_literal(obj):
    if isinstance(obj, str):
        obj = obj.encode('utf-8')
    else:
        obj = str(obj).encode('utf-8')
    buf = create_string_buffer(len(obj) * 2)
    length = libmysql.c.mysql_escape_string(buf, obj, len(obj))
    return "'%s'" % string_at(buf, length).decode('utf-8')
