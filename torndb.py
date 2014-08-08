#!/usr/bin/env python
#-*-coding:utf-8-*-
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A lightweight wrapper around MySQLdb.

Originally part of the Tornado framework.  The tornado.database module
is slated for removal in Tornado 3.0, and it is now available separately
as torndb.
"""

"""
    通读全部源代码，发现其实这个文件做的事，除了对查询做了比较友好的结果封装外，其他的
    查询之类的基本就是老样子，估计是为了接口的完整性，所以简单定义了名字并实现了接口功能
    所以这个接口非常的简单，但是也可以从中学习一下接口的定义过程，虽然实现的功能非常简单
"""

from __future__ import absolute_import, division, with_statement

import copy
import itertools
import logging
import os
import time

try:
    import MySQLdb.constants
    import MySQLdb.converters
    import MySQLdb.cursors
except ImportError:
    # If MySQLdb isn't available this module won't actually be useable,
    # but we want it to at least be importable on readthedocs.org,
    # which has limitations on third-party modules.
    if 'READTHEDOCS' in os.environ:
        MySQLdb = None
    else:
        raise

version = "0.3"
version_info = (0, 3, 0, 0)

class Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage::

        db = torndb.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    We explicitly set the timezone to UTC and assume the character encoding to
    UTF-8 (can be changed) on all connections to avoid time zone and encoding errors.

    The sql_mode parameter is set by default to "traditional", which "gives an error instead of a warning"
    (http://dev.mysql.com/doc/refman/5.0/en/server-sql-mode.html). However, it can be set to
    any other mode including blank (None) thereby explicitly clearing the SQL mode.
    """

    """
        构造器的初始化的过程，就是解析传入的参数，设置连接参数，然后创建一个mysql的连接，
        实例化时，就创建了到mysql的连接，并设置了autocommit，
        创建连接时，会检测是否已经存在连接，若存在，就关闭，然后连接
    """
    def __init__(self, host, database, user=None, password=None,
                 max_idle_time=7 * 3600, connect_timeout=0, 
                 time_zone="+0:00", charset = "utf8", sql_mode="TRADITIONAL"):
        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(conv=CONVERSIONS, use_unicode=True, charset=charset,
                    db=database, init_command=('SET time_zone = "%s"' % time_zone),
                    connect_timeout=connect_timeout, sql_mode=sql_mode)
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()  # 对于实例来说，实例化后这个值不变
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)

    # 删除实例，就是关闭mysql连接
    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        self._db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        """
            这里面做了两件事，1、保证到mysql的连接存在没有断开，否则就重连，更新最后使用时间；
                              2、返回 db.cursor()方法
        """
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)  # 此处执行成功，就有一个cursor对象，里面保存着结果集
            """
                关于cursor.description 可以看文档 
                http://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-description.html
            """
            column_names = [d[0] for d in cursor.description] 

            """
                这里就是封装带来的似乎的好处了，这里有列名，有某一行的所有数据，这里就生成一个列名:列值的字典类型作为一个row行,
                最终返回的是一个列表，按行作为一个元素的列表
                ，后续使用时，根据名字来取值,很直观和方便
                itertools.izip: izip(p, q, ...) --> (p[0], q[0]), (p[1], q[1]), ... 
                利用这个，就可以很方便的组装起来想要拼装的列表或者字典，如果是需要字典，只需要
                dict(itertools.izip(p,q))
                这里虽然也可以使用dict()来转换，但是这里却使用了一个重载了字典的 __getattr__的类，
                好处是在使用这个对象的实例时，才会发生转换？还是说效率高一些？
            """
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    # 这个函数存在这里不晓得是想要搞啥子，只返回查询结果只有一行的结果，很多时候都会异常
    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *parameters, **kwparameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    """
        lastrowid 这个字段的意思可以参考
        http://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-lastrowid.html
        比如对于有自增字段的表来说，当执行了insert操作，那么lastrowid返回回来的就是新插入数据的自增字段的值
    """
    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    # 返回执行成功后受影响的行数
    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    # 方法赋值给另外一个名字，调用时更清晰
    update = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        """
            修复client library的一个缺陷，保证当想要使用连接时，连接没有断开，
            这里采用 当前时间 - 最后使用时间 > 七个小时，那么就重新连接一次，
            无论是否重连，都把最后使用时间更新为当前时间
        """
        
        if (self._db is None or
            (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            """ 
                execute(self, query, args=None)
                args如果不为空，可以是一个序列或者mapping，
                如果是一个序列，那么query中需要占位符 %s，如果是一个mapping，那么需要
                %(key)s 占位
            """
            return cursor.execute(query, kwparameters or parameters)
        except OperationalError:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

if MySQLdb is not None:
    # Fix the access conversions to properly recognize unicode/binary
    FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
    FLAG = MySQLdb.constants.FLAG
    CONVERSIONS = copy.copy(MySQLdb.converters.conversions)

    field_types = [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING]
    if 'VARCHAR' in vars(FIELD_TYPE):
        field_types.append(FIELD_TYPE.VARCHAR)

    for field_type in field_types:
        CONVERSIONS[field_type] = [(FLAG.BINARY, str)] + CONVERSIONS[field_type]

    # Alias some common MySQL exceptions
    IntegrityError = MySQLdb.IntegrityError
    OperationalError = MySQLdb.OperationalError
