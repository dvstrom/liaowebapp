#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "dvstrom"

'''
Database operation module
'''
import time, uuid, functools, threading, logging

class Dict(dict):

    '''

    '''
    def __init__(self, names=(), values=(), **kw):
        super(Dict,self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self,key):
        try:
            return self[k]
        except KeyError:
             raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self,key,value):
        self[key]= value

        
engine = None
class _Engine(object):

    def __init__(self,connect):
        self._connect = connect

    def connect(self):
        return self._connect

class DBError(Exception):
    pass

def create_engine(user,password,database,host="127.0.0.1",port=5432,**kw):
    import psycopg2
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized')
    params = dict(user=user,password=password,database=database,host=host,port=port)
    defaults = dict(connection_factory=None,cursor_factory=None,async=False)
    for k, v in defaults.iteritems():
        params[k]=kw.pop(k,v)
    params.update(kw)
    engine = _Engine(lambda: psycopg2.connect(**params))
    logging.info('Init postgresql engine <%s> ok.'%hex(id(engine)))
    


class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transaction = 0

    def is_int(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transaction = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def course(self):
        return self.connection.course()

_db_ctx = _DbCtx

class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_int():
            _db_ctx.init()
            self.should_cleanup = True
        return self
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


def connection():
    return _ConnectionCtx()


def with_connection(func):
    @functools.wraps(func)
    def _warapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
        return _warapper

