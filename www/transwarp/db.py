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

def next_id(t=None):
    if t is None:
        t = time.time()
    return '%015d%s000' %(int(t*1000),uuid.uuid4.hex)

def _profiling(start,sql=''):
    t=time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s:%s' %(t,sql))
    else:
        logging.info('[PROFILING] [DB] %s:%s' %(t,sql))
    



class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass

class _lasyConnection(object):

    def __init__(self):
        self.connection = None
        
    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>...' %hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...'%hex(id(connection)))
            connection.close()
        
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

        
engine = None
class _Engine(object):

    def __init__(self,connect):
        self._connect = connect

    def connect(self):
        return self._connect


def create_engine(user,password,database,host="127.0.0.1",port=5432,**kw):
    import psycopg2
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized')
    params = dict(user=user,password=password,database=database,host=host,port=port)
    print params
    defaults = dict(connection_factory=None,cursor_factory=None,async=False)
    for k, v in defaults.iteritems():
        params[k]=kw.pop(k,v)
    params.update(kw)
    print ("beging con postgresql")
    logging.info("beginng connect postgresql")
    engine = _Engine(lambda: psycopg2.connect(**params))
    logging.info('Init postgresql engine <%s> ok.'%hex(id(engine)))
    print ('Init postgresql enging %s ok' %hex(id(engine)))
    



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

class _TransactionCtx(object):

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_int():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('beging transactions...' if _db_ctx.transactions==1 else 'join current transactions')

        return self

    def __exit__(self, exctype, value, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions ==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()

        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok')
        except :
            logging.warning('commit failed try rollback')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok')

def transaction():
    return _TransactionCtx()


            
def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args,**kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper

            
