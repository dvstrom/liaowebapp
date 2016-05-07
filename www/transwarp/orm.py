#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'whcy'

'''  

'''
import time,logging
import db

class Field(object):


    def __init__(self, **kw):
        "docstring"
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count = Field._count + 1

    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' %(self.__class__.__name__,self.name,self.ddl,self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)

class StringField(Field):

    def __init__(self, **kw):
        "docstring"
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar(225)'
        super(StringField,self).__init__(**kw)

class IntegerField(Field):

    def __init__(self, **kw):
        '''
        '''
        
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)

class FloatField(Field):

    def __init__(self,**kw):
        "docstring"

        if not 'default' in kw:
            kw['default'] = 0.0
        if not 'ddl' in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)
class BooleanField(Field):

    def __init__(self,**kw):
        "docstring"
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)

class TextField(Field):

    def __init__(self,**kw):

        "docstring"

        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'text'
        super(TextField, self).__init__(**kw)

class BlobField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)

class VersionField(Field):

    def __init__(self,name=None):
        super(VersionField, self).__init__(name=name,default=0,ddl='bigint')


_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

def _gen_sql(table_name, mappings):
    pk = None
    sql = ['--generating SQL for %s:' %table_name,'create talbe `%s`('% table_name]
    for f in sorted(mappings.values(), lambda x, y:cmp(x.order,y.order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' %f)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and ' `%s` %s,'% (f.name, ddl) or '`%s` %s not null,'%(f.name,ddl))
        sql.append(');')
        return '\n'.join(sql)

class ModelMetaclass(type):

    '''
    Metaclass for model object
    '''
    def __new__(mcs, name, bases, attrs):
        """
        skip base Model class:
        """
        if name == "model":
            return type.__new__(mcs, name, bases, attrs)
        """
        store all subclasses info
        """
        if not hasattr(mcs, 'subclasses'):
            mcs.subclasses = {}
        if not name in mcs.subclasses:
            mcs.subclasses[name] = name
        else:
            logging.warning('Redefine class:%s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping:%s=> %s' %(k,v))
                """
                check duplicate primary key
                """
                if v.primary_key:
                    if primary_key:
                        raise TypeError('cannot define more than 1 primary key in class:%s' % name)
                    if v.updatable:
                        logging.warning('NOTE:change primary key to non-updatale')
                        v.updatable = False
                    if v.nullable: 
                        logging.warning('NOTE:change primary key to non-nullable')
                        v.nullable = False
                    primary_key = v
                mappings[k] = v
        """
        check exist of primary key
        """
        if not primary_key:
            raise TypeError('primary key not define in class:%s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mapping__'] = mappings
        attrs['__primary_key'] = primary_key
        attrs['__sql__']=lambda self:_gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None
        return type.__new__(mcs,name,bases,attrs)

