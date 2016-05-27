#!/usr/bin/env python
# -*- coding: utf-8 -*-

from models import Blog, comment, testuser2
from transwarp import db

db.create_engine(user='chenyu', password='cy78102', database='test2')

u = testuser2(name='Test', email='test@example.com', password='1234567890', image='about:blank')

u.insert()

print 'new user id:', u.id

u1 = testuser2.find_first('where email=?', 'test@example.com')
print 'find user\'s name:', u1.name

u1.delete()

u2 = testuser2.find_first('where email=?', 'test@example.com')
print 'find user:', u2
 
