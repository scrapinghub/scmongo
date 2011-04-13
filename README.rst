=======
scmongo
=======

This project contains some extensions for using MongoDB with the Scrapy
web crawling framework.

Currently available classes:

Mongo Cache Storage
-------------------

Class: ``scmongo.httpcache.MongoCacheStorage``

A HTTP cache storage backend that stores responses on MongoDB (GridFS).

Requirements
============

* Scrapy 0.12 or above
* pymongo 1.6 or above

Install
=======

Download and run: ``python setup.py install``
