=======
scmongo
=======

This project contains some extensions for using MongoDB with the Scrapy
web crawling framework.

Requirements
============

* Scrapy 0.14 or above
* pymongo 2.4 or above

Install
=======

Download and run: ``python setup.py install``

Available extensions
====================

Mongo Cache Storage
-------------------

Module: ``scmongo.httpcache``

A MongoDB backend for HTTP cache storage. It stores responses using GridFS.

To use it, set the following Scrapy setting in your project::

    HTTPCACHE_STORAGE = 'scmongo.httpcache.MongoCacheStorage'
