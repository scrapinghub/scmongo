=======
scmongo
=======

This project contains some extensions for using MongoDB with the Scrapy
web crawling framework.

Requirements
============

* Scrapy 0.12 or above
* pymongo 1.6 or above

Install
=======

Download and run: ``python setup.py install``

Available extensions
====================

Mongo Cache Storage
-------------------

Class: ``scmongo.httpcache.MongoCacheStorage``

A MongoDB backend for HTTP cache storage. It stores responses using GridFS.
