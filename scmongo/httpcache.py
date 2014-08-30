"""
Mongo Cache Storage

A MongoDB backend for HTTP cache storage. It stores responses using GridFS.

To use it, set the following Scrapy setting in your project:

    HTTPCACHE_STORAGE = 'scmongo.httpcache.MongoCacheStorage'

"""
import os
from time import time

from scrapy.responsetypes import responsetypes
from scrapy.exceptions import NotConfigured
from scrapy.utils.request import request_fingerprint
from scrapy.http import Headers

try:
    from pymongo import MongoClient
    from gridfs import GridFS, errors
except ImportError:
    MongoClient = None

def get_database(settings):
    """Return Mongo database based on the given settings, also pulling the
    mongo host, port and database form the environment variables if they're not
    defined in the settings.
    """

    host = settings['HTTPCACHE_MONGO_HOST'] \
        or settings['MONGO_HOST'] \
        or os.environ.get('MONGO_HOST') \
        or 'localhost'
    port = settings.getint('HTTPCACHE_MONGO_PORT') \
        or settings.getint('MONGO_PORT') \
        or int(os.environ.get('MONGO_PORT', '27017')) \
        or 27017
    db = settings['HTTPCACHE_MONGO_DATABASE'] \
        or settings['MONGO_DATABASE'] \
        or os.environ.get('MONGO_DATABASE') \
        or settings['BOT_NAME']
    user = settings['HTTPCACHE_MONGO_USERNAME'] \
        or settings['MONGO_USERNAME'] \
        or os.environ.get('MONGO_USERNAME')
    password = settings['HTTPCACHE_MONGO_PASSWORD'] \
        or settings['MONGO_PASSWORD'] \
        or os.environ.get('MONGO_PASSWORD')

    return (host, port, db, user, password)


class MongoCacheStorage(object):
    """Storage backend for Scrapy HTTP cache, which stores responses in MongoDB
    GridFS.

    If HTTPCACHE_SHARDED is True, a different collection will be used for
    each spider, similar to FilesystemCacheStorage using folders per spider.
    """

    def __init__(self, settings):
        if MongoClient is None:
            raise NotConfigured('%s is missing pymongo or gridfs module.' %
                                self.__class__.__name__)
        self.expire = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.sharded = settings.getbool('HTTPCACHE_SHARDED', False)
        host, port, db, user, password = get_database(settings)
        self.db = MongoClient(host, port)[db]
        if user is not None and password is not None:
            self.db.authenticate(user, password)
        self.fs = {}

    def open_spider(self, spider):
        _shard = 'httpcache'
        if self.sharded:
            _shard = 'httpcache.%s' % spider.name
        self.fs[spider] = GridFS(self.db, _shard)

    def close_spider(self, spider):
        del self.fs[spider]

    def __del__(self):
        if hasattr(self, 'db'):
            self.db.connection.close()

    def retrieve_response(self, spider, request):
        gf = self._get_file(spider, request)
        if gf is None:
            return # not cached
        url = str(gf.url)
        status = str(gf.status)
        headers = Headers([(x, map(str, y)) for x, y in gf.headers.iteritems()])
        body = gf.read()
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        key = self._request_key(spider, request)
        kwargs = {
            '_id': key,
            'time': time(),
            'status': response.status,
            'url': response.url,
            'headers': dict(response.headers),
        }
        try:
            self.fs[spider].put(response.body, **kwargs)
        except errors.FileExists:
            self.fs[spider].delete(key)
            self.fs[spider].put(response.body, **kwargs)

    def _get_file(self, spider, request):
        key = self._request_key(spider, request)
        try:
            gf = self.fs[spider].get(key)
        except errors.NoFile:
            return # not found
        if 0 < self.expire < time() - gf.time:
            return # expired
        return gf

    def _request_key(self, spider, request):
        rfp = request_fingerprint(request)
        # We could disable the namespacing in sharded mode (old behaviour),
        # but keeping it allows us to merge collections later without
        # worrying about key conflicts.
        #if self.sharded:
        #    return request_fingerprint(request)
        return '%s/%s' % (spider.name, request_fingerprint(request))
