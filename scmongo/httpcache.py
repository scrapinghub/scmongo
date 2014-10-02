"""
Mongo Cache Storage

A MongoDB backend for HTTP cache storage. It stores responses using GridFS.

To use it, set the following Scrapy setting in your project:

    HTTPCACHE_STORAGE = 'scmongo.httpcache.MongoCacheStorage'

"""
import os
from time import time

from scrapy import log
from scrapy.responsetypes import responsetypes
from scrapy.exceptions import NotConfigured
from scrapy.utils.request import request_fingerprint
from scrapy.http import Headers

try:
    from pymongo import MongoClient, MongoReplicaSetClient
    from pymongo.errors import ConfigurationError
    from pymongo import version_tuple as mongo_version
    from gridfs import GridFS, errors
except ImportError:
    MongoClient = None

def get_database(settings):
    """Return Mongo database based on the given settings, also pulling the
    mongo host, port and database form the environment variables if they're not
    defined in the settings.

    HOST may be a 'mongodb://' URI string, in which case it will override any
    set PORT and DATABASE parameters.

    If user and password parameters are specified and also passed in a
    mongodb URI, the call to authenticate() later (probably) overrides the URI
    string's earlier login.

    Specifying an auth 'mechanism' or 'source' (Mongo 2.5+) currently only
    works by using a host URI string (we don't pass these to authenticate()),
    any kwargs will be passed on to the MongoClient call (e.g. for ssl setup).
    """

    conf = {
        'host': settings['HTTPCACHE_MONGO_HOST'] \
                or settings['MONGO_HOST'] \
                or os.environ.get('MONGO_HOST')
                or 'localhost',
        'port': settings.getint('HTTPCACHE_MONGO_PORT') \
                or settings.getint('MONGO_PORT') \
                or int(os.environ.get('MONGO_PORT', '27017')) \
                or 27017,
        'db': settings['HTTPCACHE_MONGO_DATABASE'] \
                or settings['MONGO_DATABASE'] \
                or os.environ.get('MONGO_DATABASE') \
                or settings['BOT_NAME'],
        'user': settings['HTTPCACHE_MONGO_USERNAME'] \
                or settings['MONGO_USERNAME'] \
                or os.environ.get('MONGO_USERNAME'),
        'password': settings['HTTPCACHE_MONGO_PASSWORD'] \
                or settings['MONGO_PASSWORD'] \
                or os.environ.get('MONGO_PASSWORD'),
    }
    # Support passing any other options to MongoClient;
    # options passed as "positional arguments" take precedence
    kwargs = settings.getdict('HTTPCACHE_MONGO_CONFIG', None) \
                or settings.getdict('MONGO_CONFIG')
    conf.update(kwargs)
    return conf


class MongoCacheStorage(object):
    """Storage backend for Scrapy HTTP cache, which stores responses in MongoDB
    GridFS.

    If HTTPCACHE_SHARDED is True, a different collection will be used for
    each spider, similar to FilesystemCacheStorage using folders per spider.
    """

    def __init__(self, settings, **kw):
        if MongoClient is None:
            raise NotConfigured('%s is missing pymongo or gridfs module.' %
                                self.__class__.__name__)
        if (2,4,0) > mongo_version:
            version = '.'.join('%s'% v for v in mongo_version)
            raise NotConfigured('%s requires pymongo version >= 2.4 but got %s' %
                    (self.__class__.__name__, version))
        self.expire = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.sharded = settings.getbool('HTTPCACHE_SHARDED', False)
        kwargs = get_database(settings)
        kwargs.update(kw)
        db = kwargs.pop('db')
        user = kwargs.pop('user', None)
        password = kwargs.pop('password', None)
        if 'replicaSet' in kwargs:
            client = MongoReplicaSetClient(**kwargs)
        else:
            client = MongoClient(**kwargs)

        # do not override a database passed in a 'mongodb://' URI string
        try:
            self.db = client.get_default_database()
        except ConfigurationError:
            self.db = client[db]
        except TypeError:
            # get_default_database() only since pymongo 2.6, but
            # pymongo>2.5.2 only works with mongodb > ~2.4.3
            # fall back to parsing uri string in this edge-case :(
            if 'mongodb://' in kwargs.get('host'):
                try:
                    from urlparse import urlparse
                    loc = urlparse(uri).path.strip('/')
                    if not loc:
                        self.db = client[db]
                except (ImportError, Exception):
                    client.connection.close()
                    raise NotConfigured('%s could not reliably detect if \
                    there was a database passed in URI string. Please install \
                    urlparse to fix this, or use host:port arguments instead.' %
                    self.__class__.__name__)
            else:
                self.db = client[db]

        if user is not None and password is not None:
            self.db.authenticate(user, password)
        log.msg('%s connected to %s:%s, using database \'%s\'' %
                (self.__class__.__name__, client.host, client.port, db),
                level=log.DEBUG)
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
        key = self._request_key(spider, request)
        gf = self._get_file(spider, key)
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
        metadata = {
            '_id': key,
            'time': time(),
            'status': response.status,
            'url': response.url,
            'headers': dict(response.headers),
        }
        try:
            self.fs[spider].put(response.body, **metadata)
        except errors.FileExists:
            self.fs[spider].delete(key)
            self.fs[spider].put(response.body, **metadata)

    def _get_file(self, spider, key):
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
        #    return rfp
        return '%s/%s' % (spider.name, rfp)
