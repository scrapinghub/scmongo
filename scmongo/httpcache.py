import cPickle as pickle
from time import time

from scrapy.http import Headers
from scrapy.core.downloader.responsetypes import responsetypes
from scrapy import conf
from scrapy.utils.request import request_fingerprint

from scmongo.util import get_database


class MongoCacheStorage(object):

    def __init__(self, settings=conf.settings):
        self.expiration_secs = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.db = get_database(settings)
        self.cols = {}

    def open_spider(self, spider):
        self.cols[spider] = self.db['httpcache.%s' % spider.name]

    def close_spider(self, spider):
        del self.cols[spider]

    def retrieve_response(self, spider, request):
        data = self._read_data(spider, request)
        if data is None:
            return # not cached
        url = data['url']
        status = data['status']
        headers = Headers(data['headers'])
        body = data['body']
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response
 
    def store_response(self, spider, request, response):
        key = self._request_key(request)
        data = self._response_data(response)
        doc = {
            '_id': key,
            'time': str(time()),
            'data': pickle.dumps(data),
        }
        self.cols[spider].save(doc)

    def _read_data(self, spider, request):
        key = self._request_key(request)
        response = self.cols[spider].find_one(key)
        if response is None:
            return # not found
        if 0 < self.expiration_secs < time() - float(response['time']):
            return # expired
        return pickle.loads(str(response['data']))

    def _request_key(self, request):
        return request_fingerprint(request)

    def _response_data(self, response):
        return {
            'status': response.status,
            'url': response.url,
            'headers': dict(response.headers),
            'body': response.body,
        }
