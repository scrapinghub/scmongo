import os
import pymongo

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

    return pymongo.Connection(host, port)[db]
