import os
import pymongo

def get_database(settings):
    """Return Mongo database based on the given settings, also pulling the
    mongo host, port and database form the environment variables if they're not
    defined in the settings.
    """

    host = settings['HTTPCACHE_MONGO_HOST'] \
        or os.environ.get('MONGO_HOST', 'localhost')
    port = int(settings['HTTPCACHE_MONGO_PORT'] \
        or os.environ.get('MONGO_PORT', '27017'))
    db = settings['HTTPCACHE_MONGO_DATABASE'] \
        or os.environ.get('MONGO_DATABASE', settings.get['BOT_NAME'])

    return pymongo.Connection(host, port)[db]
