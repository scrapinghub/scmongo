from distutils.core import setup

setup(name='scmongo',
      version='0.1',
      license='BSD',
      description='Scrapy extensions for MongoDB',
      author='Scrapinghub',
      author_email='info@scrapinghub.com',
      url='http://github.com/scrapinghub/scmongo',
      keywords="scrapy mongodb",
      packages=['scmongo'],
      platforms = ['Any'],
      install_requires = ['Scrapy', 'pymongo'],
      classifiers = [ 'Development Status :: 4 - Beta',
                      'License :: OSI Approved :: BSD License',
                      'Operating System :: OS Independent',
                      'Programming Language :: Python']
)
