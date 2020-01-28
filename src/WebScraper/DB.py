from pymongo import collection, MongoClient, ASCENDING
from urllib.request import urlopen
from utils import FilterLinks


class Database:

    websDB = MongoClient(port=27017).websDB

    notVisited = websDB['notVisited']
    visited = websDB['notVisited']
    error = websDB['error']

    @staticmethod
    def Reset():
        '''Resets the database to defaults'''
        print('Resetting database ...')
        urlList = list(set(urlopen(
            'http://fmartinz.webs.ull.es/adquisicion/digitales.txt').read().decode('utf8').split('\n')))

        Database.notVisited.delete_many({})
        Database.notVisited.create_index(
            [('url', ASCENDING)], unique=True)
        Database.visited.delete_many({})
        Database.visited.create_index(
            [('url', ASCENDING)], unique=True)
        Database.error.delete_many({})
        Database.error.create_index(
            [('url', ASCENDING)], unique=True)

        Database.notVisited.insert_many(
            list(map(lambda url: {'url': url, 'depth': 0}, urlList)))

    @staticmethod
    def InsertNotVisitedWebpages(urls, depth):
        '''Adds urls to notVisited collection'''
        urls = FilterLinks(urls)

        documents = []
        for url in urls:
            documents.append({'url': url, 'depth': depth})

        Database.notVisited.insert_many(documents)
