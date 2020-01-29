from pymongo import collection, MongoClient, ASCENDING
from urllib.request import urlopen
from Utils import IsBaseDomain, GetBaseDomain


class Database:

    client = MongoClient(port=27017)

    notVisited = client.websDB['notVisited']
    visited = client.websDB['visited']
    error = client.websDB['error']

    @staticmethod
    def Reset():
        '''Resets the database to defaults'''
        print('Resetting database ...')
        urlList = list(set(urlopen(
            'http://fmartinz.webs.ull.es/adquisicion/digitales.txt').read().decode('utf8').split('\n')))
        Database.notVisited.drop()
        Database.notVisited.create_index(
            [('url', ASCENDING)], unique=True)

        Database.visited.drop()
        Database.visited.create_index(
            [('url', ASCENDING)], unique=True)

        Database.error.drop()
        Database.error.create_index(
            [('url', ASCENDING)], unique=True)

        Database.notVisited.insert_many(
            list(map(lambda url: {'url': url, 'depth': 0, 'baseDomain': IsBaseDomain(url)}, urlList)))

    @staticmethod
    def InsertNotVisitedWebpages(urls, depth):
        '''Adds urls to notVisited collection'''
        urls = Database.FilterLinks(urls)

        documents = []
        for url in urls:
            documents.append({'url': url, 'depth': depth,
                              'baseDomain': IsBaseDomain(url)})

        if (len(documents) > 0):
            Database.notVisited.insert_many(documents)

    @staticmethod
    def FilterLinks(links):
        '''Removes links which are already included in notVisited, visited or error collections'''

        links = list(set(links) - set(map(lambda element: element['url'], list(Database.notVisited.find(
            {'url': {'$in': links}})))))

        links = list(set(links) - set(map(lambda element: element['url'], list(
            Database.error.find({'url': {'$in': links}})))))

        links = list(set(links) - set(map(lambda element: element['url'], list(
            Database.visited.find({'url': {'$in': links}})))))

        return links
