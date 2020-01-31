from pymongo import collection, MongoClient, ASCENDING, TEXT
from urllib.request import urlopen
from Utils import IsBaseDomain, GetBaseDomain
import os


class Database:

    client = MongoClient(port=os.getenv('MONGO_PORT', 27017))

    notVisited = client.websDB['notVisited']
    visited = client.websDB['visited']
    error = client.websDB['error']

    @staticmethod
    def Initialize():
        '''Initializes the database'''
        print('Initializing database ...')
        urlList = list(set(urlopen(
            'http://fmartinz.webs.ull.es/adquisicion/digitales.txt').read().decode('utf8').split('\n')))

        Database.visited.remove({})
        Database.visited.create_index(
            [('url', ASCENDING)], unique=True)
        Database.visited.create_index(
            [('title', TEXT), ('meta.keywords', TEXT), ('headers.h1', TEXT),
             ('headers.h2', TEXT), ('content', TEXT)],
            name='SearchIndex',
            default_language='spanish',
            weights={'title': 10, 'meta.keywords': 5, 'headers.h1': 3, 'headers.h2': 2, 'content': 1})

        Database.error.remove({})
        Database.error.create_index(
            [('url', ASCENDING)], unique=True)

        Database.notVisited.remove({})
        Database.notVisited.create_index(
            [('url', ASCENDING)], unique=True)

        Database.notVisited.insert_many(
            list(map(lambda url: {'url': url, 'depth': 0, 'baseDomain': IsBaseDomain(url)}, urlList)))

    @staticmethod
    def PrepareLinksList(urls, depth):
        '''Prepares a list of links to be inserted'''
        urls = Database.FilterLinks(urls)

        documents = []
        for url in urls:
            documents.append({'url': url, 'depth': depth,
                              'baseDomain': IsBaseDomain(url)})
        return documents

    @staticmethod
    def InsertNotVisitedWebpages(documents):
        '''Adds urls to notVisited collection'''
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

    @staticmethod
    def GetKnownDomains():
        '''Returns a link of known domains'''

        return list(map(lambda document: GetBaseDomain(document), list(Database.visited.find({}, {'url': 1}).distinct('url'))))

    @staticmethod
    def GetKnownDomainsLinks(links):
        '''Returns links of known domains'''

        knownDomains = set(Database.GetKnownDomains())

        a = list(filter(lambda link: GetBaseDomain(link) in knownDomains, links))

        print(len(a))

        return a

    @staticmethod
    def GetNewDomainsLinks(links):
        '''Returns links of unknown domains'''

        return list(set(links) - set(Database.GetKnownDomainsLinks(links)))
