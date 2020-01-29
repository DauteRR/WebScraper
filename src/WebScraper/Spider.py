from DB import Database
from pymongo.bulk import BulkWriteError
import math
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
import socket
from datetime import datetime
from Utils import GetWebpageInformation, IsBaseDomain
from threading import Thread


class Spider(Thread):

    targetMetaTags = [
        'keywords',
        'description',
        'author',
        'lang',
        'locality',
        'organization',
    ]

    def __init__(self, spiderID, batchSize, timeout, maxDepth):
        '''Spider constructor'''
        print(f'Creating Spider {spiderID}')
        Thread.__init__(self)
        self.id = spiderID
        self.batchSize = batchSize
        self.timeout = timeout
        self.maxDepth = maxDepth

        self.visited = []
        self.error = []
        self.toVisit = []

    def PrintMessage(self, message):
        print('Spider', self.id + ' -', message)

    def StoreBatchResults(self, batchStartTime, batchNumber):
        self.PrintMessage('Batch ' + str(batchNumber) + ' took ' +
                          str((datetime.now() - batchStartTime).total_seconds()) + ' seconds to complete')

        if len(self.visited) > 0:
            Database.visited.insert_many(self.visited)

        if len(self.error) > 0:
            Database.error.insert_many(self.error)

        visitedUrls = list(set(map(lambda element: element['url'], self.visited)).union(
            set(map(lambda element: element['url'], self.error))))
        Database.notVisited.delete_many(
            {'url': {'$in': visitedUrls}})

    def run(self):
        '''Iterates over a list of urls retrieving web pages information'''
        self.PrintMessage('Amount of webpages: ' + str(len(self.toVisit)))
        self.PrintMessage('Timeout: ' + str(self.timeout) + ' s')
        self.PrintMessage('Max depth: ' + str(self.maxDepth))
        self.PrintMessage('Batch size: ' + str(self.batchSize))
        self.PrintMessage(
            'Batches: ' + str(math.ceil(len(self.toVisit) / self.batchSize)))

        startTime = datetime.now()
        batchStartTime = datetime.now()
        batchVisitedWebpages = 0
        batchNumber = 1

        for i in range(0, len(self.toVisit)):
            url = self.toVisit[i]['url']
            depth = self.toVisit[i]['depth']

            try:
                headers = {
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'
                }
                request = Request(url, headers=headers)
                response = urlopen(request, timeout=self.timeout)
                contentType = response.headers.get_content_type()
                charset = response.headers.get_content_charset()

                if contentType == 'text/html':
                    html = response.read().decode(charset if charset else 'utf8')

                    getLinks = depth + 1 <= self.maxDepth
                    pageInformation = GetWebpageInformation(
                        html, getLinks, Spider.targetMetaTags)
                    if getLinks:
                        Database.InsertNotVisitedWebpages(
                            pageInformation['links'], depth)

                    del pageInformation['links']
                    pageInformation['url'] = url
                    pageInformation['baseDomain'] = IsBaseDomain(url)
                    self.visited.append(pageInformation)
                else:
                    self.visited.append({'url': url})

            except HTTPError as error:
                self.error.append({'url': url, 'errorType': 'HTTPError',
                                   'message': error.code, 'lastVisited': datetime.now(), 'baseDomain': IsBaseDomain(url)})
            except URLError as error:
                self.error.append({'url': url, 'errorType': 'URLError',
                                   'message': str(error.reason), 'lastVisited': datetime.now(), 'baseDomain': IsBaseDomain(url)})
            except socket.timeout as error:
                pass
            except UnicodeDecodeError as error:
                self.error.append({'url': url, 'errorType': 'UnicodeDecodeError',
                                   'message': error.reason, 'lastVisited': datetime.now(), 'baseDomain': IsBaseDomain(url)})
            except UnicodeEncodeError as error:
                self.error.append({'url': url, 'errorType': 'UnicodeEncodeError',
                                   'message': error.reason, 'lastVisited': datetime.now(), 'baseDomain': IsBaseDomain(url)})

            batchVisitedWebpages += 1
            if batchVisitedWebpages == self.batchSize or i == len(self.toVisit) - 1:
                self.StoreBatchResults(batchStartTime, batchNumber)
                self.visited = []
                self.error = []
                batchVisitedWebpages = 0
                batchStartTime = datetime.now()
                batchNumber += 1

        self.PrintMessage('Total elapsed seconds: ' +
                          str((datetime.now() - startTime).total_seconds()))
