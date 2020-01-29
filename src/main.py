import click
import math
from Spider import Spider, Database
from Utils import GetBaseDomain
from colorama import Fore, Style


colors = [Fore.RED, Fore.GREEN, Fore.YELLOW,
          Fore.BLUE, Fore.MAGENTA, Fore.CYAN, Fore.WHITE]

styles = [Style.DIM, Style.NORMAL, Style.BRIGHT]


@click.command()
@click.option('-b', '--batchSize', 'batchSize', help='Size of batches', type=click.INT, default=25)
@click.option('-t', '--threads', 'threads', help='Amount of threads', type=click.INT, default=8)
@click.option('-s', '--seconds', 'timeout', help='Maximum timeout in seconds', type=click.FLOAT, default=1)
@click.option('-d', '--depth', 'maxDepth', help='Maximum depth', type=click.INT, default=1)
@click.option('-l', '--threadLimit', 'threadLimit', help='Maximum amount of webpages assigned to a thread', type=click.INT, default=600)
@click.option('-w', '--webpagesLimit', 'webpagesLimit', help='Total webpages limit', type=click.INT, default=4800)
@click.option('-r', '--resetDB', 'resetDB', is_flag=True, help='Tells whether to reset the database or not', type=click.BOOL, default=False)
@click.option('-m', '--mode', 'mode', help='Execution mode', type=click.Choice(['internal', 'newDomains']), default='internal')
def main(batchSize=25, threads=8, timeout=1, maxDepth=1, threadLimit=600, webpagesLimit=4800, resetDB=False, mode='internal'):

    colorsCombinations = [(color, style)
                          for style in styles for color in colors]

    db = Database()

    if (resetDB):
        db.Reset()
        return 0

    toVisit = []

    if mode == 'newDomains':
        toVisit = list(db.notVisited.find(
            {'baseDomain': True}).limit(webpagesLimit))
    else:
        knownDomains = set(map(lambda element: element['url'], list(
            db.visited.find({'baseDomain': True}))))

        toVisit = list(filter(lambda element:
                              GetBaseDomain(element['url']) in knownDomains, list(db.notVisited.find({'baseDomain': False}))))[0:webpagesLimit]

    print('Webpages to visit:', len(toVisit))
    print('Threads:', threads)

    threads = len(toVisit) if len(toVisit) < threads else threads

    if threads == 0:
        print('Nothing to do...')
        return 0

    webpagesPerSpider = int(math.floor(len(toVisit) / threads))

    webpagesPerSpider = threadLimit if webpagesPerSpider > threadLimit else webpagesPerSpider

    chunks = [toVisit[i: i + webpagesPerSpider]
              for i in range(0, len(toVisit), webpagesPerSpider)]

    # print(list(map(lambda foo: len(foo), chunks)))
    # print(len(chunks))
    for i in range(threads):
        spiderColors = colorsCombinations[i % len(colorsCombinations)]
        spider = Spider(str(i), batchSize, timeout, maxDepth,
                        spiderColors[0], spiderColors[1])
        spider.toVisit = chunks[i]
        # print(i)
        spider.start()


if __name__ == '__main__':
    main()
