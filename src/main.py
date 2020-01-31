import click
import math
from Spider import Spider, Database
from Utils import GetBaseDomain
from colorama import Fore, Style
import concurrent.futures


colors = [Fore.RED, Fore.GREEN, Fore.YELLOW,
          Fore.BLUE, Fore.MAGENTA, Fore.CYAN, Fore.WHITE]

styles = [Style.DIM, Style.NORMAL, Style.BRIGHT]


@click.command()
@click.option('-b', '--batchSize', 'batchSize', help='Size of batches', type=click.INT, default=25)
@click.option('-n', '--numberOfThreads', 'threads', help='Amount of threads', type=click.INT, default=8)
@click.option('-t', '--timeout', 'timeout', help='Maximum timeout in seconds', type=click.FLOAT, default=3.0)
@click.option('-d', '--depth', 'maxDepth', help='Maximum depth', type=click.INT, default=1)
@click.option('-l', '--limit', 'limitPerSpider', help='Maximum amount of webpages assigned to each Spider', type=click.INT, default=200)
@click.option('-w', '--webpagesLimit', 'webpagesLimit', help='Initial webpages limit', type=click.INT, default=1016)
@click.option('-i', '--initialize', 'initialize', is_flag=True, help='Initialize database', type=click.BOOL, default=False)
@click.option('-r', '--recursive', 'recursive', help='Recursive mode', is_flag=True, type=click.BOOL, default=False)
@click.option('-a', '--allLinks', 'allLinks', help='Save each encountered link', is_flag=True, type=click.BOOL, default=False)
@click.option('-m', '--mode', 'mode', help='Execution mode', type=click.Choice(['normal', 'explore', 'in-depth']), default='normal')
def main(batchSize=25, threads=8, timeout=3.0, maxDepth=1, limitPerSpider=200, webpagesLimit=1016, initialize=False, recursive=False, allLinks=False, mode='normal'):

    colorsCombinations = [(color, style)
                          for style in styles for color in colors]

    db = Database()

    if (initialize):
        db.Initialize()
        return 0

    toVisit = []

    if mode == 'explore':
        recursive = False
        allLinks = True
        toVisit = list(db.notVisited.find(
            {'baseDomain': True}, {'url': 1, 'depth': 1}))
    elif mode == 'in-depth':
        recursive = True
        allLinks = False
        toVisit = list(db.notVisited.find(
            {'baseDomain': False}, {'url': 1, 'depth': 1}))
        knownDomains = set(db.GetKnownDomains())
        toVisit = list(filter(lambda element: GetBaseDomain(
            element['url']) in knownDomains, toVisit))
    else:
        toVisit = list(db.notVisited.find(
            {}, {'url': 1, 'depth': 1}).limit(webpagesLimit))

    threads = len(toVisit) if len(
        toVisit) < threads else threads

    print(f'{Fore.BLUE}Webpages to visit: {len(toVisit)}{Style.RESET_ALL}')
    print(f'{Fore.BLUE}Threads: {threads}{Style.RESET_ALL}')

    if threads == 0:
        print(f'{Fore.BLUE}Nothing to do...{Style.RESET_ALL}')
        return 0

    webpagesPerSpider = int(math.floor(len(toVisit) / threads))

    webpagesPerSpider = limitPerSpider if webpagesPerSpider > limitPerSpider else webpagesPerSpider

    chunks = [toVisit[i: i + webpagesPerSpider]
              for i in range(0, len(toVisit), webpagesPerSpider)]

    Spider.allLinks = allLinks
    Spider.batchSize = batchSize
    Spider.limit = limitPerSpider
    Spider.maxDepth = maxDepth
    Spider.recursive = recursive
    Spider.timeout = timeout

    print(f'{Fore.BLUE}Spiders: {len(chunks)}{Style.RESET_ALL}')

    spiders = []
    for i in range(len(chunks)):
        spiderColors = colorsCombinations[i % len(colorsCombinations)]
        spider = Spider(str(i), spiderColors[0], spiderColors[1])
        spider.toVisit = chunks[i]
        spiders.append(spider)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:

        futures = [executor.submit(spider.Search) for spider in spiders]

        for future in futures:
            future.result()


if __name__ == '__main__':
    main()
