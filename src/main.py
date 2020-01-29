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
@click.option('-t', '--threads', 'threads', help='Amount of threads', type=click.INT, default=8)
@click.option('-s', '--seconds', 'timeout', help='Maximum timeout in seconds', type=click.FLOAT, default=3.0)
@click.option('-d', '--depth', 'maxDepth', help='Maximum depth', type=click.INT, default=1)
@click.option('-l', '--limit', 'limit', help='Maximum amount of webpages assigned to a Spider', type=click.INT, default=600)
@click.option('-w', '--webpagesLimit', 'webpagesLimit', help='Total webpages limit', type=click.INT, default=4800)
@click.option('-r', '--resetDB', 'resetDB', is_flag=True, help='Tells whether to reset the database or not', type=click.BOOL, default=False)
@click.option('-m', '--mode', 'mode', help='Execution mode', type=click.Choice(['internal', 'newDomains']), default='internal')
def main(batchSize=25, threads=8, timeout=3.0, maxDepth=1, limit=600, webpagesLimit=4800, resetDB=False, mode='internal'):

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

    threads = len(toVisit) if len(toVisit) < threads else threads

    if threads == 0:
        print(f'{Fore.BLUE}Nothing to do...{Style.RESET_ALL}')
        return 0

    webpagesPerSpider = int(math.floor(len(toVisit) / threads))

    webpagesPerSpider = limit if webpagesPerSpider > limit else webpagesPerSpider

    chunks = [toVisit[i: i + webpagesPerSpider]
              for i in range(0, len(toVisit), webpagesPerSpider)]

    spiders = []
    for i in range(len(chunks)):
        spiderColors = colorsCombinations[i % len(colorsCombinations)]
        spider = Spider(str(i), batchSize, timeout, maxDepth,
                        spiderColors[0], spiderColors[1])
        spider.toVisit = chunks[i]
        spiders.append(spider)

    print(f'{Fore.BLUE}Webpages to visit: {len(toVisit)}{Style.RESET_ALL}')
    print(f'{Fore.BLUE}Threads: {threads}{Style.RESET_ALL}')
    print(f'{Fore.BLUE}Spiders: {len(spiders)}{Style.RESET_ALL}')

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:

        futures = [executor.submit(spider.Search) for spider in spiders]

        for future in futures:
            future.result()


if __name__ == '__main__':
    main()
