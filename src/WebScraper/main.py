import click
from Spider import Spider, Database


@click.command()
@click.option('-b', '--batchSize', 'batchSize', help='Size of batches', type=click.INT, default=25)
@click.option('-t', '--threads', 'threads', help='Amount of threads', type=click.INT, default=8)
@click.option('-s', '--seconds', 'timeout', help='Maximum timeout in seconds', type=click.FLOAT, default=1)
@click.option('-d', '--depth', 'maxDepth', help='Maximum depth', type=click.INT, default=1)
@click.option('-l', '--threadLimit', 'threadLimit', help='Maximum amount of webpages assigned to a thread', type=click.INT, default=600)
@click.option('-w', '--webpagesLimit', 'webpagesLimit', help='Total webpages limit', type=click.INT, default=4800)
@click.option('-r', '--resetDB', 'resetDB', is_flag=True, help='Tells whether to reset the database or not', type=click.BOOL, default=False)
def main(batchSize=25, threads=8, timeout=1, maxDepth=1, threadLimit=600, webpagesLimit=4800, resetDB=False):

    db = Database()

    if (resetDB):
        db.Reset()

    myFirstSpider = Spider('1', batchSize, timeout, maxDepth)

    # myFirstSpider.VisitWebpages(list(db.notVisited.find({}).limit(100)))


if __name__ == '__main__':
    main()
