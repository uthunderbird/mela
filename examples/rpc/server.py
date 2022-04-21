import asyncio

from mela import Mela

app = Mela(__name__)
app.configure_from_yaml('application.yml')


async def fetch(url):
    # let's pretend we're asynchronously fetching url here and return its body
    await asyncio.sleep(1)
    return url


@app.rpc_server("fetcher")
async def fetcher(link, message):
    return await fetch(link)


if __name__ == '__main__':
    app.run()
