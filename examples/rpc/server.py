import asyncio

from mela import Mela

app = Mela(__name__)
app.configure_from_yaml('application.yml')

i = 0


@app.rpc_server("fetcher")
async def fetcher(body, message):
    global i
    i+=1
    await asyncio.sleep(5-i)
    print(body)
    return body


if __name__ == '__main__':
    app.run()
