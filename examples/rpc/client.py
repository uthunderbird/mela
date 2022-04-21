import asyncio

from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


fetcher = app.rpc_client("fetcher")


async def main():
    # content1 = await
    # print(content1)
    # content2 = await
    # print(content2)

    g = await asyncio.gather(fetcher.call(url1), fetcher.call(url2))
    print(g)


if __name__ == '__main__':
    url1 = 'https://tengrinews.kz/kazakhstan_news/vorvalis-dom-izbili-almatinka-rasskazala-zaderjanii-supruga-459127/'
    url2 = 'https://www.inform.kz/ru/skol-ko-lichnyh-podsobnyh-hozyaystv-naschityvaetsya-v-kazahstane_a3896073'
    app.run(main())