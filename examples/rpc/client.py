from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


fetcher = app.rpc_client("fetcher")


async def main():
    content1 = await fetcher.call(url1)
    print(content1)
    content2 = await fetcher.call(url2)
    print(content2)


if __name__ == '__main__':
    url1 = 'https://tengrinews.kz/kazakhstan_news/vorvalis-dom-izbili-almatinka-rasskazala-zaderjanii-supruga-459127/'
    url2 = 'https://www.inform.kz/ru/skol-ko-lichnyh-podsobnyh-hozyaystv-naschityvaetsya-v-kazahstane_a3896073'
    app.run(main())
