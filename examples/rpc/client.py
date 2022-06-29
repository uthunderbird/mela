import asyncio

from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


fetcher = app.rpc_client("fetcher")

bot_manager = app.rpc_client("bot_manager")


async def main():
    # RPC calls over RabbitMQ never were simpler!
    res = await fetcher.call({'asdf': 5, 'lol': [3, 8, ["haha", "wow"]]})
    print(res)

    # we can even gather call results!
    g = await asyncio.gather(fetcher.call(url1), fetcher.call(url2))
    print(g)

    create_bot_result = await bot_manager.call(
        {'bot_id': 1, 'bot_username': "LalkaPalka", 'bot_password': "supersecret"},
        headers={'method': 'create_bot'}
    )
    print(f"create_bot result: {create_bot_result}")

    get_bot_result = await bot_manager.call({'bot_id': 1}, headers={'method': 'get_bot'})
    print(f"get_bot_result: {get_bot_result}")

    unknown_method_result = await bot_manager.call({'bot_id': 4}, headers={'method': 'getBot'})
    print(f"unknown method result: {unknown_method_result}")


if __name__ == '__main__':
    url1 = 'https://tengrinews.kz/kazakhstan_news/vorvalis-dom-izbili-almatinka-rasskazala-zaderjanii-supruga-459127/'
    url2 = 'https://www.inform.kz/ru/skol-ko-lichnyh-podsobnyh-hozyaystv-naschityvaetsya-v-kazahstane_a3896073'
    app.run(main())
