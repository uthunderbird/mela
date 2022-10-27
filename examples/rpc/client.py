import asyncio

from mela import Mela

app = Mela(__name__)


fetcher = app.rpc_client("fetcher")

bot_manager = app.rpc_client("bot_manager")


async def main():
    # RPC calls over RabbitMQ never were simpler!
    res = await fetcher.call({'asdf': 5, 'lol': [3, 8, ["haha", "wow"]]})
    # res

    # we can even gather call results!
    g = await asyncio.gather(fetcher.call(url1), fetcher.call(url2))
    # g

    create_bot_result = await bot_manager.call({
        'bot_id': 1,
        'bot_username': "LalkaPalka",
        'bot_password': "supersecret",
    },
        headers={'method': 'create_bot'},
    )
    # create_bot result {create_bot_result}

    get_bot_result = await bot_manager.call({'bot_id': 1}, headers={'method': 'get_bot'})
    # get_bot_result {get_bot_result}

    unknown_method_result = await bot_manager.call({'bot_id': 4}, headers={'method': 'getBot'})
    # unknown method result: {unknown_method_result}


if __name__ == '__main__':
    url1 = (
        'https://tengrinews.kz/kazakhstan_news/vorvalis-dom-izbili-'
        'almatinka-rasskazala-zaderjanii-supruga-459127/'
    )
    url2 = (
        'https://www.inform.kz/ru/skol-ko-lichnyh-podsobnyh-'
        'hozyaystv-naschityvaetsya-v-kazahstane_a3896073'
    )
    app.run(main())
