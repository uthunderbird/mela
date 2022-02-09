from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


@app.rpc_server("fetcher")
async def fetcher(body, message):
    print(body)
    return body


if __name__ == '__main__':
    app.run()
