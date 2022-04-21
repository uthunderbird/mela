from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')

i = 0


@app.service("printer")
async def printer(body, message):
    global i
    i += 1
    if i % 2 == 0:
        raise NotImplementedError("Method is not implemented")
    return i
    # for obj in body:
    #     if i % 2 == 0:
    #         yield obj
    #     else:
    #         yield obj, {'routing_key': "test_queue2"}
    #         raise ConnectionError("something went wrong")
    #     i += 1


if __name__ == '__main__':
    app.run()
