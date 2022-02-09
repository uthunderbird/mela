from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


@app.service("splitter")
def printer(body, message):
    i = 0
    for obj in body:
        if i % 2 == 0:
            yield obj
        else:
            yield obj, {'routing_key': "test_queue2"}
            raise ConnectionError("something went wrong")
        i += 1


if __name__ == '__main__':
    app.run()
