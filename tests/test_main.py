import pytest

from mela.abc import AbstractComponent
from mela.component import LoggableComponent
from mela.component import MelaBiDirectConnection
from mela.connection import HostConnectionConfiguration
from mela.component import MelaConnectionWrapper


class SomeComponent(AbstractComponent):
    def setup(self):
        super(SomeComponent, self).setup()


class SomeLoggableComponent(LoggableComponent):
    def setup(self):
        super(SomeLoggableComponent, self).setup()

    def shutdown(self):
        super(SomeLoggableComponent, self).shutdown()


def test_component_should_not_start_before_setup_completed():
    sc = SomeComponent("some_name")
    with pytest.raises(AssertionError):
        sc.run()

    sc.setup()
    sc.run()
    sc.shutdown()

    with pytest.raises(AssertionError):
        sc.run()


def test_component_should_not_be_stopped_before_setup_completed():
    sc = SomeComponent("some_name")
    with pytest.raises(AssertionError):
        sc.shutdown()


def test_component_should_know_own_parent():
    scp = SomeComponent("parent")
    scc = SomeComponent("child", scp)
    assert scc.parent is scp
    assert scp.parent is None
    assert scc.name == "parent.child"


def test_loggable_component():
    slc = SomeLoggableComponent("root")
    slc.setup()
    slc.run()
    slc.shutdown()


def test_component_name():
    sc = SomeComponent("root")
    scc = SomeComponent("subroot", parent=sc)
    sccc = SomeComponent("target", parent=scc)
    assert sccc.name == 'root.subroot.target'


def test_connection(mocker):
    mocker.patch('mela.component.MelaConnection._instantiate', return_value=1)
    sc = SomeComponent("root")
    config = HostConnectionConfiguration(host="localhost", username="", password="", log_level="DEBUG")
    bidi_connection = MelaBiDirectConnection('local', parent=sc, config=config)
    bidi_connection.setup()
    bidi_connection.read
    MelaConnectionWrapper._instantiate.assert_called_once()
    bidi_connection.write
    assert MelaConnectionWrapper._instantiate.call_count == 2
    bidi_connection.run()
    bidi_connection.shutdown()


def test_service():
    pass
