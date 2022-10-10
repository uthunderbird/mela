import pytest

from mela.abc import AbstractComponent
from mela.component import LoggableComponent


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
