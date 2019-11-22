from dame.stages import Stages

from .test_classes import PlusOne, PlusTwo, Source


def test_dag():
    stages = Stages(Source, (PlusTwo, PlusOne))
    assert stages.topsorted == [Source, PlusOne, PlusTwo]
    assert list(map(lambda t: t.__class__, iter(stages))) == stages.topsorted
    assert list(map(lambda t: t.__class__, stages.to("p1"))) == [Source, PlusOne]
