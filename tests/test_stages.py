from dame.stages import Stages

from .test_classes import PlusOne, PlusTwo, ThreeNums


def test_dag():
    stages = Stages(ThreeNums, (PlusTwo, PlusOne))
    assert list(map(lambda t: t.__class__, iter(stages))) == [
        ThreeNums,
        PlusOne,
        PlusTwo,
    ]
    assert list(map(lambda t: t.__class__, stages.to("p1"))) == [ThreeNums, PlusOne]
