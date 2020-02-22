from dame.stages import Stages

from .test_classes import PlusOne, PlusTwo, ThreeNums


def test_dag():
    stages = Stages(ThreeNums, (PlusTwo, PlusOne))
    assert list(iter(stages)) == [
        PlusOne,
        PlusTwo,
    ]
    assert list(stages.to("p1")) == [PlusOne]
