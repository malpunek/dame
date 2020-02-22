from dame.worker import SequentialWorker

from .test_classes import ThreeNums, PlusOne, PlusTwo, PlusXN


class TestStages:
    def to(self, keyword):
        if keyword == "p1":
            return iter([PlusOne])
        if keyword == "pxn":
            return iter([PlusXN])
        return iter([PlusOne, PlusTwo])

    def __iter__(self):
        yield PlusOne
        yield PlusXN
        yield PlusTwo


def test_computations():
    stages = TestStages()
    source = ThreeNums()
    context = {PlusXN.__name__: {"args": [10], "kwargs": {"n": 3}}}
    worker = SequentialWorker(stages, context)

    data_1 = {**source[0], "p1": 1}
    data_2 = {**data_1, "p2": 2}
    data_full = {**data_2, "pxn": 10 ** 3}

    assert worker.compute_to(source[0], "p1") == data_1
    assert worker.compute_to(source[0], "p2") == data_2

    assert worker.compute_full(source[0]) == data_full

    assert worker.compute_stage(dict(data_1), PlusTwo()) == data_2
    assert worker.compute_stage(source[0], PlusOne()) == data_1
