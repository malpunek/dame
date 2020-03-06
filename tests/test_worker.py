from dame.worker import SequentialWorker, WorkManager
from dame.stages import Stages
from dame.source import SourceWrap
from .test_classes import ThreeNums, PlusOne, PlusTwo, PlusXN


class MockStages(Stages):
    def __init__(self):
        pass

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


class MockStorage:
    def __init__(self, *args, **kwargs):
        pass

    def open(self):
        self.data = {}
        print("Opening")

    def save(self, idx, transform, data):
        print(f"Saving {idx}.{transform.__class__.__name__}")
        if idx not in self.data:
            self.data[idx] = {}
        self.data[idx][transform.__class__.__name__] = data

    def load(self, idx, transform):
        print(f"Loading {idx}.{transform.__class__.__name__}")
        if idx == 0:
            if transform.__class__ == PlusOne:
                print("Returning")
                return {"p1": 1}
            elif transform.__class__ == PlusTwo:
                print("Returning")
                return {"p2": 2}
        return self.data.get(idx, {}).get(transform.__class__.__name__, None)

    def close(self):
        print(f"Finalizing")
        delattr(self, "data")


def test_computations():
    stages = MockStages()
    source = SourceWrap(ThreeNums())
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


def test_uses_store(capfd):
    source = ThreeNums()
    context = {PlusXN.__name__: {"args": [10]}}
    transforms = (PlusOne, PlusXN, PlusTwo)
    manager = WorkManager(source, transforms, context, n_processes=1, store=MockStorage)

    for i in manager.fast_compute():
        pass

    out = capfd.readouterr()
    expected_out = "Opening\nLoading 0.PlusXN\nSaving 0.PlusXN\nLoading 0.PlusOne\nReturning\nLoading 0.PlusTwo\nReturning\nLoading 1.PlusXN\nSaving 1.PlusXN\nLoading 1.PlusOne\nSaving 1.PlusOne\nLoading 1.PlusTwo\nSaving 1.PlusTwo\nLoading 2.PlusXN\nSaving 2.PlusXN\nLoading 2.PlusOne\nSaving 2.PlusOne\nLoading 2.PlusTwo\nSaving 2.PlusTwo\nFinalizing\n"
    assert out.out == expected_out
