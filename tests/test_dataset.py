from pytest import raises

from dame import Dataset

from .test_classes import StandardDataset, PlusOne


def test_gets_item():
    data = StandardDataset()
    assert data[0] == {"number": 0, "p1": 1, "p2": 2}
    assert data[103] == {"number": 103, "p1": 104, "p2": 105}


def test_len():
    assert len(StandardDataset()) == 3


full = [{"number": x, "p1": x + 1, "p2": x + 2} for x in range(3)]


def test_iter():
    assert list(StandardDataset()) == full


def test_assequence():
    data = StandardDataset()
    assert list(data.assequence()) == full
    assert list(data.assequence(of="p1")) == [{"p1": d["p1"]} for d in full]
    assert list(data.assequence(of=set(["p1", "p2"]))) == [
        {"p1": d["p1"], "p2": d["p2"]} for d in full
    ]


def test_safety_no_source():
    with raises(AssertionError):

        class NoSource(Dataset):
            pass

        for i in NoSource():
            pass


def test_safety_overlapping_keywords():
    with raises(AssertionError):

        class RepeatT(Dataset):
            transforms = (PlusOne, PlusOne)

        for i in RepeatT():
            pass
