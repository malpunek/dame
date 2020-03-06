from pytest import raises

from dame import Dataset

from .test_classes import ThreeNums, PlusOne, PlusTwo, PlusXN


class StandardDataset(Dataset):
    source = ThreeNums()
    transforms = (PlusOne, PlusTwo)


def expected(num):
    return {"number": num, "p1": num + 1, "p2": num + 2, "idx": num}


def test_gets_item():
    data = StandardDataset()
    assert data[0] == expected(0)
    assert data[2] == expected(2)


def test_len():
    assert len(StandardDataset()) == 3


full = [expected(x) for x in range(3)]


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


class OtherDataset(StandardDataset):
    transforms = (PlusOne, PlusXN)


def test_right_args():
    data = OtherDataset()
    data.set_arguments_for(PlusXN, 2, n=10)
    assert data[0] == {"number": 0, "p1": 1, "pxn": 1024, "idx": 0}
    data.set_arguments_for(PlusXN, 2, n=9)
    assert data[0] == {"number": 0, "p1": 1, "pxn": 512, "idx": 0}
