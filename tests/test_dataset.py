from pytest import raises


from dame import Dataset


class PlusOne:
    requires = ("number", )
    provides = ("p1", )

    def apply(self, *, number):
        return {"p1": number + 1}


class PlusTwo:
    requires = ("p1", )
    provides = ("p2", )

    def apply(self, *, p1):
        return {"p2": p1 + 1}


class Source:

    keyword = "number"

    def __getitem__(self, idx):
        return idx

    def __len__(self):
        return 3

    def __iter__(self):
        return iter(range(3))


class IterLessSource:
    keyword = "number"

    def __getitem__(self, idx):
        if idx >= 3:
            raise IndexError
        return idx


class StandardDataset(Dataset):
    source_cls = Source
    transforms = (PlusOne, PlusTwo)


class IterLessDataset(Dataset):
    source_cls = IterLessSource
    transforms = (PlusOne, PlusTwo)


def test_gets_item():
    data = StandardDataset()
    assert data[0] == {"number": 0, "p1": 1, "p2": 2}
    assert data[103] == {"number": 103, "p1": 104, "p2": 105}


def test_len():
    assert len(StandardDataset()) == 3


full = [{"number": x, "p1": x + 1, "p2": x + 2} for x in range(3)]


def test_iter():
    for data in [StandardDataset(), IterLessDataset()]:
        assert list(data) == full


def test_assequence():
    for data in [StandardDataset(), IterLessDataset()]:
        assert list(data.assequence()) == full
        assert list(data.assequence(of="p1")) == [{"p1": d["p1"]} for d in full]
        assert list(data.assequence(of=set(["p1", "p2"]))) == [
            {"p1": d["p1"], "p2": d["p2"]} for d in full
        ]


def test_safety_no_source():
    with raises(AssertionError):

        class NoSource(Dataset):
            pass


def test_safety_overlapping_keywords():
    with raises(AssertionError):

        class RepeatT(Dataset):
            transforms = (PlusOne, PlusOne)
