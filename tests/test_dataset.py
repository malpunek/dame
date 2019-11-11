from dame import Dataset


class PlusOne:
    required = "number"

    def apply(self, *, number):
        return {"p1": number + 1}


class PlusTwo:
    required = "p1"

    def apply(self, *, p1):
        return {"p2": p1 + 1}


class Source:
    keyword = "number"

    def __getitem__(self, idx):
        return idx


class MyDataset(Dataset):
    source_cls = Source
    transforms = (PlusOne, PlusTwo)


class TestDataset:
    def test_gets_item(self):
        data = MyDataset()
        assert data[0] == {'number': 0, 'p1': 1, 'p2': 2}
        assert data[0]["p1"] == 1
        assert data[0]["p2"] == 2
        assert data[103]["p2"] == 105
        assert data[103]["number"] == 103
