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
