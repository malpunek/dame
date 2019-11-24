from dame import Dataset
from dame.source import Source


class PlusOne:
    requires = ("number",)
    provides = ("p1",)

    def apply(self, *, number):
        return {"p1": number + 1}


class PlusTwo:
    requires = ("p1",)
    provides = ("p2",)

    def apply(self, *, p1):
        return {"p2": p1 + 1}


class ThreeNums(Source):

    provides = ("number",)

    def __getitem__(self, idx):
        return {"number": idx}

    def __len__(self):
        return 3


class StandardDataset(Dataset):
    sources = ThreeNums
    transforms = (PlusOne, PlusTwo)
