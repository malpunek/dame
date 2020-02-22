from itertools import chain
from collections import Counter

from .worker import WorkManager


def _get_all_keywords(obj):
    """Gets all the keywords declared by a dataset.

    Args:
        obj (Dataset or cls): The dataset object or a subclass of a Dataset

    Returns:
        list(string): all keywords declared by obj.
    """
    sources = [obj.source]
    keywords = [
        chain.from_iterable([t.provides for t in chain(obj.transforms, sources)])
    ]
    return keywords


class Dataset:
    r"""The most important class in DAME.

    Manages access, computation of dataset elements

    What you probably want to do is to subclass this class and redefine it's source
    and transforms attributes.

    Attributes:
        source (Source_cls): As lightweight as possible data source
        transforms (Iterable[Transform_cls]): Processing of data items at element level

    """
    source = None
    transforms = tuple()
    context = {}

    def __init_subclass__(cls, **kwargs):
        r"""Validates (roughly) the subclass's transforms and source

        This method is called each time a subclass of dataset is created.
        It ensures that the transforms have 'provides' attribute and
        that no provided keywords overlap.

        Args:
            cls (Dataset subclass): The new subclass of Dataset
        """
        # TODO: Not sure if that's a good place to check.
        # Maybe all validation should be done in __init__.
        # The advantage of current approach is that you get the error sooner.
        # TODO: this is stupid - somebody might want to provide some more generic
        # functionality to Dataset
        assert (
            getattr(cls, "source", None) is not None
        ), "Dataset can't exist without a source. "
        repeated_keywords = [
            kw for kw, num in Counter(_get_all_keywords(cls)).most_common() if num > 1
        ]
        assert len(repeated_keywords) == 0, (
            "Transforms' provided keywords must be unique. "
            f"Violated by {repeated_keywords}"
        )
        super().__init_subclass__(**kwargs)

    @property
    def manager(self):
        if not hasattr(self, "_manager"):
            self._manager = WorkManager(self.source, self.transforms, self.context)
        return self._manager

    def __getitem__(self, idx):
        r"""Computes a single dataset element"""
        return self.manager.compute_one(idx)

    def __len__(self):
        return len(self.manager.source_instance)

    def __iter__(self):
        r"""Returns an iterator over the dataset with all transforms applied"""
        return self.manager.fast_compute()

    def assequence(self, of=None):
        r"""Returns a sequence of `{key: computed_value for key in of}`

        Args:
            of (str|set(str), optional): the keywords to keep in each element.
                Defaults to None.
        """
        if of is None:
            return iter(self)
        return map(
            lambda data: {key: value for key, value in data.items() if key in of}, self
        )
