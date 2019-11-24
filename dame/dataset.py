from itertools import chain
from collections import Counter


def _get_all_keywords(obj):
    """Gets all the keywords declared by a dataset.

    Args:
        obj (Dataset or cls): The dataset object or a subclass of a Dataset

    Returns:
        list(string): all keywords declared by obj.
    """
    sources = obj.sources
    if not isinstance(sources, (list, tuple)):
        sources = [sources]
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
        sources (Source_cls): As lightweight as possible data source
        transforms (Iterable[Transform_cls]): Processing of data items at element level

    """
    sources = None
    transforms = tuple()

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
        assert getattr(cls, "sources", None) is not None, (
            "Dataset can't exist without a source. "
            "If you know what you're doing try UnsafeDataset"
        )
        repeated_keywords = [
            kw for kw, num in Counter(_get_all_keywords(cls)).most_common() if num > 1
        ]
        assert len(repeated_keywords) == 0, (
            "Transforms' provided keywords must be unique. "
            f"Violated by {repeated_keywords}"
        )
        super().__init_subclass__(**kwargs)

    def __init__(self):
        r"""Initiates the dataset."""
        self.init_source()
        self.stages = self.make_transforms()

    def init_source(self):
        """Initializes the source attribute."""
        if not isinstance(self.sources, (list, tuple)):
            self.sources = [self.sources]
        self.concrete_srcs = [src() for src in self.sources]

    def __getitem__(self, idx):
        r"""Computes a single dataset element"""
        from_sources = [src[idx] for src in self.concrete_srcs]
        data = dict(chain.from_iterable([fs.items() for fs in from_sources]))
        return self.compute(data, self.stages)

    def __len__(self):
        return len(self.concrete_srcs[0])

    def __iter__(self):
        r"""Returns an iterator over the dataset with all transforms applied"""
        for src_vals in zip(*[src for src in self.concrete_srcs]):
            data = dict(chain.from_iterable([val.items() for val in src_vals]))
            yield self.compute(data, self.stages)

    def make_transforms(self):
        r"""Instantionates transforms.

        Override if you want to pass custom keywords to transform

        Returns:
            Iterable[Transform]: Sequence of instances of available transfoms.
        """
        return [transform_cls() for transform_cls in self.transforms]

    def compute(self, data, stages):
        r"""Computes an element by applying stages to data.

        Args:
            data (dict(str -> Any)): the initial data.
            stages (Iterable[Transform]): the stages to apply over data.

        Returns:
            dict(str -> Any): Computed element
        """
        for stage in stages:
            kwargs = {
                key: value for key, value in data.items() if key in stage.requires
            }
            new_data = stage.apply(**kwargs)
            data.update(new_data)
        return data

    def compute_many(self, datas, stages):
        r"""Applies compute to a sequence of datas.

        Args:
            datas (Iterable[dict(str -> Any)]): An iterable of initial datas.
            stages (Iterable[Transform]): the stages to apply over data.
        """
        for data in datas:
            yield self.compute(data, stages)

    def assequence(self, of=None):
        r"""Returns a sequence of `{key: computed_value for key in of}`

        Args:
            of (str|set(str), optional): the keywords to keep in each element.
                Defaults to None.
        """
        if isinstance(of, str):
            of = set([of])
        datas = iter(self)
        if of is None:
            return datas
        return (
            {key: value for key, value in data.items() if key in of} for data in datas
        )
