class Dataset:
    r"""The most important class in DAME.

    Manages access, computation of dataset elements

    What you probably want to do is to subclass this class and redefine it's source
    and transforms attributes.

    Attributes:
        source_cls (Source_cls): As lightweight as possible data source
        transforms (Iterable[Transform_cls]): Processing of data items at element level

    """
    source_cls = None
    transforms = tuple()

    def __init__(self):
        r"""Initiates the dataset."""
        self.stages = self.make_transforms()
        self.source = self.source_cls()
        self.source.keyword = getattr(self.source, "keyword", "from_source")

    def __getitem__(self, idx):
        r"""Computes a single dataset element"""
        data = {self.source.keyword: self.source[idx]}
        return self.compute(data, self.stages)

    def __len__(self):
        return len(self.source)

    def __iter__(self):
        r"""Returns an iterator over the dataset with all transforms applied"""
        for src_val in self.source:
            data = {self.source.keyword: src_val}
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
                key: value for key, value in data.items() if key in stage.required
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
