class Dataset:
    r"""The most important class in DAME.

    Manages access, computation of dataset elements

    What you probably want to do is to subclass this class and redefine it's source
    and transforms attributes.

    Attributes:
        source_cls (Source_cls): As lightweight as possible data source
        transforms (Iterable(Transform_cls)): Processing of data items at element level

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
        for stage in self.stages:
            kwargs = {
                key: value for key, value in data.items() if key in stage.required
            }
            new_data = stage.apply(**kwargs)
            data.update(new_data)
        return data

    def make_transforms(self):
        r"""Instantionates transforms.

        Override if you want to pass custom keywords to transform

        Returns:
            Iterable(Transform): Sequence of instances of available transfoms.
        """
        return [transform_cls() for transform_cls in self.transforms]
