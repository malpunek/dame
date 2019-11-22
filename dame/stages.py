class Stages:
    """DAG functionality for Dataset's transforms."""

    def __init__(self, source, transforms):
        self.transforms = transforms
        self.source = source
        self.topsort()
        self.make_instances()

    def topsort(self):
        """Sorts the transforms topologically."""
        provider = {key: t for t in self.transforms for key in t.provides}
        provider[self.source.keyword] = self.source

        depends = {t: set() for t in self.transforms}
        depends[self.source] = set()
        for t in self.transforms:
            for key in t.requires:
                depends[provider[key]].add(t)

        # TODO: maybe instead of t.requires dame should just use t.apply.params
        requires = {t: set(t.requires) for t in self.transforms}
        requires[self.source] = set()
        Q = [self.source]
        ordered = []
        while Q:
            t = Q.pop()
            ordered.append(t)
            for dependant in depends[t]:
                if t == self.source:
                    provides = set([self.source.keyword])
                else:
                    provides = set(t.provides)
                requires[dependant] -= provides
                if len(requires[dependant]) == 0:
                    Q.append(dependant)
        assert (
            len(ordered) == len(self.transforms) + 1
        ), "Something is wrong with topsort!"
        self.topsorted = ordered
        self.provider = provider

    def make_instances(self):
        """Instantionate the transforms.

        Override this method if you want to provide parameters to
        transforms.
        """
        self.stages = [t() for t in self.topsorted]

    def __iter__(self):
        return iter(self.stages)

    def to(self, *keywords):
        result = set()
        Q = list([self.provider[kw] for kw in keywords])
        while Q:
            t = Q.pop()
            if t not in result:
                result.add(t)
                Q.extend(set(self.provider[kw] for kw in getattr(t, "requires", [])))
        return filter(lambda t: t.__class__ in result, self.stages)
