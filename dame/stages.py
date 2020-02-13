class Stages:
    """DAG functionality for Dataset's transforms."""

    def __init__(self, sources, transforms):
        if not isinstance(sources, (list, tuple)):
            sources = [sources]
        self.stages = self.topsort(sources + list(transforms))

    def topsort(self, stages):
        """Sorts the transforms topologistagcally."""
        provider = {key: s for s in stages for key in s.provides}
        self.provider = provider

        # TODO: maybe instead of t.requires dame should just use t.apply.params
        depends = {s: set() for s in stages}
        for s in stages:
            for key in getattr(s, "requires", tuple()):
                depends[provider[key]].add(s)

        requires = {s: set(getattr(s, "requires", tuple())) for s in stages}
        Q = [s for s, deps in requires.items() if len(deps) == 0]
        ordered = []
        while Q:
            s = Q.pop()
            ordered.append(s)
            for dependant in depends[s]:
                provides = set(s.provides)
                requires[dependant] -= provides
                if len(requires[dependant]) == 0:
                    Q.append(dependant)
        assert len(ordered) == len(stages), "Something is wrong with topsort!"
        return ordered

    def __iter__(self):
        return iter(self.stages)

    def __getitem__(self, idx):
        return self.stages[idx]

    def to(self, *keywords):
        result = set()
        Q = list([self.provider[kw] for kw in keywords])
        while Q:
            t = Q.pop()
            if t not in result:
                result.add(t)
                Q.extend(set(self.provider[kw] for kw in getattr(t, "requires", [])))
        return filter(lambda t: t in result, self.stages)
