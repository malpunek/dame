class SourceWrap:
    r"""Helper class to relax requirements for a source.
    It solves two problems:
        1) the source doesn't have to supply the index key in data
        2) __getitem__ provided for sources that only contain __iter__.
            It's fast when accesing elements in increasing order.
    """

    def __init__(self, source_instance):
        self.instance = source_instance
        self._hack_iter = None
        self._hack_next_idx = None

    def __getitem__(self, idx):
        if hasattr(self.instance, "__getitem__"):
            return {**self.instance[idx], "idx": idx}
        return self.hack_getitem(idx)

    def __iter__(self):
        for idx, data in enumerate(self.instance):
            yield {**data, "idx": idx}

    def hack_getitem(self, idx):

        if self._hack_iter is None or self._hack_next_idx > idx:
            self._hack_iter = iter(self.instance)
            self._hack_next_idx = 0
        diff = idx - self._hack_next_idx
        for _ in range(diff):
            self._hack_next_idx += 1
            _ = next(self._hack_iter)
        self._hack_next_idx += 1
        return {**next(self._hack_iter), "idx": idx}
