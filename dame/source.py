class Source:

    provides = ("from source",)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]
