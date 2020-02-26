from dame.source import SourceWrap


class MockSource:
    def __iter__(self):
        for i in range(10):
            yield {"what": i + 10}


def test_hack_getitem():
    src = SourceWrap(MockSource())
    for _ in range(2):
        for j in range(10):
            assert src[j] == {"what": j + 10, "idx": j}
