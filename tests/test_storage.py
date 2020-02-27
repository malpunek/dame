from contextlib import contextmanager
from tempfile import TemporaryDirectory
from io import BytesIO
import pickle

from peewee import SqliteDatabase
import numpy as np

from dame.storage import PeeWeeStore, NumpyPlaceholder

from .test_classes import PlusXN


@contextmanager
def test_db():
    with TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/db.sqlite3"
        yield SqliteDatabase(db_path, pragmas={"foreign_keys": 1})


def test_separate_numpy_data():
    r1, r2 = np.random.rand(2, 10)
    data = {"a": 1, "na": r1, "b": 2, "nb": r2}
    newdata, np_data = PeeWeeStore.separate_numpy_data(data)
    assert newdata["a"] == 1 and newdata["b"] == 2
    assert (np_data == [r1, r2]) or (np_data == [r2, r1])
    assert isinstance(newdata["na"], NumpyPlaceholder) and isinstance(
        newdata["nb"], NumpyPlaceholder
    )
    assert np.all(np_data[newdata["na"].num] == r1)
    assert np.all(np_data[newdata["nb"].num] == r2)


def test_unpack_blobs():
    data = {"a": 1, "b": 2, "c": NumpyPlaceholder(0), "d": NumpyPlaceholder(1)}
    data_blob = pickle.dumps(data)
    np_blob = BytesIO()
    arr1, arr2 = np.random.rand(2, 10)
    np.save(np_blob, arr1)
    np.save(np_blob, arr2)
    np_blob = np_blob.getvalue()
    unpacked_data = PeeWeeStore.unpack_blobs(data_blob, np_blob)
    assert np.all(unpacked_data["c"] == arr1)
    assert np.all(unpacked_data["d"] == arr2)
    del unpacked_data["c"]
    del unpacked_data["d"]
    del data["c"]
    del data["d"]
    assert data == unpacked_data


def test_storage():
    with test_db() as tmp_db:
        transform = PlusXN(3)
        store = PeeWeeStore(tmp_db, (transform,))
        data = {"idx": 0, "random": np.random.rand(5), "foo": "bar"}
        store.save(data, transform)
        recv = store.load(0, transform)
        assert np.all(recv["random"] == data["random"])
        del recv["random"]
        del data["random"]
        assert recv == data
