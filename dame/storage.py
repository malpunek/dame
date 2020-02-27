from datetime import datetime
from io import BytesIO
import pickle

import numpy as np
from peewee import (
    Model,
    FixedCharField,
    ForeignKeyField,
    DateTimeField,
    BlobField,
    IntegerField,
)


class TransformModel(Model):
    name = FixedCharField(max_length=128)
    digest = FixedCharField(max_length=128)
    version = DateTimeField(default=datetime.now)


class Result(Model):
    origin = ForeignKeyField(TransformModel, backref="results")
    dataset_index = IntegerField()
    pickled_data = BlobField()
    numpy_data = BlobField(null=True)


class NumpyPlaceholder:
    def __init__(self, num):
        self.num = num


class PeeWeeStore:
    def __init__(self, db, transforms):
        self.transforms = transforms
        self.db = db
        # Models in relation are automatically bound
        TransformModel.bind(db)
        self.db.connect()
        self.db.create_tables([TransformModel, Result])

    def close(self):
        self.db.close()

    @property
    def transform_ids(self):
        if not hasattr(self, "_transform_ids"):
            self._transform_ids = {
                t.__class__.__name__: TransformModel.get_or_create(
                    digest=t.version(), name=t.__class__.__name__
                )[0].id
                for t in self.transforms
            }
        return self._transform_ids

    @staticmethod
    def separate_numpy_data(data):
        np_data = []
        for key in sorted(data.keys()):
            if isinstance(data[key], np.ndarray):
                np_data.append(data[key])
                data[key] = NumpyPlaceholder(len(np_data) - 1)
        return data, np_data

    @staticmethod
    def get_blobs(data, np_data):
        out = pickle.dumps(data)
        if len(np_data) > 0:
            np_out = BytesIO()
            for arr in np_data:
                np.save(np_out, arr)
            return out, np_out.getvalue()
        return out, None

    @staticmethod
    def unpack_blobs(data_blob, np_data_blob):
        data = pickle.loads(data_blob)
        if np_data_blob is not None:
            ord_keys = [i for i in data.items() if isinstance(i[1], NumpyPlaceholder)]
            ord_keys = sorted(ord_keys, key=lambda item: item[1].num)
            ord_keys, _ = zip(*ord_keys)
            np_data_blob = BytesIO(np_data_blob)
            for key in ord_keys:
                data[key] = np.load(np_data_blob)
        return data

    def save(self, data, transform):
        data = dict(data)
        out, np_out = self.get_blobs(*self.separate_numpy_data(data))
        trans = self.transform_ids[transform.__class__.__name__]
        res = Result(
            origin_id=trans,
            dataset_index=data["idx"],
            pickled_data=out,
            numpy_data=np_out,
        )
        res.save()

    def load(self, idx, transform):
        trans = self.transform_ids[transform.__class__.__name__]
        res = Result.get_or_none(origin_id=trans, dataset_index=idx)
        if res is None:
            return None
        return self.unpack_blobs(res.pickled_data, res.numpy_data)
