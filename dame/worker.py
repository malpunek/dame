from multiprocessing import Pool

from .stages import Stages
from .source import SourceWrap


def make_stage_with_context(stage_cls, context):
    if stage_cls.__name__ in context:
        ctx = context[stage_cls.__name__]
        return stage_cls(*ctx.get("args", []), **ctx.get("kwargs", {}))
    return stage_cls()


class SequentialWorker:
    r"""Performs all the necessary computations in a current process"""

    @staticmethod
    def make_instance(stages, context):
        SequentialWorker.instance = SequentialWorker(stages, context)

    def __init__(self, stages, context):
        self.stages = stages
        self.context = context

    @property
    def stage_instances(self):
        if not hasattr(self, "_stage_instances"):
            self._stage_instances = [
                make_stage_with_context(stage_cls, self.context)
                for stage_cls in self.stages
            ]
        return self._stage_instances

    @staticmethod
    def instance_compute_to(d, kw):
        return SequentialWorker.instance.compute_to(d, kw)

    def compute_to(self, data, keyword):
        r"""Computes an element upto the transform T that provides the keyword (including T).

        Args:
            data (dict): data from source
            keyword (string): Computation stops while the data has keyword
        
        Returns:
            dict: data after transformation via all declared transforms
        """
        cls_it = self.stages.to(keyword)
        stage_it = iter(self.stage_instances)
        while keyword not in data:
            stage = next(stage_it)
            next_cls = next(cls_it)
            while not isinstance(stage, next_cls):
                stage = next(stage_it)
            data = self.compute_stage(data, stage)
        return data

    @staticmethod
    def instance_compute_full(d):
        return SequentialWorker.instance.compute_full(d)

    def compute_full(self, data):
        r"""Computes an element using all the transforms
        
        Args:
            data (dict): data from source
        
        Returns:
            dict: data after transformation via all declared transforms
        """
        for stage in self.stage_instances:
            data = self.compute_stage(data, stage)
        return data

    @staticmethod
    def instance_compute_stage(d, s):
        return SequentialWorker.instance.compute_stage(d, s)

    def compute_stage(self, data, stage):
        r"""Computes a single stage
        
        Args:
            data (dict): data from previous transforms
            stage (Transform): Transformation to apply
        
        Returns:
            dict: updated data
        """
        kwargs = {
            key: value
            for key, value in data.items()
            if key in self.stages.get_requirements(stage)
        }
        new_data = stage.apply(**kwargs)
        data.update(new_data)
        return data


class PoolJoiningIterator:
    def __init__(self, base_iter, pool):
        self.it = base_iter
        self.pool = pool

    def __iter__(self):
        try:
            yield from self.it
        finally:
            self.pool.close()
            self.pool.join()


class WorkManager:
    """Manages the spawning of workers in separate processes."""

    def __init__(self, source, transforms, context, n_processes=None):
        self.n_processes = n_processes
        self.source = source
        self.context = context
        self.stages = Stages(self.source, transforms)

    @property
    def source_instance(self):
        if not hasattr(self, "_source_instance"):
            self._source_instance = make_stage_with_context(self.source, self.context)
        return self._source_instance

    @property
    def wrapped_source_instance(self):
        if not hasattr(self, "_wrapped_source_instance"):
            self._wrapped_source_instance = SourceWrap(self.source_instance)
        return self._wrapped_source_instance

    def fast_compute(self):
        pool = Pool(
            processes=self.n_processes,
            initializer=SequentialWorker.make_instance,
            initargs=(self.stages, self.context),
        )

        res_it = pool.imap(
            SequentialWorker.instance_compute_full,
            (data for data in self.wrapped_source_instance),
        )

        return iter(PoolJoiningIterator(res_it, pool))

    def compute_one(self, idx):
        worker = SequentialWorker(self.stages, self.context)
        return worker.compute_full(self.wrapped_source_instance[idx])
