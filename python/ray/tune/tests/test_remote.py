import inspect
import unittest
from unittest.mock import patch

import ray
import ray.train
from ray.tune import choice, register_trainable, run, run_experiments
from ray.tune.experiment import Experiment, Trial
from ray.tune.result import TIMESTEPS_TOTAL
from ray.tune.search.hyperopt import HyperOptSearch
from ray.util.client.ray_client_helpers import ray_start_client_server


def train_fn(config):
    for i in range(100):
        ray.tune.report(dict(timesteps_total=i))


class RemoteTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testRemoteRunExperiments(self):
        register_trainable("f1", train_fn)
        exp1 = Experiment(
            **{
                "name": "foo",
                "run": "f1",
            }
        )
        [trial] = run_experiments(exp1, _remote=True)
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testRemoteRun(self):
        analysis = run(train_fn, _remote=True)
        [trial] = analysis.trials
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testRemoteRunArguments(self):
        def mocked_run(*args, **kwargs):
            capture_args_kwargs = (args, kwargs)
            return run(*args, **kwargs), capture_args_kwargs

        with patch("ray.tune.tune.run", mocked_run):
            analysis, capture_args_kwargs = run(train_fn, _remote=True)
        args, kwargs = capture_args_kwargs
        self.assertFalse(args)
        kwargs.pop("run_or_experiment")
        kwargs.pop("_remote")
        kwargs.pop("progress_reporter")  # gets autodetected and set

        default_kwargs = {
            k: v.default for k, v in inspect.signature(run).parameters.items()
        }
        default_kwargs.pop("run_or_experiment")
        default_kwargs.pop("_remote")
        default_kwargs.pop("progress_reporter")

        self.assertDictEqual(kwargs, default_kwargs)

    def testRemoteRunWithSearcher(self):
        analysis = run(
            train_fn,
            search_alg=HyperOptSearch(),
            config={"a": choice(["a", "b"])},
            metric="timesteps_total",
            mode="max",
            _remote=True,
        )
        [trial] = analysis.trials
        self.assertEqual(trial.status, Trial.TERMINATED)
        self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testRemoteRunExperimentsInClient(self):
        ray.init()
        assert not ray.util.client.ray.is_connected()
        with ray_start_client_server():
            assert ray.util.client.ray.is_connected()

            register_trainable("f1", train_fn)
            exp1 = Experiment(
                **{
                    "name": "foo",
                    "run": "f1",
                }
            )
            [trial] = run_experiments(exp1)
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)

    def testRemoteRunInClient(self):
        ray.init()
        assert not ray.util.client.ray.is_connected()
        with ray_start_client_server():
            assert ray.util.client.ray.is_connected()

            analysis = run(train_fn)
            [trial] = analysis.trials
            self.assertEqual(trial.status, Trial.TERMINATED)
            self.assertEqual(trial.last_result[TIMESTEPS_TOTAL], 99)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
