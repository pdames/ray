import pytest

<<<<<<< HEAD
from ray.autoscaler.aws.utils import resource_cache, client_cache
=======
from ray.autoscaler._private.aws.utils import resource_cache, client_cache
>>>>>>> 1c5df7098699c0f5b1ff604f63db48e8db072a50

from botocore.stub import Stubber


@pytest.fixture()
def iam_client_stub():
    resource = resource_cache("iam", "us-west-2")
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ec2_client_stub():
    resource = resource_cache("ec2", "us-west-2")
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def cloudwatch_client_stub():
    resource = resource_cache("cloudwatch", "us-west-2")
    with Stubber(resource.meta.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture()
def ssm_client_stub():
    client = client_cache("ssm", "us-west-2")
    with Stubber(client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()
