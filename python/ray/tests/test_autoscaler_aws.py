from datetime import datetime

import pytest
import os
import copy
import yaml

from unittest import mock

from botocore.stub import ANY

import ray.autoscaler.aws.config
from ray.autoscaler.aws.config import key_pair

from ray.autoscaler.commands import fillout_defaults
from ray.autoscaler.commands import validate_config

# Override global constants used in AWS autoscaler config artifact names.
# This helps ensure that any unmocked test doesn't alter non-test artifacts.
ray.autoscaler.aws.config.RAY = \
    "ray-autoscaler-aws-test"
ray.autoscaler.aws.config.DEFAULT_RAY_INSTANCE_PROFILE = \
    ray.autoscaler.aws.config.RAY + "-v1"
ray.autoscaler.aws.config.DEFAULT_RAY_IAM_ROLE = \
    ray.autoscaler.aws.config.RAY + "-v1"
ray.autoscaler.aws.config.SECURITY_GROUP_TEMPLATE = \
    ray.autoscaler.aws.config.RAY + "-{}"

# Default IAM instance profile to expose to tests.
DEFAULT_INSTANCE_PROFILE = {
    "Arn": "arn:aws:iam::336924118301:instance-profile/ExampleInstanceProfile",
    "CreateDate": datetime(2013, 6, 12, 23, 52, 2, 2),
    "InstanceProfileId": "AID2MAB8DPLSRHEXAMPLE",
    "InstanceProfileName": "ExampleInstanceProfile",
    "Path": "/",
    "Roles": [
        {
            "Arn": "arn:aws:iam::336924118301:role/Test-Role",
            "AssumeRolePolicyDocument": "ExampleAssumeRolePolicyDocument",
            "CreateDate": datetime(2013, 1, 9, 6, 33, 26, 2),
            "Path": "/",
            "RoleId": "AIDGPMS9RO4H3FEXAMPLE",
            "RoleName": "Test-Role",
        },
    ]
}

# Default EC2 key pair to expose to tests.
DEFAULT_KEY_PAIR = {
    "KeyFingerprint": "1f:51:ae:28:bf:89:e9:d8:1f:25:5d:37:2d:7d:b8:ca:9f",
    "KeyName": ray.autoscaler.aws.config.RAY + "_us-west-2",
}

# Primary EC2 subnet to expose to tests.
DEFAULT_SUBNET = {
    "AvailabilityZone": "us-west-2a",
    "AvailableIpAddressCount": 251,
    "CidrBlock": "10.0.1.0/24",
    "DefaultForAz": False,
    "MapPublicIpOnLaunch": True,
    "State": "available",
    "SubnetId": "subnet-fa7c47",
    "VpcId": "vpc-ba71007",
}

# Secondary EC2 subnet to expose to tests as required.
AUX_SUBNET = {
    "AvailabilityZone": "us-west-2a",
    "AvailableIpAddressCount": 251,
    "CidrBlock": "192.168.1.0/24",
    "DefaultForAz": False,
    "MapPublicIpOnLaunch": True,
    "State": "available",
    "SubnetId": "subnet-c47fe37",
    "VpcId": "vpc-70017ab",
}

# Default security group settings immediately after creation
# (prior to inbound rule configuration).
DEFAULT_SG = {
    "Description": "Auto-created security group for Ray workers",
    "GroupName": ray.autoscaler.aws.config.RAY + "-security-groups",
    "OwnerId": "test-owner",
    "GroupId": "sg-1234abcd",
    "VpcId": DEFAULT_SUBNET["VpcId"],
    "IpPermissions": [],
    "IpPermissionsEgress": [{
        "FromPort": -1,
        "ToPort": -1,
        "IpProtocol": "-1",
        "IpRanges": [{
            "CidrIp": "0.0.0.0/0"
        }]
    }],
    "Tags": []
}

# Secondary security group settings after creation
# (prior to inbound rule configuration).
AUX_SG = copy.deepcopy(DEFAULT_SG)
AUX_SG["GroupName"] += "-aux"
AUX_SG["GroupId"] = "sg-dcba4321"

# Default security group settings immediately after creation on aux subnet
# (prior to inbound rule configuration).
DEFAULT_SG_AUX_SUBNET = copy.deepcopy(DEFAULT_SG)
DEFAULT_SG_AUX_SUBNET["VpcId"] = AUX_SUBNET["VpcId"]
DEFAULT_SG_AUX_SUBNET["GroupId"] = AUX_SG["GroupId"]

# Default security group settings once default inbound rules are applied
# (if used by both head and worker nodes)
DEFAULT_SG_WITH_RULES = copy.deepcopy(DEFAULT_SG)
DEFAULT_SG_WITH_RULES["IpPermissions"] = [{
    "FromPort": -1,
    "ToPort": -1,
    "IpProtocol": "-1",
    "UserIdGroupPairs": [{
        "GroupId": DEFAULT_SG["GroupId"]
    }]
}]
DEFAULT_SG_WITH_RULES["IpPermissions"].extend([{
    "FromPort": channel[ray.autoscaler.aws.config.FROM_PORT],
    "ToPort": channel[ray.autoscaler.aws.config.TO_PORT],
    "IpProtocol": channel[ray.autoscaler.aws.config.IP_PROTOCOL],
    "IpRanges": [{
        "CidrIp": "0.0.0.0/0"
    }]
} for channel in ray.autoscaler.aws.config.DEFAULT_INBOUND_CHANNELS])

# Default security group once default inbound rules are applied
# (if using separate security groups for head and worker nodes).
DEFAULT_SG_DUAL_GROUP_RULES = copy.deepcopy(DEFAULT_SG_WITH_RULES)
DEFAULT_SG_DUAL_GROUP_RULES["IpPermissions"][0]["UserIdGroupPairs"].append({
    "GroupId": AUX_SG["GroupId"]
})

# Default security group on aux subnet once default inbound rules are applied.
DEFAULT_SG_WITH_RULES_AUX_SUBNET = copy.deepcopy(DEFAULT_SG_DUAL_GROUP_RULES)
DEFAULT_SG_WITH_RULES_AUX_SUBNET["VpcId"] = AUX_SUBNET["VpcId"]
DEFAULT_SG_WITH_RULES_AUX_SUBNET["GroupId"] = AUX_SG["GroupId"]

# Secondary security group settings after default inbound rules are applied
# (if used by both head and worker nodes)
AUX_SG_WITH_RULES = copy.deepcopy(DEFAULT_SG_WITH_RULES)
AUX_SG_WITH_RULES["GroupName"] = AUX_SG["GroupName"]
AUX_SG_WITH_RULES["GroupId"] = AUX_SG["GroupId"]
AUX_SG_WITH_RULES["IpPermissions"][0]["UserIdGroupPairs"] = [{
    "GroupId": AUX_SG["GroupId"]
}]

# Secondary security group once default inbound rules are applied
# (if using separate security groups for head and worker nodes).
AUX_SG_DUAL_GROUP_RULES = copy.deepcopy(AUX_SG_WITH_RULES)
AUX_SG_DUAL_GROUP_RULES["IpPermissions"][0]["UserIdGroupPairs"].insert(
    0, {"GroupId": DEFAULT_SG["GroupId"]})

# Default security group with only default cross-group rules.
# (if using separate security groups for head and worker nodes).
DEFAULT_SG_DUAL_GROUP_RULES_ONLY = copy.deepcopy(DEFAULT_SG_DUAL_GROUP_RULES)
DEFAULT_SG_DUAL_GROUP_RULES_ONLY["IpPermissions"].pop(1)

# Worker security group after inbound rules are applied from
# ray/autoscaler/aws/example-security-groups.yaml
EXAMPLE_WORKERS_SECURITY_GROUP = copy.deepcopy(AUX_SG_DUAL_GROUP_RULES)
EXAMPLE_WORKERS_SECURITY_GROUP["IpPermissions"].pop(1)

# Head security group after required rules have been appended that are missing
# from ray/autoscaler/aws/example-security-groups.yaml
EXAMPLE_HEAD_SG_REQUIRED_RULES = copy.deepcopy(DEFAULT_SG_WITH_RULES)
EXAMPLE_HEAD_SG_REQUIRED_RULES["IpPermissions"][0]["UserIdGroupPairs"].append({
    "GroupId": EXAMPLE_WORKERS_SECURITY_GROUP["GroupId"]
})
EXAMPLE_HEAD_SG_REQUIRED_RULES["IpPermissions"][1]["IpRanges"] = [{
    "CidrIp": "0.0.0.0/0"
}]
EXAMPLE_HEAD_SG_REQUIRED_RULES["IpPermissions"].append({
    "FromPort": 80,
    "ToPort": 80,
    "IpProtocol": "tcp",
    "IpRanges": [{
        "CidrIp": "10.1.0.0/16"
    }, {
        "CidrIp": "192.168.1.0/24"
    }],
    "Ipv6Ranges": [{
        "CidrIpv6": "2002::1234:abcd:ffff:c0a8:101/128"
    }]
})

# Head security group after inbound rules are applied from
# ray/autoscaler/aws/example-security-groups.yaml
EXAMPLE_HEAD_SECURITY_GROUP = copy.deepcopy(EXAMPLE_HEAD_SG_REQUIRED_RULES)
EXAMPLE_HEAD_SECURITY_GROUP["IpPermissions"].pop(0)


def _load_aws_security_group_example_config():
    import ray.autoscaler.aws as ray_aws
    return os.path.join(
        os.path.dirname(ray_aws.__file__), "example-security-groups.yaml")


def _mock_path_exists_key_pair(path):
    key_name, key_path = key_pair(0, "us-west-2", DEFAULT_KEY_PAIR["KeyName"])
    return path == key_path


def _configure_iam_role_default_stubs(iam_client_stub):
    iam_client_stub.add_response(
        "get_instance_profile",
        expected_params={
            "InstanceProfileName": ray.autoscaler.aws.config.
            DEFAULT_RAY_INSTANCE_PROFILE
        },
        service_response={"InstanceProfile": DEFAULT_INSTANCE_PROFILE})


def _configure_key_pair_default_stubs(ec2_client_stub):
    patcher = mock.patch("os.path.exists")
    os_path_exists_mock = patcher.start()
    os_path_exists_mock.side_effect = _mock_path_exists_key_pair

    ec2_client_stub.add_response(
        "describe_key_pairs",
        expected_params={
            "Filters": [{
                "Name": "key-name",
                "Values": [DEFAULT_KEY_PAIR["KeyName"]]
            }]
        },
        service_response={"KeyPairs": [DEFAULT_KEY_PAIR]})


def _configure_subnet_default_stubs(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={},
        service_response={"Subnets": [DEFAULT_SUBNET]})


def _skip_to_configure_sg_stubs(ec2_client_stub, iam_client_stub):
    _configure_iam_role_default_stubs(iam_client_stub)
    _configure_key_pair_default_stubs(ec2_client_stub)
    _configure_subnet_default_stubs(ec2_client_stub)


def _describe_subnets_echo_stub(ec2_client_stub, subnet):
    ec2_client_stub.add_response(
        "describe_subnets",
        expected_params={
            "Filters": [{
                "Name": "subnet-id",
                "Values": [subnet["SubnetId"]]
            }]
        },
        service_response={"Subnets": [subnet]})


def _describe_no_security_groups_stub(ec2_client_stub):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"Filters": ANY},
        service_response={})


def _create_sg_echo_stub(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "create_security_group",
        expected_params={
            "Description": security_group["Description"],
            "GroupName": security_group["GroupName"],
            "VpcId": security_group["VpcId"]
        },
        service_response={"GroupId": security_group["GroupId"]})


def _describe_sgs_on_vpc_stub(ec2_client_stub, vpc_ids, security_groups):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"Filters": [{
            "Name": "vpc-id",
            "Values": vpc_ids
        }]},
        service_response={"SecurityGroups": security_groups})


def _authorize_sg_ingress_stub(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "authorize_security_group_ingress",
        expected_params={
            "GroupId": security_group["GroupId"],
            "IpPermissions": security_group["IpPermissions"]
        },
        service_response={})


def _describe_sg_echo_stub(ec2_client_stub, security_group):
    ec2_client_stub.add_response(
        "describe_security_groups",
        expected_params={"GroupIds": [security_group["GroupId"]]},
        service_response={"SecurityGroups": [security_group]})


def _create_sg_different_rules_same_vpc_stubs(ec2):
    # head and worker nodes have no custom subnets defined,
    # so return only the default subnet for both security groups
    _describe_subnets_echo_stub(ec2, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    _describe_no_security_groups_stub(ec2)
    # expect new default security group creation on the head/worker node VPC
    _create_sg_echo_stub(ec2, DEFAULT_SG)
    # expect new default security group details to be retrieved after creation
    _describe_sgs_on_vpc_stub(ec2, [DEFAULT_SUBNET["VpcId"]], [DEFAULT_SG])

    # given different head/worker inbound rule configs...
    # expect an attempt to retrieve the aux group on the shared VPC
    _describe_sgs_on_vpc_stub(ec2, [DEFAULT_SUBNET["VpcId"]], [DEFAULT_SG])
    # given only the default security group on the shared VPC...
    # expect the aux security group to be created
    _create_sg_echo_stub(ec2, AUX_SG)
    # expect new aux security group details to be retrieved following creation
    _describe_sgs_on_vpc_stub(ec2, [DEFAULT_SUBNET["VpcId"]],
                              [DEFAULT_SG, AUX_SG])


def test_create_sg_different_rules_same_vpc(iam_client_stub, ec2_client_stub):
    # use default stubs to skip ahead to security group configuration
    _skip_to_configure_sg_stubs(ec2_client_stub, iam_client_stub)

    # use default stubs to skip ahead to security group rule authorization
    _create_sg_different_rules_same_vpc_stubs(ec2_client_stub)

    # given no existing default security group inbound rules...
    # expect to authorize all configured head inbound rules
    _authorize_sg_ingress_stub(ec2_client_stub, EXAMPLE_HEAD_SECURITY_GROUP)
    # given this modification to the default security group...
    # expect the next read of ip_permissions to reload them
    _describe_sg_echo_stub(ec2_client_stub, EXAMPLE_HEAD_SECURITY_GROUP)

    # given no cross-security-group rules for the default security group...
    # expect to authorize all required cross-security-group rules
    _authorize_sg_ingress_stub(ec2_client_stub,
                               DEFAULT_SG_DUAL_GROUP_RULES_ONLY)

    # given no existing aux security group inbound rules...
    # expect to authorize all configured worker inbound rules
    _authorize_sg_ingress_stub(ec2_client_stub, EXAMPLE_WORKERS_SECURITY_GROUP)
    # given this modification to the aux security group...
    # expect the next read of ip_permissions to reload them
    _describe_sg_echo_stub(ec2_client_stub, EXAMPLE_WORKERS_SECURITY_GROUP)

    # given the prior modification to the default security group...
    # expect the next read of one of its properties to reload it
    _describe_sg_echo_stub(ec2_client_stub, EXAMPLE_HEAD_SG_REQUIRED_RULES)

    # given our mocks and the security group example config file as input...
    # expect the config to be loaded, validated, and bootstrapped successfully
    config_file = _load_aws_security_group_example_config()
    config = yaml.safe_load(open(config_file).read())
    config = fillout_defaults(config)
    validate_config(config)
    config = ray.autoscaler.aws.config.bootstrap_aws(config)

    # expect the finalized config to have different head and worker node
    # security groups on the same subnet
    assert config["head_node"]["SecurityGroupIds"] == [DEFAULT_SG["GroupId"]]
    assert config["head_node"]["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]
    assert config["worker_nodes"]["SecurityGroupIds"] == [AUX_SG["GroupId"]]
    assert config["worker_nodes"]["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


CONFIG_DEFAULT_RULES_DIFFERENT_VPC = {
    "cluster_name": "security-groups",
    "min_workers": 1,
    "max_workers": 1,
    "provider": {
        "type": "aws",
        "region": "us-west-2",
        "availability_zone": "us-west-2a",
    },
    "auth": {
        "ssh_user": "ubuntu",
    },
    "head_node": {
        "SubnetIds": [DEFAULT_SUBNET["SubnetId"]]
    },
    "worker_nodes": {
        "SubnetIds": [AUX_SUBNET["SubnetId"]]
    }
}


def test_create_sg_different_vpc_same_rules(iam_client_stub, ec2_client_stub):
    # use default stubs to skip ahead to security group configuration
    _skip_to_configure_sg_stubs(ec2_client_stub, iam_client_stub)

    # given head and worker nodes with custom subnets defined...
    # expect to first describe the worker subnet ID
    _describe_subnets_echo_stub(ec2_client_stub, AUX_SUBNET)
    # expect to second describe the head subnet ID
    _describe_subnets_echo_stub(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    _describe_no_security_groups_stub(ec2_client_stub)
    # expect to first create a security group on the worker node VPC
    _create_sg_echo_stub(ec2_client_stub, DEFAULT_SG_AUX_SUBNET)
    # expect new worker security group details to be retrieved after creation
    _describe_sgs_on_vpc_stub(ec2_client_stub, [AUX_SUBNET["VpcId"]],
                              [DEFAULT_SG_AUX_SUBNET])
    # expect to second create a security group on the head node VPC
    _create_sg_echo_stub(ec2_client_stub, DEFAULT_SG)
    # expect new head security group details to be retrieved after creation
    _describe_sgs_on_vpc_stub(ec2_client_stub, [DEFAULT_SUBNET["VpcId"]],
                              [DEFAULT_SG])

    # given no existing default head security group inbound rules...
    # expect to authorize all default head inbound rules
    _authorize_sg_ingress_stub(ec2_client_stub, DEFAULT_SG_DUAL_GROUP_RULES)
    # given no existing default worker security group inbound rules...
    # expect to authorize all default worker inbound rules
    _authorize_sg_ingress_stub(ec2_client_stub,
                               DEFAULT_SG_WITH_RULES_AUX_SUBNET)

    # given the prior modification to the head security group...
    # expect the next read of a head security group property to reload it
    _describe_sg_echo_stub(ec2_client_stub, DEFAULT_SG_WITH_RULES)
    # given the prior modification to the worker security group...
    # expect the next read of a worker security group property to reload it
    _describe_sg_echo_stub(ec2_client_stub, DEFAULT_SG_WITH_RULES_AUX_SUBNET)

    # given our mocks and test config as input...
    # expect the test config to be bootstrapped successfully
    config = ray.autoscaler.aws.config.bootstrap_aws(
        copy.deepcopy(CONFIG_DEFAULT_RULES_DIFFERENT_VPC))

    # expect the bootstrapped config to show different head and worker security
    # groups residing on different subnets
    assert config["head_node"]["SecurityGroupIds"] == [DEFAULT_SG["GroupId"]]
    assert config["head_node"]["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]
    assert config["worker_nodes"]["SecurityGroupIds"] == [AUX_SG["GroupId"]]
    assert config["worker_nodes"]["SubnetIds"] == [AUX_SUBNET["SubnetId"]]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


CONFIG_DEFAULT_RULES_EQUIVALENT_RULES = {
    "cluster_name": "security-groups",
    "min_workers": 1,
    "max_workers": 1,
    "provider": {
        "type": "aws",
        "region": "us-west-2",
        "availability_zone": "us-west-2a",
        "security_group": {
            "head": {
                "inbound_rules": [{
                    "FromPort": 80,
                    "ToPort": 80,
                    "IpProtocol": "tcp",
                    "IpRanges": [{
                        "CidrIp": "64.39.64.0/18",
                    }, {
                        "CidrIp": "64.39.96.0/18"
                    }],
                    "Ipv6Ranges": [{
                        "CidrIpv6": "2Aa2:0:0:1234::/64",
                    }, {
                        "CidrIpv6": "2aA2::1234:0:0:0:0/64"
                    }]
                }]
            }
        }
    },
    "auth": {
        "ssh_user": "ubuntu",
    },
    "head_node": {},
    "worker_nodes": {}
}


def test_create_sg_equivalent_rule_dedupes(iam_client_stub, ec2_client_stub):
    # prepare an expected security group with deterministically deduped rules
    sg_with_deduped_rules = copy.deepcopy(DEFAULT_SG)
    sg_with_deduped_rules["IpPermissions"] = [{
        "FromPort": 80,
        "ToPort": 80,
        "IpProtocol": "tcp",
        "IpRanges": [{
            "CidrIp": "64.39.64.0/18"
        }],
        "Ipv6Ranges": [{
            "CidrIpv6": "2aa2:0:0:1234::/64"
        }]
    }]

    # prepare an expected final security group with deduped and required rules
    sg_with_deduped_and_required_rules = copy.deepcopy(sg_with_deduped_rules)
    sg_with_deduped_and_required_rules["IpPermissions"].append(
        copy.deepcopy(DEFAULT_SG_DUAL_GROUP_RULES["IpPermissions"][0]))

    # use default stubs to skip ahead to security group configuration
    _skip_to_configure_sg_stubs(ec2_client_stub, iam_client_stub)

    # use default stubs to skip ahead to security group rule authorization
    _create_sg_different_rules_same_vpc_stubs(ec2_client_stub)

    # given no existing default security group inbound rules...
    # expect to authorize all configured head inbound rules
    # expect all equivalent rules to be deterministically deduped
    _authorize_sg_ingress_stub(ec2_client_stub, sg_with_deduped_rules)
    # given this modification to the default security group...
    # expect the next read of ip_permissions to reload them
    _describe_sg_echo_stub(ec2_client_stub, sg_with_deduped_rules)

    # given no cross-security-group rules for the default security group...
    # expect to authorize all required cross-security-group rules
    _authorize_sg_ingress_stub(ec2_client_stub,
                               DEFAULT_SG_DUAL_GROUP_RULES_ONLY)

    # given no existing aux security group inbound rules...
    # expect to authorize all default worker inbound rules
    _authorize_sg_ingress_stub(ec2_client_stub, AUX_SG_DUAL_GROUP_RULES)

    # given the prior modification to the default security group...
    # expect the next read of one of its properties to reload it
    _describe_sg_echo_stub(ec2_client_stub, sg_with_deduped_and_required_rules)

    # given the prior modification to the aux security group...
    # expect the next read of one of its properties to reload it
    _describe_sg_echo_stub(ec2_client_stub, AUX_SG_WITH_RULES)

    # given our mocks and test config as input...
    # expect the test config to be bootstrapped successfully
    config = ray.autoscaler.aws.config.bootstrap_aws(
        copy.deepcopy(CONFIG_DEFAULT_RULES_EQUIVALENT_RULES))

    # expect config to show different head and worker security groups residing
    # on the same subnet
    assert config["head_node"]["SecurityGroupIds"] == [DEFAULT_SG["GroupId"]]
    assert config["head_node"]["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]
    assert config["worker_nodes"]["SecurityGroupIds"] == [AUX_SG["GroupId"]]
    assert config["worker_nodes"]["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
