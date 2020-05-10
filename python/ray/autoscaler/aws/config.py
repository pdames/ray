from distutils.version import StrictVersion
from functools import partial
import json
import os
import time
import logging
import ipaddress
import copy

import boto3
from botocore.config import Config
import botocore

from ray.ray_constants import BOTO_MAX_RETRIES

logger = logging.getLogger(__name__)

RAY = "ray-autoscaler"
DEFAULT_RAY_INSTANCE_PROFILE = RAY + "-v1"
DEFAULT_RAY_IAM_ROLE = RAY + "-v1"
SECURITY_GROUP_TEMPLATE = RAY + "-{}"

DEFAULT_AMI_NAME = "AWS Deep Learning AMI (Ubuntu 18.04) V26.0"

# Obtained from https://aws.amazon.com/marketplace/pp/B07Y43P7X5 on 4/25/2020.
DEFAULT_AMI = {
    "us-east-1": "ami-0dbb717f493016a1a",  # US East (N. Virginia)
    "us-east-2": "ami-0d6808451e868a339",  # US East (Ohio)
    "us-west-1": "ami-09f2f73141c83d4fe",  # US West (N. California)
    "us-west-2": "ami-008d8ed4bd7dc2485",  # US West (Oregon)
    "ca-central-1": "ami-0142046278ba71f25",  # Canada (Central)
    "eu-central-1": "ami-09633db638556dc39",  # EU (Frankfurt)
    "eu-west-1": "ami-0c265211f000802b0",  # EU (Ireland)
    "eu-west-2": "ami-0f532395ff8544941",  # EU (London)
    "eu-west-3": "ami-03dbbdf69bbb06b29",  # EU (Paris)
    "sa-east-1": "ami-0da2c49fe75e7e5ed",  # SA (Sao Paulo)
}

FROM_PORT = 0
TO_PORT = 1
IP_PROTOCOL = 2

ALL_TRAFFIC_CHANNEL = (-1, -1, "-1")
SSH_CHANNEL = (22, 22, "tcp")
DEFAULT_INBOUND_CHANNELS = [SSH_CHANNEL]

IPP_TARGETS_BY_GROUP = {
    "IpRanges": "CidrIp",
    "Ipv6Ranges": "CidrIpv6",
    "PrefixListIds": "PrefixListId",
    "UserIdGroupPairs": "GroupId"
}

RESOURCE_CACHE = {}


assert StrictVersion(boto3.__version__) >= StrictVersion("1.4.8"), \
    "Boto3 version >= 1.4.8 required, try `pip install -U boto3`"


def key_pair(i, region, key_name):
    """
    If key_name is not None, key_pair will be named after key_name.
    Returns the ith default (aws_key_pair_name, key_pair_path).
    """
    if i == 0:
        key_pair_name = ("{}_{}".format(RAY, region)
                         if key_name is None else key_name)
        return (key_pair_name,
                os.path.expanduser("~/.ssh/{}.pem".format(key_pair_name)))

    key_pair_name = ("{}_{}_{}".format(RAY, i, region)
                     if key_name is None else key_name + "_key-{}".format(i))
    return (key_pair_name,
            os.path.expanduser("~/.ssh/{}.pem".format(key_pair_name)))


# Suppress excessive connection dropped logs from boto
logging.getLogger("botocore").setLevel(logging.WARNING)


def bootstrap_aws(config):
    # The head node needs to have an IAM role that allows it to create further
    # EC2 instances.
    config = _configure_iam_role(config)

    # Configure SSH access, using an existing key pair if possible.
    config = _configure_key_pair(config)

    # Pick a reasonable subnet if not specified by the user.
    config = _configure_subnet(config)

    # Cluster workers should be in a security group that permits traffic within
    # the group, and also SSH access from outside.
    config = _configure_security_group(config)

    # Provide a helpful message for missing AMI.
    _check_ami(config)

    return config


def _configure_iam_role(config):
    if "IamInstanceProfile" in config["head_node"]:
        return config

    profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)

    if profile is None:
        logger.info("_configure_iam_role: "
                    "Creating new instance profile {}".format(
                        DEFAULT_RAY_INSTANCE_PROFILE))
        client = _client("iam", config)
        client.create_instance_profile(
            InstanceProfileName=DEFAULT_RAY_INSTANCE_PROFILE)
        profile = _get_instance_profile(DEFAULT_RAY_INSTANCE_PROFILE, config)
        time.sleep(15)  # wait for propagation

    assert profile is not None, "Failed to create instance profile"

    if not profile.roles:
        role = _get_role(DEFAULT_RAY_IAM_ROLE, config)
        if role is None:
            logger.info("_configure_iam_role: "
                        "Creating new role {}".format(DEFAULT_RAY_IAM_ROLE))
            iam = _resource("iam", config)
            iam.create_role(
                RoleName=DEFAULT_RAY_IAM_ROLE,
                AssumeRolePolicyDocument=json.dumps({
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "ec2.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole",
                        },
                    ],
                }))
            role = _get_role(DEFAULT_RAY_IAM_ROLE, config)
            assert role is not None, "Failed to create role"
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2FullAccess")
        role.attach_policy(
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
        profile.add_role(RoleName=role.name)
        time.sleep(15)  # wait for propagation

    logger.info("_configure_iam_role: "
                "Role not specified for head node, using {}".format(
                    profile.arn))
    config["head_node"]["IamInstanceProfile"] = {"Arn": profile.arn}

    return config


def _configure_key_pair(config):
    if "ssh_private_key" in config["auth"]:
        assert "KeyName" in config["head_node"]
        assert "KeyName" in config["worker_nodes"]
        return config

    ec2 = _resource("ec2", config)

    # Try a few times to get or create a good key pair.
    MAX_NUM_KEYS = 30
    for i in range(MAX_NUM_KEYS):

        key_name = config["provider"].get("key_pair", {}).get("key_name")

        key_name, key_path = key_pair(i, config["provider"]["region"],
                                      key_name)
        key = _get_key(key_name, config)

        # Found a good key.
        if key and os.path.exists(key_path):
            break

        # We can safely create a new key.
        if not key and not os.path.exists(key_path):
            logger.info("_configure_key_pair: "
                        "Creating new key pair {}".format(key_name))
            key = ec2.create_key_pair(KeyName=key_name)

            # We need to make sure to _create_ the file with the right
            # permissions. In order to do that we need to change the default
            # os.open behavior to include the mode we want.
            with open(key_path, "w", opener=partial(os.open, mode=0o600)) as f:
                f.write(key.key_material)
            break

    if not key:
        raise ValueError(
            "No matching local key file for any of the key pairs in this "
            "account with ids from 0..{}. ".format(key_name) +
            "Consider deleting some unused keys pairs from your account.")

    assert os.path.exists(key_path), \
        "Private key file {} not found for {}".format(key_path, key_name)

    logger.info("_configure_key_pair: "
                "KeyName not specified for nodes, using {}".format(key_name))

    config["auth"]["ssh_private_key"] = key_path
    config["head_node"]["KeyName"] = key_name
    config["worker_nodes"]["KeyName"] = key_name

    return config


def _configure_subnet(config):
    ec2 = _resource("ec2", config)
    use_internal_ips = config["provider"].get("use_internal_ips", False)
    subnets = sorted(
        (s for s in ec2.subnets.all() if s.state == "available" and (
            use_internal_ips or s.map_public_ip_on_launch)),
        reverse=True,  # sort from Z-A
        key=lambda subnet: subnet.availability_zone)
    if not subnets:
        raise Exception(
            "No usable subnets found, try manually creating an instance in "
            "your specified region to populate the list of subnets "
            "and trying this again. Note that the subnet must map public IPs "
            "on instance launch unless you set 'use_internal_ips': True in "
            "the 'provider' config.")
    if "availability_zone" in config["provider"]:
        azs = config["provider"]["availability_zone"].split(",")
        subnets = [s for s in subnets if s.availability_zone in azs]
        if not subnets:
            raise Exception(
                "No usable subnets matching availability zone {} "
                "found. Choose a different availability zone or try "
                "manually creating an instance in your specified region "
                "to populate the list of subnets and trying this again.".
                format(config["provider"]["availability_zone"]))

    subnet_ids = [s.subnet_id for s in subnets]
    subnet_descr = [(s.subnet_id, s.availability_zone) for s in subnets]
    if "SubnetIds" not in config["head_node"]:
        config["head_node"]["SubnetIds"] = subnet_ids
        logger.info("_configure_subnet: "
                    "SubnetIds not specified for head node, using {}".format(
                        subnet_descr))

    if "SubnetIds" not in config["worker_nodes"]:
        config["worker_nodes"]["SubnetIds"] = subnet_ids
        logger.info("_configure_subnet: "
                    "SubnetId not specified for workers,"
                    " using {}".format(subnet_descr))

    return config


def _configure_security_group(config):
    if "SecurityGroupIds" in config["head_node"] and \
            "SecurityGroupIds" in config["worker_nodes"]:
        return config  # have user-defined groups

    security_groups = _upsert_security_groups(config)
    head_sg = security_groups["head"]
    workers_sg = security_groups["workers"]

    if "SecurityGroupIds" not in config["head_node"]:
        logger.info(
            "_configure_security_group: "
            "SecurityGroupIds not specified for head node, using {} ({})"
            .format(head_sg.group_name, head_sg.id))
        config["head_node"]["SecurityGroupIds"] = [head_sg.id]

    if "SecurityGroupIds" not in config["worker_nodes"]:
        logger.info("_configure_security_group: "
                    "SecurityGroupIds not specified for workers, using {} ({})"
                    .format(workers_sg.group_name, workers_sg.id))
        config["worker_nodes"]["SecurityGroupIds"] = [workers_sg.id]

    return config


def _check_ami(config):
    """Provide helpful message for missing ImageId for node configuration."""

    region = config["provider"]["region"]
    default_ami = DEFAULT_AMI.get(region)
    if not default_ami:
        # If we do not provide a default AMI for the given region, noop.
        return

    if config["head_node"].get("ImageId", "").lower() == "latest_dlami":
        config["head_node"]["ImageId"] = default_ami
        logger.info("_check_ami: head node ImageId is 'latest_dlami'. "
                    "Using '{ami_id}', which is the default {ami_name} "
                    "for your region ({region}).".format(
                        ami_id=default_ami,
                        ami_name=DEFAULT_AMI_NAME,
                        region=region))

    if config["worker_nodes"].get("ImageId", "").lower() == "latest_dlami":
        config["worker_nodes"]["ImageId"] = default_ami
        logger.info("_check_ami: worker nodes ImageId is 'latest_dlami'. "
                    "Using '{ami_id}', which is the default {ami_name} "
                    "for your region ({region}).".format(
                        ami_id=default_ami,
                        ami_name=DEFAULT_AMI_NAME,
                        region=region))


def _upsert_security_groups(config):
    vpc_group_name = SECURITY_GROUP_TEMPLATE.format(config["cluster_name"])
    rules_group_name = vpc_group_name + "-aux"

    security_groups = _get_or_create_vpc_security_groups(
        config, {vpc_group_name, rules_group_name}, vpc_group_name)
    _upsert_security_group_rules(config, security_groups, rules_group_name)

    return security_groups


def _get_or_create_vpc_security_groups(conf, all_group_names, vpc_group_name):
    worker_subnet_id = conf["worker_nodes"]["SubnetIds"][0]
    head_subnet_id = conf["head_node"]["SubnetIds"][0]

    worker_vpc_id = _get_vpc_id_or_die(conf, worker_subnet_id)
    head_vpc_id = _get_vpc_id_or_die(conf, head_subnet_id) \
        if head_subnet_id != worker_subnet_id else worker_vpc_id

    vpc_ids = [worker_vpc_id, head_vpc_id] \
        if head_vpc_id != worker_vpc_id else [worker_vpc_id]
    all_sgs = _get_security_groups(conf, vpc_ids, all_group_names)

    worker_sg = next((_ for _ in all_sgs if _.vpc_id == worker_vpc_id), None)
    if worker_sg is None:
        worker_sg = _create_security_group(conf, worker_vpc_id, vpc_group_name)
        all_sgs.append(worker_sg)

    head_sg = next((_ for _ in all_sgs if _.vpc_id == head_vpc_id), None)
    if head_sg is None:
        head_sg = _create_security_group(conf, head_vpc_id, vpc_group_name)

    return {"head": head_sg, "workers": worker_sg}


def _get_vpc_id_or_die(config, subnet_id):
    ec2 = _resource("ec2", config)
    subnet = list(
        ec2.subnets.filter(Filters=[{
            "Name": "subnet-id",
            "Values": [subnet_id]
        }]))
    assert len(subnet) == 1, "Subnet not found"
    subnet = subnet[0]
    return subnet.vpc_id


def _get_or_create_security_group(config, vpc_id, group_name):
    return _get_security_group(config, vpc_id, group_name) or \
           _create_security_group(config, vpc_id, group_name)


def _get_security_group(config, vpc_id, group_name):
    security_group = _get_security_groups(config, [vpc_id], [group_name])
    return None if not security_group else security_group[0]


def _get_security_groups(config, vpc_ids, group_names):
    ec2 = _resource("ec2", config)
    existing_groups = list(
        ec2.security_groups.filter(Filters=[{
            "Name": "vpc-id",
            "Values": vpc_ids
        }]))
    matching_groups = []
    for sg in existing_groups:
        if sg.group_name in group_names:
            matching_groups.append(sg)
    return matching_groups


def _create_security_group(config, vpc_id, group_name):
    client = _client("ec2", config)
    client.create_security_group(
        Description="Auto-created security group for Ray workers",
        GroupName=group_name,
        VpcId=vpc_id)
    security_group = _get_security_group(config, vpc_id, group_name)
    logger.info("_create_security_group: Created new security group {} ({})"
                .format(security_group.group_name, security_group.id))
    assert security_group, "Failed to create security group"
    return security_group


def _upsert_security_group_rules(config, security_groups, rules_group_name):
    sg_conf = config["provider"].get("security_group", {})
    head_rules = sg_conf.get("head", {}).get("inbound_rules", {})
    wrkr_rules = sg_conf.get("workers", {}).get("inbound_rules", {})

    head_sg = security_groups["head"]
    wrkr_sg = security_groups["workers"]

    if head_rules != wrkr_rules:
        if head_sg.id == wrkr_sg.id:
            wrkr_sg = _get_or_create_security_group(config, wrkr_sg.vpc_id,
                                                    rules_group_name)
            security_groups["workers"] = wrkr_sg

    sgids = {head_sg.id, wrkr_sg.id}

    head_channel_rules = _group_rules_by_channel(sg_conf, head_rules, head_sg)
    _update_inbound_rules(head_sg, head_channel_rules, sgids)
    wrkr_channel_rules = _group_rules_by_channel(sg_conf, wrkr_rules, wrkr_sg)
    _update_inbound_rules(wrkr_sg, wrkr_channel_rules, sgids)


def _group_rules_by_channel(sg_config, rules, security_group):
    """Group rules by network "channel" (FromPort, ToPort, IpProtocol)"""
    channel_rules = {}

    # add rules specified explicitly by channel
    for rule in rules:
        channels = _get_ipp_channels(rule)
        for channel in channels:
            _union_targets(rule, channel_rules.setdefault(channel, {}))

    # add default rules on all channels not already configured above
    default_rules = sg_config.get("inbound_rules", {})
    for rule in default_rules:
        channels = _get_ipp_channels(rule)
        for channel in channels:
            if not channel_rules.setdefault(channel, {}):
                _union_targets(rule, channel_rules.setdefault(channel, {}))

    # revoke all unspecified rules if one or more authorized rules exist
    if channel_rules:
        for ip_permission in security_group.ip_permissions:
            channel_rules.setdefault(_get_ipp_channel(ip_permission), {})

    return channel_rules


def _get_ipp_channels(ipp):
    if ipp.get("FromPort") or ipp.get("ToPort") or ipp.get("IpProtocol"):
        assert ipp.get("FromPort"), "Rule missing FromPort: {}".format(ipp)
        assert ipp.get("ToPort"), "Rule missing ToPort: {}".format(ipp)
        assert ipp.get("IpProtocol"), "Rule missing IpProtocol: {}".format(ipp)
        return [_get_ipp_channel(ipp)]
    return DEFAULT_INBOUND_CHANNELS


def _get_ipp_channel(ip_permission):
    return (ip_permission.get("FromPort", -1), ip_permission.get("ToPort", -1),
            ip_permission.get("IpProtocol"))


def _union_targets(rule, prior_channel_targets):
    targets = dict()
    for target_group in IPP_TARGETS_BY_GROUP:
        targets[target_group] = _get_ipp_targets(rule, target_group)

    targets["IpRanges"] = _canonicalize_cidr_ips(targets["IpRanges"])
    targets["Ipv6Ranges"] = _canonicalize_cidr_ips(targets["Ipv6Ranges"])

    for target_key, target_value in targets.items():
        prior_channel_targets[target_key] = prior_channel_targets.get(
            target_key, target_value).union(target_value)


def _get_ipp_targets(ipp, group):
    return {_[IPP_TARGETS_BY_GROUP[group]].lower() for _ in ipp.get(group, [])}


def _canonicalize_cidr_ips(cidr_ips):
    """
    returns a string set containing the canonical form of all input CIDR IPs
    this effectively deduplicates all equivalent, but unequal, input CIDR IPs
    """
    canonical_targets = set()
    # sort CIDR IPs to ensure deterministic deduplication
    # this primarily helps us write more precise stub-based boto3 unit tests
    for cidr_ip in sorted(cidr_ips):
        canonical_targets.add(str(ipaddress.ip_network(cidr_ip, False)))
    return canonical_targets


def _update_inbound_rules(sg, channel_rules, sgids):
    _update_security_group_ingress(sg, channel_rules, sgids, True)
    req_channel_rules = _get_required_channel_rules(sg, channel_rules, sgids)
    _update_security_group_ingress(sg, req_channel_rules, sgids, False)


def _update_security_group_ingress(sg, channel_rules, required_sgids, revoke):
    ipps_to_revoke = []
    channel_rules_to_add = {}

    for channel, original_sources in channel_rules.items():
        sources = copy.deepcopy(original_sources)
        sgids = sources.get("UserIdGroupPairs", set())
        if channel == ALL_TRAFFIC_CHANNEL:
            sources["UserIdGroupPairs"] = sgids.union(required_sgids)

        channel_targets = dict()
        for target_group in IPP_TARGETS_BY_GROUP:
            channel_targets[target_group] = set(sources.get(target_group, {}))

        src_to_add = channel_rules_to_add.setdefault(channel, channel_targets)

        sg_ipps = sg.ip_permissions
        for ipp in (_ for _ in sg_ipps if channel == _get_ipp_channel(_)):
            for group in IPP_TARGETS_BY_GROUP:
                _revoke_or_add(ipp, group, ipps_to_revoke, sources, src_to_add)

    if ipps_to_revoke and revoke:
        logger.info("_update_inbound_rules: "
                    "Revoking inbound rules from {} ({}) -- {}".format(
                        sg.group_name, sg.id, ipps_to_revoke))
        sg.revoke_ingress(IpPermissions=ipps_to_revoke)

    if any(_target_exists(_) for _ in channel_rules_to_add.values()):
        ipps_to_authorize = _create_ip_permissions(channel_rules_to_add)
        logger.info("_update_inbound_rules: "
                    "Adding inbound rules to {} ({}) -- {}".format(
                        sg.group_name, sg.id, ipps_to_authorize))
        sg.authorize_ingress(IpPermissions=ipps_to_authorize)


def _revoke_or_add(ipp, target_group, ipp_revokes, whitelist, rules_to_add):
    target = IPP_TARGETS_BY_GROUP[target_group]
    for ipp_targets in (_ for _ in ipp.get(target_group, []) if _.get(target)):
        ipp_revokes.append({
            "FromPort": ipp.get("FromPort", -1),
            "ToPort": ipp.get("ToPort", -1),
            "IpProtocol": ipp.get("IpProtocol"),
            target_group: [{target: ipp_targets[target]}]
        }) if ipp_targets[target] not in whitelist.get(target_group, {}) \
           else rules_to_add[target_group].remove(ipp_targets[target])


def _target_exists(targets):
    for target_group in IPP_TARGETS_BY_GROUP:
        if targets.get(target_group):
            return True
    return False


def _get_required_channel_rules(sg, channel_rules, sgids):
    required_channel_rules = {
        ALL_TRAFFIC_CHANNEL: {
            "UserIdGroupPairs": set(sgids)
        }
    }
    sg_ipp_channels = {(_get_ipp_channel(_) for _ in sg.ip_permissions)}
    for channel in DEFAULT_INBOUND_CHANNELS:
        rules_exist = bool(channel_rules) or channel in sg_ipp_channels
        if not rules_exist:
            default_targets = {"IpRanges": ["0.0.0.0/0"]}
            required_channel_rules[channel] = default_targets
    return required_channel_rules


def _create_ip_permissions(channel_rules):
    ip_permissions = []
    # sort channels for deterministic ordering of IP permission list
    # this primarily helps us write more precise boto3 unit test stubs
    for channel in sorted(channel_rules.keys()):
        targets = channel_rules[channel]
        if _target_exists(targets):
            _create_ip_permission(channel, targets, ip_permissions)

    return ip_permissions


def _create_ip_permission(channel, targets, ip_permissions):
    ipp = {
        "FromPort": channel[FROM_PORT],
        "ToPort": channel[TO_PORT],
        "IpProtocol": channel[IP_PROTOCOL],
    }
    for target_group in IPP_TARGETS_BY_GROUP:
        _append_ipp_targets(targets, ipp, target_group)

    ip_permissions.append(ipp)


def _append_ipp_targets(targets, ipp, target_group):
    src_key = IPP_TARGETS_BY_GROUP[target_group]
    targets_to_add = targets.get(target_group)
    if targets_to_add:
        # sort targets for deterministic ordering of IP permission targets list
        # this primarily helps us write more precise boto3 unit test stubs
        ipp[target_group] = [{src_key: _} for _ in sorted(targets_to_add)]


def _get_role(role_name, config):
    iam = _resource("iam", config)
    role = iam.Role(role_name)
    try:
        role.load()
        return role
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return None
        else:
            raise exc


def _get_instance_profile(profile_name, config):
    iam = _resource("iam", config)
    profile = iam.InstanceProfile(profile_name)
    try:
        profile.load()
        return profile
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return None
        else:
            raise exc


def _get_key(key_name, config):
    ec2 = _resource("ec2", config)
    for key in ec2.key_pairs.filter(Filters=[{
            "Name": "key-name",
            "Values": [key_name]
    }]):
        if key.name == key_name:
            return key


def _client(name, config):
    return _resource(name, config).meta.client


def _resource(name, config):
    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
    aws_credentials = config["provider"].get("aws_credentials", {})
    return RESOURCE_CACHE.setdefault(
        name,
        boto3.resource(
            name,
            config["provider"]["region"],
            config=boto_config,
            **aws_credentials))
