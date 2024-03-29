#!/usr/bin/env python3

import boto3
import json
import configparser
import time
# import click


def get_config(cfg_file, required=[]):
    ''' Get the configuration from cfg_file and validate it.

    Validation is done against a list of tuples (section, key) that must
    be present in the config file.
    '''

    config = configparser.ConfigParser()
    config.read_file(open(cfg_file))

    # Validate that all required fields exist
    try:
        for entry in required:
            config.get(*entry)
    except (configparser.NoSectionError, KeyError):
        print("The config file ({}) must include an {} section with {}".format(
            config_file,
            *entry)
        )
        exit(0)

    return config


def connect_iam(config):
    ''' Connect to AWS IAM.

    Given a pointer to a configparser object connect to a AWS IAM endpoint
    and return the resource.
    '''
    print('Connecting to AWS IAM endpoint...')
    iam = boto3.resource(
            'iam',
            region_name='us-west-2',
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
        )
    return iam


def role_arn(iam, role_name):
    ''' Check if a role with a specific name is defined.

    If the role exists it's ARN is returned, if not False is returned.
    '''
    for role in iam.roles.all():
        if role.name == role_name:
            return role.arn
    return None


def create_role(iam, role_name):
    ''' Create a new IAM role and attach and S3ReadOnly policy to it.

    Returns the IAM role ARN.
    '''
    assume_role_policy_document = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    })

    try:
        role = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=assume_role_policy_document
        )
    except Exception as e:
        print("Error while creating role: '{}'\n{}".format(role_name, e))

    # Attach S3 Read Only Policy
    try:
        role.attach_policy(
                PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )
    except Exception as e:
        print("Error attaching S3 Read Only policy to role: '{}'\n{}".format(
            role.arn,
            e
        ))
    return role.arn


def connect_redshift(config):
    ''' Connect to an AWS Redshift endpoint.'''
    print('Connecting to AWS Redshift endpoint...')
    redshift = boto3.client(
            'redshift',
            region_name='us-west-2',
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET'),
    )
    return redshift


def get_cluster_status(redshift, cluster_name):
    ''' Get the status and properties of a Redshift cluster.

    Given it's name and a connection to a Amazon Redshift endpoint check for
    it's existence. If the cluster exists return a tuple with it's state and
    and array with it's properties.
    '''
    try:
        cluster_properties = redshift.describe_clusters(
                ClusterIdentifier=cluster_name
        )['Clusters'][0]
    except Exception:
        print("No Redshift cluster found with name: {}".format(cluster_name))
        return (None, None)
    return (cluster_properties['ClusterStatus'], cluster_properties)


def create_cluster(
        redshift,
        role_arn,
        cl_name,
        cl_db_name,
        cl_user,
        cl_password,
        cl_port=5439,
        cl_ntype='dc2.large',
        cl_nnodes=4,
):
    ''' Create a new redshift cluster.

    This call blocks until the new cluster is 'available'.
    '''
    # create the cluster
    print("Creating Redshift cluster: '{}'".format(cl_name))
    try:
        redshift.create_cluster(
                ClusterIdentifier=cl_name,
                DBName=cl_db_name,
                Port=int(cl_port),
                ClusterType='single-node' if int(cl_nnodes) == 1
                            else 'multi-node',
                NumberOfNodes=int(cl_nnodes),
                NodeType=cl_ntype,
                MasterUsername=cl_user,
                MasterUserPassword=cl_password,
                PubliclyAccessible=True,
                IamRoles=[role_arn],
        )
    except Exception as e:
        print("Failed to create Redshift cluster:\n{}".format(e))
        return None

    # wait for cluster to be ready
    print('Waiting for cluster to be available...', end='', flush=True)
    time.sleep(3)
    (status, properties) = get_cluster_status(redshift, cl_name)
    while not status or status.lower() != 'available':
        print('.', end='', flush=True)
        time.sleep(2)
        (status, properties) = get_cluster_status(redshift, cl_name)
    print(' Cluster available!')
    return properties


def write_user_config(infra_config, iam_role_arn, endpoint):
    ''' Write the dwh.cfg configuration file.

    Given the infrastructure configuration (configparse), an IAM role ARN
    and a Amazon Redshift endpoint create the configuration file necessary
    to run the create_tables and etl scripts.
    '''
    config = configparser.ConfigParser()
    config['CLUSTER'] = {
            'HOST': endpoint,
            'DB_NAME': infra_config.get('DWH', 'DWH_DB'),
            'DB_USER': infra_config.get('DWH', 'DWH_DB_USER'),
            'DB_PASSWORD': infra_config.get('DWH', 'DWH_DB_PASSWORD'),
            'DB_PORT': infra_config.get('DWH', 'DWH_PORT'),
    }
    config['IAM_ROLE'] = {
            'ARN': iam_role_arn,
    }
    config['S3'] = {
            'LOG_DATA': 's3://udacity-dend/log-data',
            'LOG_JSONPATH': 's3://udacity-dend/log_json_path.json',
            'SONG_DATA': 's3://udacity-dend/song-data',
    }
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
    return


if __name__ == '__main__':
    config_file = 'iac.cfg'
    required_config_fields = [
        ('AWS', 'KEY'),
        ('AWS', 'SECRET'),
        ('DWH', 'DWH_IAM_ROLE_NAME'),
        ('DWH', 'DWH_CLUSTER_IDENTIFIER'),
        ('DWH', 'DWH_DB'),
        ('DWH', 'DWH_DB_USER'),
        ('DWH', 'DWH_DB_PASSWORD'),
        ('DWH', 'DWH_PORT'),
        ('DWH', 'DWH_NODE_TYPE'),
        ('DWH', 'DWH_NUM_NODES'),
    ]

    config = get_config(config_file, required_config_fields)

    iam = connect_iam(config)

    # Check for existing role with configured name
    role_name = config.get('DWH', 'DWH_IAM_ROLE_NAME')
    role_arn = role_arn(iam, role_name)
    if role_arn:
        print("ARN for existing role '{}': {}".format(role_name, role_arn))
    else:
        print("Creating new role {}".format(role_name))
        role_arn = create_role(iam, role_name)

    # Cluster operations
    redshift = connect_redshift(config)
    cluster_name = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    (cluster_status, cluster_props) = get_cluster_status(
            redshift,
            cluster_name
    )
    if cluster_status:
        print("Redshift cluster {} exists:\n\tendpoint: {}".format(
            cluster_name,
            cluster_props['Endpoint']['Address']
        ))
    else:
        cluster_props = create_cluster(
            redshift,
            role_arn,
            cluster_name,
            config.get('DWH', 'DWH_DB'),
            config.get('DWH', 'DWH_DB_USER'),
            config.get('DWH', 'DWH_DB_PASSWORD'),
            config.get('DWH', 'DWH_PORT'),
            config.get('DWH', 'DWH_NODE_TYPE'),
            config.get('DWH', 'DWH_NUM_NODES'),
        )
        print("Redshift cluster {} created:\n\tendpoint: {}".format(
            cluster_name,
            cluster_props['Endpoint']['Address']
        ))
    print('Writing out user configuration.')
    write_user_config(config, role_arn, cluster_props['Endpoint']['Address'])
    exit
