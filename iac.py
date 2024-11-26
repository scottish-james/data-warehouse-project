import boto3
import json
import configparser
import botocore.exceptions


# Load credentials and configuration
def load_config(file_path):
    """Load configuration from a file."""
    config = configparser.ConfigParser()
    try:
        config.read(file_path)
        if not config.sections():
            raise FileNotFoundError(f"Configuration file '{file_path}' is empty or missing.")
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}")
        raise


CONFIG_FILE = 'iac.cfg'
config = load_config(CONFIG_FILE)

AWS_CREDENTIALS = {
    "aws_access_key_id": config.get('AWS', 'KEY'),
    "aws_secret_access_key": config.get('AWS', 'SECRET'),
    "aws_session_token": config.get('AWS', 'SESSION_TOKEN', fallback=None),
    "region_name": config.get('AWS', 'REGION', fallback=None)
}


def initialize_client(service):
    """Initialize a boto3 client for a given service."""
    try:
        client = boto3.client(service, **AWS_CREDENTIALS)
        return client
    except botocore.exceptions.BotoCoreError as e:
        print(f"Error initializing client for service '{service}': {e}")
        raise


def user_exists(iam, user_name):
    """Check if the IAM user exists."""
    try:
        iam.get_user(UserName=user_name)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False
    except Exception as e:
        print(f"Error checking if user '{user_name}' exists: {e}")
        raise


def delete_iam_role(iam, role_name):
    """Delete an existing IAM role and detach associated policies."""
    try:
        policies = iam.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']
        for policy in policies:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])
        iam.delete_role(RoleName=role_name)
        print(f"IAM Role {role_name} deleted.")
    except iam.exceptions.NoSuchEntityException:
        print(f"IAM Role {role_name} does not exist.")
    except Exception as e:
        print(f"Error deleting IAM role '{role_name}': {e}")
        raise


def create_iam_role(iam, role_name):
    """Create an IAM role with Redshift access."""
    try:
        delete_iam_role(iam, role_name)

        assume_role_policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        })

        role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy)
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        print(f"IAM Role {role_name} created.")
        return role['Role']['Arn']
    except Exception as e:
        print(f"Error creating IAM role '{role_name}': {e}")
        raise


def create_redshift_cluster(redshift, config, iam_role_arn):
    """Create a Redshift cluster if it doesn't already exist."""
    try:
        cluster_id = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
        clusters = redshift.describe_clusters()['Clusters']

        if any(cluster['ClusterIdentifier'] == cluster_id for cluster in clusters):
            print(f"Redshift cluster '{cluster_id}' already exists.")
            return None

        redshift.create_cluster(
            ClusterType=config.get('DWH', 'DWH_CLUSTER_TYPE'),
            NodeType=config.get('DWH', 'DWH_NODE_TYPE'),
            NumberOfNodes=int(config.get('DWH', 'DWH_NUM_NODES')),
            DBName=config.get('DWH', 'DWH_DB'),
            ClusterIdentifier=cluster_id,
            MasterUsername=config.get('DWH', 'DWH_DB_USER'),
            MasterUserPassword=config.get('DWH', 'DWH_DB_PASSWORD'),
            Port=int(config.get('DWH', 'DWH_PORT')),
            IamRoles=[iam_role_arn],
            PubliclyAccessible=True
        )
        print(f"Redshift cluster '{cluster_id}' creation initiated.")
    except Exception as e:
        print(f"Error creating Redshift cluster: {e}")
        raise


def wait_for_cluster_availability(redshift, cluster_id):
    """Wait for the Redshift cluster to become available."""
    try:
        print("Waiting for Redshift cluster to be available...")
        while True:
            cluster = redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
            if cluster['ClusterStatus'] == 'available':
                endpoint = cluster['Endpoint']['Address']
                vpc_id = cluster['VpcId']
                print(f"Cluster '{cluster_id}' is now available at {endpoint}.")
                return endpoint, vpc_id
    except Exception as e:
        print(f"Error while waiting for cluster '{cluster_id}' to be available: {e}")
        raise


def export_config(file_path, endpoint, iam_role_arn, config):
    """Export the configuration to a file."""
    try:
        config_dwh = configparser.ConfigParser()

        config_dwh['CLUSTER'] = {
            'HOST': endpoint,
            'DB_NAME': config['DWH']['DWH_DB'],
            'DB_USER': config['DWH']['DWH_DB_USER'],
            'DB_PASSWORD': config['DWH']['DWH_DB_PASSWORD'],
            'DB_PORT': config['DWH']['DWH_PORT'],
            'REGION': config['AWS']['REGION'],
        }

        config_dwh['IAM_ROLE'] = {'ARN': iam_role_arn}
        config_dwh['S3'] = config['S3']

        with open(file_path, 'w') as config_file:
            config_dwh.write(config_file)
        print(f"Configuration exported to {file_path}.")
    except Exception as e:
        print(f"Error exporting configuration to '{file_path}': {e}")
        raise


def authorize_ingress(ec2, security_group_id):
    """Authorize ingress for the security group."""
    try:
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpProtocol='tcp',
            FromPort=5439,
            ToPort=5439,
            CidrIp='0.0.0.0/0'  # Replace with your IP range for security
        )
        print(f"Ingress rule added to security group {security_group_id} for port 5439.")
    except Exception as e:
        print(f"Error authorizing ingress for security group '{security_group_id}': {e}")
        raise


# Main Workflow
if __name__ == "__main__":
    try:
        iam = initialize_client('iam')
        redshift = initialize_client('redshift')

        # IAM Role and Redshift Cluster Setup
        ROLE_NAME = "myRedshiftRole"
        iam_role_arn = create_iam_role(iam, ROLE_NAME)

        cluster_id = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
        create_redshift_cluster(redshift, config, iam_role_arn)

        endpoint, vpc_id = wait_for_cluster_availability(redshift, cluster_id)

        # Export configuration
        export_config('dwh.cfg', endpoint, iam_role_arn, config)

        # EC2 Security Group Ingress Setup
        #ec2 = initialize_client('ec2')
        #cluster = redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
        #security_group_id = cluster['VpcSecurityGroups'][0]['VpcSecurityGroupId']
        #authorize_ingress(ec2, security_group_id)
    except Exception as e:
        print(f"Error in main workflow: {e}")
        raise