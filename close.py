import boto3
import configparser

# Load configuration from dwh.cfg
config = configparser.ConfigParser()
config.read('iac.cfg')

AWS_KEY = config.get('AWS', 'KEY')
AWS_SECRET = config.get('AWS', 'SECRET')
AWS_SESSION_TOKEN = config.get('AWS', 'SESSION_TOKEN', fallback=None)
REGION = config.get('DEFAULT', 'REGION', fallback='us-west-2')

CLUSTER_IDENTIFIER = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER', fallback=None)
IAM_ROLE_NAME = config.get('IAM', 'ROLE_NAME', fallback='myRedshiftRole')


AWS_CREDENTIALS = {
    "aws_access_key_id": config.get('AWS', 'KEY'),
    "aws_secret_access_key": config.get('AWS', 'SECRET'),
    "aws_session_token": config.get('AWS', 'SESSION_TOKEN', fallback=None),
    "region_name": config.get('AWS', 'REGION', fallback=None)
}


def delete_redshift_cluster(cluster_identifier):
    """Delete the Redshift cluster."""
    redshift = boto3.client(
        'redshift',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=REGION
    )
    try:
        print(f"Deleting Redshift cluster: {cluster_identifier}")
        response = redshift.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )
        print(f"Cluster {cluster_identifier} deletion initiated.")
    except redshift.exceptions.ClusterNotFoundFault:
        print(f"Redshift cluster {cluster_identifier} does not exist.")
    except Exception as e:
        print(f"Error deleting Redshift cluster: {e}")


def delete_iam_role(role_name):
    """Delete the IAM role and its attached policies."""
    iam = boto3.client(
        'iam',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=REGION
    )
    try:
        print(f"Detaching policies from IAM Role: {role_name}")
        attached_policies = iam.list_attached_role_policies(RoleName=role_name)
        for policy in attached_policies['AttachedPolicies']:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])
            print(f"Detached policy {policy['PolicyArn']} from role {role_name}")

        print(f"Deleting IAM Role: {role_name}")
        iam.delete_role(RoleName=role_name)
        print(f"IAM Role {role_name} deleted successfully.")
    except iam.exceptions.NoSuchEntityException:
        print(f"IAM Role {role_name} does not exist.")
    except Exception as e:
        print(f"Error deleting IAM Role: {e}")


from botocore.exceptions import ClientError


def delete_s3_bucket(bucket_name, AWS_CREDENTIALS):
    try:
        # Initialize S3 client with provided credentials
        s3_client = boto3.client('s3', **AWS_CREDENTIALS)

        # List and delete all objects in the bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                print(f"Deleting object: {obj['Key']}")
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

        # Delete bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' deleted successfully.")
    except ClientError as e:
        print(f"Error deleting bucket: {e}")

def main():
    """Main function to delete resources."""
    # Delete the Redshift cluster
    if CLUSTER_IDENTIFIER:
        delete_redshift_cluster(CLUSTER_IDENTIFIER)
    else:
        print("Cluster identifier is not provided in the configuration.")

    # Delete the IAM Role
    if IAM_ROLE_NAME:
        delete_iam_role(IAM_ROLE_NAME)
    else:
        print("IAM Role name is not provided in the configuration.")

    bucket_name = 'my-jt-demo-bucket-12345'  # Replace with your bucket name
    delete_s3_bucket(bucket_name, AWS_CREDENTIALS)


if __name__ == "__main__":
    main()