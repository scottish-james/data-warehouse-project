{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "initial_id",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:36:53.059285Z",
     "start_time": "2024-11-26T19:36:52.964905Z"
    },
    "collapsed": true,
    "jupyter": {
     "outputs_hidden": true
    }
   },
   "outputs": [],
   "source": [
    "import boto3\n",
    "import json\n",
    "import configparser"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "18c259b8ffc0e458",
   "metadata": {},
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "db7611112c47cb11",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:36:53.651156Z",
     "start_time": "2024-11-26T19:36:53.646440Z"
    }
   },
   "outputs": [],
   "source": [
    "# Load credentials and configuration\n",
    "config = configparser.ConfigParser()\n",
    "config.read('iac.cfg')\n",
    "\n",
    "AWS_CREDENTIALS = {\n",
    "    \"aws_access_key_id\": config.get('AWS', 'KEY'),\n",
    "    \"aws_secret_access_key\": config.get('AWS', 'SECRET'),\n",
    "    \"aws_session_token\": config.get('AWS', 'SESSION_TOKEN', fallback=None),\n",
    "    \"region_name\": config.get('AWS', 'REGION', fallback=None)\n",
    "}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "a45838ca22ff9f3d",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:36:54.204307Z",
     "start_time": "2024-11-26T19:36:54.201879Z"
    }
   },
   "outputs": [],
   "source": [
    "def initialize_client(service):\n",
    "    \"\"\"Initialize a boto3 client for a given service.\"\"\"\n",
    "    return boto3.client(service, **AWS_CREDENTIALS)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "37a353c694b7a568",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:36:54.698704Z",
     "start_time": "2024-11-26T19:36:54.614984Z"
    }
   },
   "outputs": [],
   "source": [
    "iam = initialize_client('iam')\n",
    "redshift = initialize_client('redshift')"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "bc7418a4183eadac",
   "metadata": {},
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "fd0c069f29a2572d",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:36:55.914243Z",
     "start_time": "2024-11-26T19:36:55.909412Z"
    }
   },
   "outputs": [],
   "source": [
    "def user_exists(iam, user_name):\n",
    "    \"\"\"Check if the IAM user exists.\"\"\"\n",
    "    try:\n",
    "        iam.get_user(UserName=user_name)\n",
    "        return True\n",
    "    except iam.exceptions.NoSuchEntityException:\n",
    "        return False\n",
    "\n",
    "def create_iam_user(iam, user_name):\n",
    "    \"\"\"Create an IAM user if it doesn't exist.\"\"\"\n",
    "    if not user_exists(iam, user_name):\n",
    "        iam.create_user(UserName=user_name)\n",
    "        print(f\"IAM User {user_name} created.\")\n",
    "    else:\n",
    "        print(f\"IAM User {user_name} already exists.\")\n",
    "              \n",
    "def delete_iam_role(iam, role_name):\n",
    "    \"\"\"Delete an existing IAM role and detach associated policies.\"\"\"\n",
    "    try:\n",
    "        policies = iam.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']\n",
    "        for policy in policies:\n",
    "            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy['PolicyArn'])\n",
    "        iam.delete_role(RoleName=role_name)\n",
    "        print(f\"IAM Role {role_name} deleted.\")\n",
    "    except iam.exceptions.NoSuchEntityException:\n",
    "        print(f\"IAM Role {role_name} does not exist.\")\n",
    "              \n",
    "def create_iam_role(iam, role_name):\n",
    "    \"\"\"Create an IAM role with Redshift access.\"\"\"\n",
    "    delete_iam_role(iam, role_name)\n",
    "    assume_role_policy = json.dumps({\n",
    "        \"Version\": \"2012-10-17\",\n",
    "        \"Statement\": [{\n",
    "            \"Effect\": \"Allow\",\n",
    "            \"Principal\": {\"Service\": \"redshift.amazonaws.com\"},\n",
    "            \"Action\": \"sts:AssumeRole\"\n",
    "        }]\n",
    "    })\n",
    "    role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy)\n",
    "    \n",
    "    print(f'Role: {role}')\n",
    "    \n",
    "    iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')\n",
    "    print(f\"IAM Role {role_name} created.\")\n",
    "    return role['Role']['Arn']\n",
    "\n",
    "def generate_aws_keys_for_user(iam, user_name):\n",
    "    \"\"\"Generate AWS access keys for a user and update the config file.\"\"\"\n",
    "    create_iam_user(iam, user_name)\n",
    "\n",
    "    # List existing access keys\n",
    "    existing_keys = iam.list_access_keys(UserName=user_name)['AccessKeyMetadata']\n",
    "    if len(existing_keys) >= 2:\n",
    "        for key in existing_keys:\n",
    "            iam.delete_access_key(UserName=user_name, AccessKeyId=key['AccessKeyId'])\n",
    "        print(f\"Deleted existing access keys for user {user_name}.\")\n",
    "\n",
    "    # Create new access key\n",
    "    keys = iam.create_access_key(UserName=user_name)['AccessKey']\n",
    "    print(f'list of keys: {keys}')\n",
    "    config.set('IAM', 'KEY', keys['AccessKeyId'])\n",
    "    config.set('IAM', 'SECRET', keys['SecretAccessKey'])\n",
    "    with open('dwh.cfg', 'w') as configfile:\n",
    "        config.write(configfile)\n",
    "    print(\"AWS keys generated and updated in configuration.\")\n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "90087e6ed524c321",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:36:57.640286Z",
     "start_time": "2024-11-26T19:36:56.567586Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "IAM Role myRedshiftRole deleted.\n",
      "Role: {'Role': {'Path': '/', 'RoleName': 'myRedshiftRole', 'RoleId': 'AROATOPBULTF4YQE2KHFJ', 'Arn': 'arn:aws:iam::237233265867:role/myRedshiftRole', 'CreateDate': datetime.datetime(2024, 11, 26, 19, 36, 57, tzinfo=tzutc()), 'AssumeRolePolicyDocument': {'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Principal': {'Service': 'redshift.amazonaws.com'}, 'Action': 'sts:AssumeRole'}]}}, 'ResponseMetadata': {'RequestId': 'f87a3e7c-c3bb-4875-ad0c-d7a5076d3b12', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Tue, 26 Nov 2024 19:36:56 GMT', 'x-amzn-requestid': 'f87a3e7c-c3bb-4875-ad0c-d7a5076d3b12', 'content-type': 'text/xml', 'content-length': '784'}, 'RetryAttempts': 0}}\n",
      "IAM Role myRedshiftRole created.\n",
      "arn:aws:iam::237233265867:role/myRedshiftRole\n"
     ]
    }
   ],
   "source": [
    "  # IAM Role and User Setup\n",
    "role_name = \"myRedshiftRole\"\n",
    "# user_name = \"myRedshiftUser\"\n",
    "iam_role_arn = create_iam_role(iam, role_name)\n",
    "\n",
    "print(iam_role_arn)\n",
    "\n",
    "#generate_aws_keys_for_user(iam, user_name)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "ec0b35ae02410c03",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:40:56.169725Z",
     "start_time": "2024-11-26T19:40:56.162274Z"
    }
   },
   "outputs": [],
   "source": [
    "def create_redshift_cluster(redshift, config, iam_role_arn):\n",
    "    \"\"\"Create a Redshift cluster if it doesn't already exist.\"\"\"\n",
    "    cluster_id = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')\n",
    "    clusters = redshift.describe_clusters()['Clusters']\n",
    "    if any(cluster['ClusterIdentifier'] == cluster_id for cluster in clusters):\n",
    "        print(f\"Redshift cluster '{cluster_id}' already exists.\")\n",
    "        return None\n",
    "\n",
    "    redshift.create_cluster(\n",
    "        ClusterType=config.get('DWH', 'DWH_CLUSTER_TYPE'),\n",
    "        NodeType=config.get('DWH', 'DWH_NODE_TYPE'),\n",
    "        NumberOfNodes=int(config.get('DWH', 'DWH_NUM_NODES')),\n",
    "        DBName=config.get('DWH', 'DWH_DB'),\n",
    "        ClusterIdentifier=cluster_id,\n",
    "        MasterUsername=config.get('DWH', 'DWH_DB_USER'),\n",
    "        MasterUserPassword=config.get('DWH', 'DWH_DB_PASSWORD'),\n",
    "        Port = int(config.get('DWH', 'DWH_PORT')),\n",
    "        IamRoles=[iam_role_arn],\n",
    "        PubliclyAccessible=True  # Ensure the cluster is accessible over TCP/IP\n",
    "    )\n",
    "    print(f\"Redshift cluster '{cluster_id}' creation initiated.\")\n",
    "\n",
    "\n",
    "def wait_for_cluster_availability(redshift, cluster_id):\n",
    "    \"\"\"Wait for the Redshift cluster to become available.\"\"\"\n",
    "    print(\"Waiting for Redshift cluster to be available...\")\n",
    "    while True:\n",
    "        cluster = redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]\n",
    "        if cluster['ClusterStatus'] == 'available':\n",
    "            endpoint = cluster['Endpoint']['Address']\n",
    "            vpc_id = cluster['VpcId']\n",
    "            print(f\"Cluster '{cluster_id}' is now available at {endpoint}.\")\n",
    "            return endpoint, vpc_id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "3e4ba46f031315c4",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:40:57.256042Z",
     "start_time": "2024-11-26T19:40:57.253694Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "dc2.large\n"
     ]
    }
   ],
   "source": [
    "print(config.get('DWH', 'DWH_NODE_TYPE'))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "f8a2491632b49ad2",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:42:33.375069Z",
     "start_time": "2024-11-26T19:40:57.877190Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Redshift cluster 'redshift-cluster' creation initiated.\n",
      "Waiting for Redshift cluster to be available...\n",
      "Cluster 'redshift-cluster' is now available at redshift-cluster.cvkdtpgzlapv.us-west-2.redshift.amazonaws.com.\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Redshift Cluster Setup\n",
    "cluster_id = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')\n",
    "create_redshift_cluster(redshift, config, iam_role_arn)\n",
    "endpoint, vpc_id = wait_for_cluster_availability(redshift, cluster_id)\n",
    "\n",
    "# Export dwh configuration file\n",
    "\n",
    "config_dwh = configparser.ConfigParser()\n",
    "\n",
    "config_dwh['CLUSTER'] = {\n",
    "    'HOST' : endpoint,\n",
    "    'DB_NAME' : config['DWH']['DWH_DB'],\n",
    "    'DB_USER' : config['DWH']['DWH_DB_USER'],\n",
    "    'DB_PASSWORD' : config['DWH']['DWH_DB_PASSWORD'],\n",
    "    'DB_PORT' : config['DWH']['DWH_PORT']\n",
    "}\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "5f5455fcc10de090",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:42:43.936447Z",
     "start_time": "2024-11-26T19:42:43.933522Z"
    }
   },
   "outputs": [],
   "source": [
    "config_dwh['CLUSTER'] = {\n",
    "    'HOST' : endpoint,\n",
    "    'DB_NAME' : config['DWH']['DWH_DB'],\n",
    "    'DB_USER' : config['DWH']['DWH_DB_USER'],\n",
    "    'DB_PASSWORD' : config['DWH']['DWH_DB_PASSWORD'],\n",
    "    'DB_PORT' : config['DWH']['DWH_PORT']\n",
    "}\n",
    "\n",
    "config_dwh['IAM_ROLE'] = {'ARN' : iam_role_arn}\n",
    "config_dwh['S3'] = config['S3']\n",
    "\n",
    "with open('dwh.cfg', 'w') as config_dwh_file:\n",
    "    config_dwh.write(config_dwh_file)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "ca3c1e57a4b1814c",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:42:46.850669Z",
     "start_time": "2024-11-26T19:42:45.871163Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Security Group ID: sg-0166d14f74e0d7e0f\n"
     ]
    }
   ],
   "source": [
    "redshift = initialize_client('redshift')\n",
    "cluster_id = 'redshift-cluster'\n",
    "\n",
    "# Get the cluster's details\n",
    "response = redshift.describe_clusters(ClusterIdentifier=cluster_id)\n",
    "cluster = response['Clusters'][0]\n",
    "\n",
    "# Get the security group ID\n",
    "security_group_id = cluster['VpcSecurityGroups'][0]['VpcSecurityGroupId']\n",
    "print(f\"Security Group ID: {security_group_id}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "3945a8850b466990",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:42:50.312062Z",
     "start_time": "2024-11-26T19:42:49.279020Z"
    }
   },
   "outputs": [
    {
     "ename": "ClientError",
     "evalue": "An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule \"peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW\" already exists",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mClientError\u001b[0m                               Traceback (most recent call last)",
      "Cell \u001b[0;32mIn[19], line 3\u001b[0m\n\u001b[1;32m      1\u001b[0m ec2 \u001b[38;5;241m=\u001b[39m initialize_client(\u001b[38;5;124m'\u001b[39m\u001b[38;5;124mec2\u001b[39m\u001b[38;5;124m'\u001b[39m)\n\u001b[0;32m----> 3\u001b[0m \u001b[43mec2\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43mauthorize_security_group_ingress\u001b[49m\u001b[43m(\u001b[49m\n\u001b[1;32m      4\u001b[0m \u001b[43m    \u001b[49m\u001b[43mGroupId\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[43msecurity_group_id\u001b[49m\u001b[43m,\u001b[49m\n\u001b[1;32m      5\u001b[0m \u001b[43m    \u001b[49m\u001b[43mIpProtocol\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[38;5;124;43mtcp\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[43m,\u001b[49m\n\u001b[1;32m      6\u001b[0m \u001b[43m    \u001b[49m\u001b[43mFromPort\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[38;5;241;43m5439\u001b[39;49m\u001b[43m,\u001b[49m\n\u001b[1;32m      7\u001b[0m \u001b[43m    \u001b[49m\u001b[43mToPort\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[38;5;241;43m5439\u001b[39;49m\u001b[43m,\u001b[49m\n\u001b[1;32m      8\u001b[0m \u001b[43m    \u001b[49m\u001b[43mCidrIp\u001b[49m\u001b[38;5;241;43m=\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[38;5;124;43m0.0.0.0/0\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[43m  \u001b[49m\u001b[38;5;66;43;03m# Replace with your IP range\u001b[39;49;00m\n\u001b[1;32m      9\u001b[0m \u001b[43m)\u001b[49m\n\u001b[1;32m     10\u001b[0m \u001b[38;5;28mprint\u001b[39m(\u001b[38;5;124mf\u001b[39m\u001b[38;5;124m\"\u001b[39m\u001b[38;5;124mIngress rule added to security group \u001b[39m\u001b[38;5;132;01m{\u001b[39;00msecurity_group_id\u001b[38;5;132;01m}\u001b[39;00m\u001b[38;5;124m for port 5439.\u001b[39m\u001b[38;5;124m\"\u001b[39m)\n",
      "File \u001b[0;32m/opt/anaconda3/lib/python3.12/site-packages/botocore/client.py:569\u001b[0m, in \u001b[0;36mClientCreator._create_api_method.<locals>._api_call\u001b[0;34m(self, *args, **kwargs)\u001b[0m\n\u001b[1;32m    565\u001b[0m     \u001b[38;5;28;01mraise\u001b[39;00m \u001b[38;5;167;01mTypeError\u001b[39;00m(\n\u001b[1;32m    566\u001b[0m         \u001b[38;5;124mf\u001b[39m\u001b[38;5;124m\"\u001b[39m\u001b[38;5;132;01m{\u001b[39;00mpy_operation_name\u001b[38;5;132;01m}\u001b[39;00m\u001b[38;5;124m() only accepts keyword arguments.\u001b[39m\u001b[38;5;124m\"\u001b[39m\n\u001b[1;32m    567\u001b[0m     )\n\u001b[1;32m    568\u001b[0m \u001b[38;5;66;03m# The \"self\" in this scope is referring to the BaseClient.\u001b[39;00m\n\u001b[0;32m--> 569\u001b[0m \u001b[38;5;28;01mreturn\u001b[39;00m \u001b[38;5;28;43mself\u001b[39;49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43m_make_api_call\u001b[49m\u001b[43m(\u001b[49m\u001b[43moperation_name\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[43mkwargs\u001b[49m\u001b[43m)\u001b[49m\n",
      "File \u001b[0;32m/opt/anaconda3/lib/python3.12/site-packages/botocore/client.py:1023\u001b[0m, in \u001b[0;36mBaseClient._make_api_call\u001b[0;34m(self, operation_name, api_params)\u001b[0m\n\u001b[1;32m   1019\u001b[0m     error_code \u001b[38;5;241m=\u001b[39m error_info\u001b[38;5;241m.\u001b[39mget(\u001b[38;5;124m\"\u001b[39m\u001b[38;5;124mQueryErrorCode\u001b[39m\u001b[38;5;124m\"\u001b[39m) \u001b[38;5;129;01mor\u001b[39;00m error_info\u001b[38;5;241m.\u001b[39mget(\n\u001b[1;32m   1020\u001b[0m         \u001b[38;5;124m\"\u001b[39m\u001b[38;5;124mCode\u001b[39m\u001b[38;5;124m\"\u001b[39m\n\u001b[1;32m   1021\u001b[0m     )\n\u001b[1;32m   1022\u001b[0m     error_class \u001b[38;5;241m=\u001b[39m \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39mexceptions\u001b[38;5;241m.\u001b[39mfrom_code(error_code)\n\u001b[0;32m-> 1023\u001b[0m     \u001b[38;5;28;01mraise\u001b[39;00m error_class(parsed_response, operation_name)\n\u001b[1;32m   1024\u001b[0m \u001b[38;5;28;01melse\u001b[39;00m:\n\u001b[1;32m   1025\u001b[0m     \u001b[38;5;28;01mreturn\u001b[39;00m parsed_response\n",
      "\u001b[0;31mClientError\u001b[0m: An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule \"peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW\" already exists"
     ]
    }
   ],
   "source": [
    "ec2 = initialize_client('ec2')\n",
    "\n",
    "ec2.authorize_security_group_ingress(\n",
    "    GroupId=security_group_id,\n",
    "    IpProtocol='tcp',\n",
    "    FromPort=5439,\n",
    "    ToPort=5439,\n",
    "    CidrIp='0.0.0.0/0'  # Replace with your IP range\n",
    ")\n",
    "print(f\"Ingress rule added to security group {security_group_id} for port 5439.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "544d861b8155053b",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T19:59:53.300114Z",
     "start_time": "2024-11-26T19:59:47.139607Z"
    }
   },
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/var/folders/0x/sytm1cbx2jn3ngxj6wfwv79m0000gn/T/ipykernel_67430/1444407225.py:15: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.\n",
      "  df = pd.read_sql_query(query, conn)\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>artist_id</th>\n",
       "      <th>name</th>\n",
       "      <th>location</th>\n",
       "      <th>latitude</th>\n",
       "      <th>longitude</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>AR00B1I1187FB433EB</td>\n",
       "      <td>Eagle-Eye Cherry</td>\n",
       "      <td>Stockholm, Sweden</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>AR00DG71187B9B7FCB</td>\n",
       "      <td>Basslovers United</td>\n",
       "      <td></td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>AR00FVC1187FB5BE3E</td>\n",
       "      <td>Panda</td>\n",
       "      <td>Monterrey, NL, México</td>\n",
       "      <td>25.6708</td>\n",
       "      <td>-100.3100</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>AR00JIO1187B9A5A15</td>\n",
       "      <td>Saigon</td>\n",
       "      <td>Brooklyn</td>\n",
       "      <td>40.6551</td>\n",
       "      <td>-73.9489</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>AR00LNI1187FB444A5</td>\n",
       "      <td>Bruce BecVar</td>\n",
       "      <td></td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>AR00MQ31187B9ACD8F</td>\n",
       "      <td>Chris Carrier</td>\n",
       "      <td></td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>AR00TGQ1187B994F29</td>\n",
       "      <td>Paula Toller</td>\n",
       "      <td></td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>AR00Y9I1187B999412</td>\n",
       "      <td>Akercocke</td>\n",
       "      <td></td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>AR00YYQ1187FB504DC</td>\n",
       "      <td>God Is My Co-Pilot</td>\n",
       "      <td>New York, NY</td>\n",
       "      <td>40.7146</td>\n",
       "      <td>-74.0071</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>9</th>\n",
       "      <td>AR016P51187B98E398</td>\n",
       "      <td>Indian Ropeman</td>\n",
       "      <td></td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "            artist_id                name               location  latitude  \\\n",
       "0  AR00B1I1187FB433EB    Eagle-Eye Cherry      Stockholm, Sweden       NaN   \n",
       "1  AR00DG71187B9B7FCB   Basslovers United                              NaN   \n",
       "2  AR00FVC1187FB5BE3E               Panda  Monterrey, NL, México   25.6708   \n",
       "3  AR00JIO1187B9A5A15              Saigon               Brooklyn   40.6551   \n",
       "4  AR00LNI1187FB444A5        Bruce BecVar                              NaN   \n",
       "5  AR00MQ31187B9ACD8F       Chris Carrier                              NaN   \n",
       "6  AR00TGQ1187B994F29        Paula Toller                              NaN   \n",
       "7  AR00Y9I1187B999412           Akercocke                              NaN   \n",
       "8  AR00YYQ1187FB504DC  God Is My Co-Pilot           New York, NY   40.7146   \n",
       "9  AR016P51187B98E398      Indian Ropeman                              NaN   \n",
       "\n",
       "   longitude  \n",
       "0        NaN  \n",
       "1        NaN  \n",
       "2  -100.3100  \n",
       "3   -73.9489  \n",
       "4        NaN  \n",
       "5        NaN  \n",
       "6        NaN  \n",
       "7        NaN  \n",
       "8   -74.0071  \n",
       "9        NaN  "
      ]
     },
     "execution_count": 25,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import psycopg2\n",
    "# Read DWH config\n",
    "config_conn = configparser.ConfigParser()\n",
    "config_conn.read('dwh.cfg')\n",
    "# Run a query\n",
    "\n",
    "query = ('''\n",
    "SELECT * \n",
    "FROM artists\n",
    "LIMIT 10\n",
    "''')\n",
    "\n",
    "conn = psycopg2.connect(\"host={} dbname={} user={} password={} port={}\".format(*config_conn['CLUSTER'].values()))\n",
    "df = pd.read_sql_query(query, conn)\n",
    "conn.close()\n",
    "\n",
    "df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "bd846df0619d6407",
   "metadata": {
    "ExecuteTime": {
     "end_time": "2024-11-26T20:28:24.299232Z",
     "start_time": "2024-11-26T20:28:23.174140Z"
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Cluster identifier is not provided in the configuration.\n",
      "Detaching policies from IAM Role: myRedshiftRole\n",
      "IAM Role myRedshiftRole does not exist.\n",
      "Error deleting bucket: An error occurred (NoSuchBucket) when calling the ListObjectsV2 operation: The specified bucket does not exist\n"
     ]
    }
   ],
   "source": [
    "%run close.py"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c7d31fa619b9f510",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
