# You can set AWS Keys here or in ENV
#os.putenv('AWS_ACCESS_KEY_ID', XXXXXX)
#os.putenv('AWS_SECRET_ACCESS_KEY', XXXXX)

# Location for cerebro logs
log_location = "/mnt/data/clustersitter"

# Keys used to login to machines
# TODO -- should define this again inside provider_config
keys = ["/opt/wifast/keys/WiFastAWSus-west-1.pem",
        "/opt/wifast/keys/WiFastAWSus-west-2.pem",
        "/opt/wifast/keys/WiFastAWSus-east-1.pem"]

# The user to login as in machines created by providers
# TODO -- this should be inside provider_config
login_user = "ubuntu"

# Define configuration for machine providers
provider_config = {
    'aws': {
        'us-east-1a': {
            '32b_image_id': 'ami-8b78afe2',
            '64b_image_id': 'ami-eb915a82',
            'key_name': 'WiFastAWS',
            'security_groups': ['clustersitter'],
            },
        'us-west-2a': {
            '32b_image_id': 'ami-d862efe8',
            '64b_image_id': 'ami-6c15985c',
            'key_name': 'WiFastAWSus-west-2',
            'security_groups': ['clustersitter'],
            },
        'us-west-1a': {
            '32b_image_id': 'ami-7dd48a38',
            '64b_image_id': 'ami-cb8ed48e',
            'key_name': 'WiFastAWSus-west-1',
            'security_groups': ['clustersitter'],
            }
        },
    }

for az in ['b', 'c', 'd']:
    provider_config['aws']['us-east-1%s' % az] = \
        provider_config['aws']['us-east-1a']

    provider_config['aws']['us-west-2%s' % az] = \
        provider_config['aws']['us-west-2a']

    provider_config['aws']['us-west-1%s' % az] = \
        provider_config['aws']['us-west-1a']

# DNS Provider configuration
dns_provider_config = {
    'class': 'dynect:Dynect',
    'customername': '',
    'username': '',
    'password': '',
    'default_domain': ''
    }
