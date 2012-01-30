import sys
keys = ["/opt/wifast/keys/WiFastAWSus-west-1.pem",
        "/opt/wifast/keys/WiFastAWSus-west-2.pem",
        "/opt/wifast/keys/WiFastAWSus-east-1.pem"]
sys.argv.append("--key-files=%s" % ','.join(keys))

from src.clustersitter.main import main
main()
