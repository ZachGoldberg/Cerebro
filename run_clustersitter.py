import sys
keys = ["/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-west-1.pem",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-west-2.pem",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-east-1.pem"]
sys.argv.append("--key-files=%s" % ','.join(keys))

from src.clustersitter.main import main
main()
