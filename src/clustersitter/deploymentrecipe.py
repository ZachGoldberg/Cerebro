import os
from fabric.api import env
from fabric.operations import run, put
from fabric.state import output


class DeploymentRecipe(object):
    def __init__(self, hostname, username, keyfile):
        self.hostname = hostname
        self.username = username
        self.keyfile = keyfile

    def deploy(self):
        env.host_string = self.hostname
        env.user = self.username
        env.key_filename = [self.keyfile]

        # Silence Fabric's echoing of everything
        #for k, v in output.items():
        #    output[k] = False

        self.run_deploy()

class MachineSitterRecipe(DeploymentRecipe):
    def run_deploy(self):
        # Find the newest build to upload
        release_dir = os.getcwd() + "/releases/"
        filelist = os.listdir(release_dir)
        filelist = filter(lambda x: not os.path.isdir(x), filelist)
        print filelist
        newest = max(filelist, key=lambda x: os.stat(release_dir + x).st_mtime)
        print newest
        remote_dir = "/home/ubuntu/clustersitter/"
        print run("mkdir -p %s" % remote_dir)
        print put(release_dir + newest, remote_dir)
        print run("cd %s && tar -xzf %s%s" % (
                remote_dir,
                remote_dir,
                newest))
        newdirname = newest.replace(".tgz", "")
        print run("cd %s/%s && python2.7 install.py" % (
                remote_dir,
                newdirname))


        # Upload the build file
        print keys



if __name__ == '__main__':
    a = MachineSitterRecipe(
        "jenkins.wifast.com",
        "ubuntu",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-west-1.pem")

    a.deploy()
