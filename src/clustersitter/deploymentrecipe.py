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
        for k, v in output.items():
            output[k] = False

        self.run_deploy()

class MachineSitterRecipe(DeploymentRecipe):
    def run_deploy(self):
        keys = run("ls", pty=False)
        print keys



if __name__ == '__main__':
    a = MachineSitterRecipe(
        "jenkins.wifast.com",
        "ubuntu",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-west-1.pem")

    a.deploy()
