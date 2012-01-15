import os
from fabric.api import env
from fabric.operations import run, put, sudo
from fabric.state import output


class DeploymentRecipe(object):
    def __init__(self, hostname, username, keys,
                 post_callback=None, options=None):
        self.hostname = hostname
        self.username = username
        self.keys = keys
        self.post_callback = post_callback or id
        self.options = options

    def deploy(self):
        env.host_string = self.hostname
        env.user = self.username
        if isinstance(self.keys, list):
            env.key_filename = self.keys
        else:
            env.key_filename = [self.keys]

        # Silence Fabric's echoing of everything
        #for k, v in output.items():
        #    output[k] = False

        retval = self.run_deploy()
        self.post_callback()
        return retval


class MachineSitterRecipe(DeploymentRecipe):
    def run_deploy(self):
        print self.hostname, self.username, self.keys
        # Find the newest build to upload
        release_dir = os.getcwd() + "/releases/"
        filelist = os.listdir(release_dir)
        filelist = filter(lambda x: not os.path.isdir(x), filelist)
        newest = max(filelist, key=lambda x: os.stat(release_dir + x).st_mtime)

        try:
            # Now create the remote directory
            remote_dir = "/home/ubuntu/clustersitter/"
            run("mkdir -p %s" % remote_dir)

            # Upload the release
            put(release_dir + newest, remote_dir)

            # Extrat it and run the installer
            run("cd %s && tar -xzf %s%s" % (
                    remote_dir,
                    remote_dir,
                    newest))
            newdirname = newest.replace(".tgz", "")
            run("cd %s/%s && python2.7 install.py" % (
                    remote_dir,
                    newdirname))

            # Launch a machine sitter as root
            sudo("cd %s/%s && ./bin/machinesitter --daemon" % (
                    remote_dir,
                    newdirname))
        except:
            import traceback
            traceback.print_exc()
            return False

        return True


if __name__ == '__main__':
    a = MachineSitterRecipe(
        "jenkins.wifast.com",
        "ubuntu",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-west-1.pem")

    a.deploy()
