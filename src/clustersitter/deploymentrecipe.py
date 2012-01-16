import logging
import os
import paramiko

logger = logging.getLogger(__name__)


class DeploymentRecipe(object):
    def __init__(self, hostname, username, keys,
                 post_callback=None, options=None):
        self.hostname = hostname
        self.username = username

        self.keys = keys
        if not isinstance(self.keys, list):
            self.keys = [self.keys]

        self.post_callback = post_callback
        self.options = options
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())

        for key in self.keys:
            try:
                self.client.connect(hostname=self.hostname,
                                    username=self.username,
                                    key_filename=key)
                break
            except:
                continue

        self.sftp = self.client.open_sftp()

    def run(self, cmd):
        logger.info("Running %s on %s" % (cmd, self.hostname))
        output = self.client.exec_command(cmd)
        stdout = output[1]
        stderr = output[2]
        import pdb
        pdb.set_trace()
        while True:
            line = stdout.readline()
            if not line:
                break

            logger.info("Output from (%s): %s" % (cmd, line.strip()))
        logger.info(stderr.readlines())

    def put(self, local, remote):
        if os.path.basename(local) != os.path.basename(remote):
            remote += os.path.basename(local)

        logger.info("Uploading %s to %s" % (local, remote))
        return self.sftp.put(local, remote)

    def sudo(self, cmd):
        return self.run("sudo bash -c '%s'" % cmd)

    def deploy(self):
        retval = self.run_deploy()

        if self.post_callback:
            self.post_callback()

        return retval


class MachineSitterRecipe(DeploymentRecipe):
    def run_deploy(self):
        print self.hostname, self.username, self.keys
        # Find the newest build to upload
        release_dir = os.getcwd() + "/releases/"
        filelist = os.listdir(release_dir)
        filelist = filter(lambda x: not os.path.isdir(x), filelist)
        filelist = filter(lambda x: "tgz" in x, filelist)
        newest = max(filelist, key=lambda x: os.stat(release_dir + x).st_mtime)

        try:
            # Now create the remote directory
            remote_dir = "/home/ubuntu/clustersitter/"
            self.run("mkdir -p %s" % remote_dir)

            # Upload the release
            self.put(release_dir + newest, remote_dir)

            # Extrat it and run the installer
            self.run("cd %s && tar -xzf %s%s" % (
                    remote_dir,
                    remote_dir,
                    newest))

            # Needed for pycrypto
            # TODO - The actual release shouldn't need this,
            # but it does for some reason
            self.sudo("apt-get install -y python-dev")

            newdirname = newest.replace(".tgz", "")
            self.run("cd %s/%s && python2.7 install.py" % (
                    remote_dir,
                    newdirname))

            # Launch a machine sitter as root
            self.sudo("cd %s/%s && ./bin/machinesitter --daemon" % (
                    remote_dir,
                    newdirname))
        except:
            import traceback
            traceback.print_exc()
            return False

        return True


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logging.basicConfig()
    a = MachineSitterRecipe(
        'ec2-75-101-175-68.compute-1.amazonaws.com',
        "ubuntu",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-east-1.pem")

    a.sudo('cd /home/ubuntu/clustersitter//tasksitter-120-fcdd5c9-1326692829 && ./bin/machinesitter --daemon')
#    a.run("ls && sleep 1 && ls")
#    a.sudo("cd /etc && ls")
#    a.put("/home/zgoldberg/workspace/tasksitter/buildout.cfg",
#                "/home/ubuntu/")
#    a.deploy()
