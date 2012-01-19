import hashlib
import logging
import os
import paramiko
import time
import subprocess

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
        self.connected = False

        for key in self.keys:
            try:
                self.client.connect(hostname=self.hostname,
                                    username=self.username,
                                    key_filename=key,
                                    timeout=10)
                self.sftp = self.client.open_sftp()
                self.connected = True
                break
            except:
                logger.warn("Couldnt ssh into %s@%s with %s" % (
                        self.username, self.hostname, key
                        ))
                continue

    def run(self, cmd):
        logger.info("Running %s on %s" % (cmd, self.hostname))
        output = self.client.exec_command(cmd)
        stdout = output[1]
        stderr = output[2]
        stdout_log = []
        while True:
            line = stdout.readline()
            if not line:
                break

            logger.info("Output from (%s): %s" % (cmd, line.strip()))
            stdout_log.append(line)
        logger.info("Stderr from (%s): %s" % (cmd, stderr.readlines()))
        return stdout_log

    def put(self, local, remote):
        if os.path.basename(local) != os.path.basename(remote):
            remote += os.path.basename(local)

        try:
            local_hash = subprocess.check_output(['md5sum', local]).split(' ')[0]
        except subprocess.CalledProcessError:
            import traceback
            logger.error(traceback.format_exc())
            return

        logger.info("Calculated local hash: %s", local_hash)
        remote_hash = self.run("md5sum %s" % remote)
        if remote_hash:
            remote_hash = remote_hash[0].split(' ')[0]
            logger.info("Calculated remote hash: %s", remote_hash)
        if local_hash != remote_hash:
            logger.info("Uploading %s to %s" % (local, remote))
        else:
            logger.info("Hashes equal, not reuploading")

        return self.sftp.put(local, remote)

    def sudo(self, cmd):
        return self.run("sudo bash -c '%s'" % cmd)

    def deploy(self):
        if not self.connected:
            logger.warn(
                "Couldn't do deployment: SSH Not connected." +
                "For AWS machines this may be normal, as it can be up to" +
                "2 minutes after they spawn before we can login")

            return False

        logger.info("Begin deployment on %s" % self.hostname)
        # 2 tries
        try:
            retval = self.run_deploy()
        except:
            try:
                retval = self.run_deploy()
            except:
                import traceback
                logger.error(traceback.format_exc())
                return False

        logger.info("Calling post-deply callback for %s" % self.hostname)
        if self.post_callback:
            self.post_callback()

        return retval


class MachineSitterRecipe(DeploymentRecipe):
    def run_deploy(self):
        # Find the newest build to upload
        release_dir = os.getcwd() + "/releases/"
        filelist = os.listdir(release_dir)
        filelist = filter(lambda x: not os.path.isdir(x), filelist)
        filelist = filter(lambda x: "tgz" in x, filelist)
        if not filelist:
            logger.error("No releases found!")
            return False

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
            self.run("cd %s/%s && python2.7 install.py -N" % (
                    remote_dir,
                    newdirname))

            # Clear any old ones
            self.sudo("pkill -9 -f sitter")

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
        'ec2-23-20-7-213.compute-1.amazonaws.com',
        "ubuntu",
        "/home/zgoldberg/workspace/wifast/keys/WiFastAWSus-east-1.pem")

#    a.sudo('cd /home/ubuntu/clustersitter/tasksitter-123-241e19c-1326699447 && ./bin/machinesitter --daemon')
#    a.run("ls && sleep 1 && ls")
#    a.sudo("cd /etc && ls")
    a.put("/home/zgoldberg/workspace/tasksitter/buildout.cfg",
                "/home/ubuntu/")
#    a.deploy()
