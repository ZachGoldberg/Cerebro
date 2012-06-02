import logging
import os
import paramiko
import subprocess

logger = logging.getLogger(__name__)


class DeploymentRecipe(object):
    def __init__(self, hostname, username, keys,
                 post_callback=None, options=None,
                 given_logger=None, dns_hostname=None,
                 launch_location=None):
        self.hostname = hostname
        self.username = username
        self.dns_hostname = dns_hostname
        self.launch_location = launch_location
        self.keys = keys
        if not isinstance(self.keys, list):
            self.keys = [self.keys]

        self.post_callback = post_callback
        self.options = options
        self.logger = logger

        if given_logger:
            self.logger = given_logger

        self.connected = False
        self.connect()

    def connect(self):
        if self.connected:
            return

        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())

        for key in self.keys:
            try:
                self.client.connect(hostname=self.hostname,
                                    username=self.username,
                                    key_filename=key,
                                    timeout=10)
                self.sftp = self.client.open_sftp()
                self.connected = True
                self.logger.info("Connected to %s@%s with %s" % (
                        self.username, self.hostname, key
                        ))
                break
            except:
                self.logger.warn("Couldnt ssh into %s@%s with %s" % (
                        self.username, self.hostname, key
                        ))
                continue

    def run(self, cmd):
        self.logger.info("Running %s on %s" % (cmd, self.hostname))
        chan = self.client.get_transport().open_session()
        chan.exec_command(cmd)
        stdout = chan.makefile('rb', -1)
        stderr = chan.makefile_stderr('rb', -1)
        stdout_log = []
        while True:
            line = stdout.readline()
            if not line:
                break

            self.logger.info("Output from (%s): %s" % (cmd, line.strip()))
            stdout_log.append(line)
        self.logger.info("Stderr from (%s): %s" % (cmd, stderr.readlines()))
        status = chan.recv_exit_status()
        chan.close()
        return stdout_log, status

    def put(self, local, remote):
        if os.path.basename(local) != os.path.basename(remote):
            remote += os.path.basename(local)

        try:
            local_hash = subprocess.check_output(
                ['md5sum', local]).split(' ')[0]
        except subprocess.CalledProcessError:
            import traceback
            self.logger.error(traceback.format_exc())
            return

        self.logger.info("Calculated local hash: %s", local_hash)
        remote_hash, _ = self.run("md5sum %s" % remote)
        if remote_hash:
            remote_hash = remote_hash[0].split(' ')[0]
            self.logger.info("Calculated remote hash: %s", remote_hash)

        val = True
        if local_hash != remote_hash:
            self.logger.info(
                "Hashes differ.  Uploading %s to %s" % (local, remote))
            val = self.sftp.put(local, remote)
        else:
            self.logger.info("Hashes equal, not reuploading")

        return val

    def sudo(self, cmd):
        return self.run("sudo bash -c '%s'" % cmd)

    def deploy(self):
        if not self.connected:
            self.logger.warn(
                "Couldn't do deployment: SSH Not connected." +
                "For AWS machines this may be normal, as it can be up to " +
                "2 minutes after they spawn before we can login")

            return False

        self.logger.info("Begin deployment on %s" % self.hostname)
        # 2 tries
        try:
            retval = self.run_deploy()
        except:
            try:
                retval = self.run_deploy()
            except:
                import traceback
                self.logger.error(traceback.format_exc())
                return False

        self.logger.info("Calling post-deply callback for %s" % self.hostname)
        if self.post_callback:
            self.post_callback()

        return retval


class MachineSitterRecipe(DeploymentRecipe):
    def run_deploy(self):
        try:
            self.sudo("apt-get update")

            if self.dns_hostname:
                self.sudo("hostname %s" % self.dns_hostname)
            else:
                self.sudo("hostname %s" % self.hostname)

            self.sudo("apt-get install python-setuptools")
            self.sudo("easy_install -U cerebrod")

            # Clear any old ones
            self.sudo("pkill -9 -f sitter")

            # ensure we have the log location created
            log_option = ""
            if self.options.get('log_location'):
                self.sudo("mkdir -p %s" % self.options['log_location'])
                log_option = "--log-location=%s" % self.options['log_location']

            # Launch a machine sitter as root
            self.sudo("machinesitter --daemon %s" % (
                    log_option
                    ))
        except:
            import traceback
            traceback.print_exc()
            return False

        return True
