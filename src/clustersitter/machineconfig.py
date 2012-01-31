class MachineConfig(object):
    def __init__(self, hostname, shared_fate_zone,
                 cpus, mem, bits=64, disk=0, data=None,
                 ip=None):
        self.hostname = hostname
        self.cpus = cpus
        self.mem = mem
        self.shared_fate_zone = shared_fate_zone
        self.bits = bits
        self.disk = disk
        self.login_name = None
        self.login_key = None
        self.data = data
        self.dns_name = None
        self.ip = ip

    def __str__(self):
        return self.hostname

    def __repr__(self):
        return str(self)
