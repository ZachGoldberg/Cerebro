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

    def serialize(self):
        return {
            'hostname': self.hostname,
            'cpus': self.cpus,
            'mem': self.mem,
            'shared_fate_zone': self.shared_fate_zone,
            'bits': self.bits,
            'disk': self.disk,
            'login_name': self.login_name,
            'login_key': self.login_key,
            'data': self.data,
            'dns_name': self.dns_name,
            'ip': self.ip,
        }

    @classmethod
    def deserialize(cls, data):
        obj = cls(hostname=data['hostname'],
                  shared_fate_zone=data['shared_fate_zone'],
                  cpus=data['cpus'],
                  mem=data['mem'],
                  bits=data['bits'],
                  disk=data['disk'],
                  data=data['data'],
                  ip=data['ip'], )
        obj.login_name = data['login_name']
        obj.login_key = data['login_key']
        obj.dns_name = data['dns_name']

        return obj
