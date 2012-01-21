import logging

logger = logging.getLogger(__name__)


class ClusterEventManager(object):
    inst = None

    def __init__(self):
        self.messages = []


    @classmethod
    def get_events(cls):
        return cls.get_manager().messages

    @classmethod
    def get_manager(cls):
        if not ClusterEventManager.inst:
            ClusterEventManager.inst = ClusterEventManager()

        return ClusterEventManager.inst

    @classmethod
    def handle(cls, msg):
        logger.info(msg)
        cls.get_manager().messages.append(msg)
