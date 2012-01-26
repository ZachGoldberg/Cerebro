from datetime import datetime
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
        if not cls.inst:
            cls.inst = cls()

        return cls.inst

    @classmethod
    def handle(cls, msg):
        logger.info(msg)
        now = datetime.now()
        cls.get_manager().messages.insert(0,
                                          "%s: %s" % (str(now),
                                                      msg))
