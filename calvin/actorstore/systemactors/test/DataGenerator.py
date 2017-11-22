from calvin.actor.actor import Actor, manage, condition
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

class DataGenerator(Actor):
    """
    Sends a token with determined size. Just to test bandwidth between actors.
    Input:
      trigger : Any token
    Output:
      data : Output data
    """

    @manage(['size'])
    def init(self, size):
        self.size = size

    @condition(['trigger'], ['data'])
    def send_data(self, trigger):
        data = "x"*self.size
        return (data, )

    action_priority = (send_data, )

