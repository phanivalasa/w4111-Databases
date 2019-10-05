class CaptureException(Exception):

    def __init__(self, msg):
        self.err_msg = msg

    def __str__(self):
        err = ""
        err += " Error - {}".format(self.err_msg)
        return err