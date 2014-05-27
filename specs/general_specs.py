import subprocess
import beanstalkc
import time

try:
    import unittest2 as unittest
except ImportError:
    import unittest

'''
'''

class BaseSpec(unittest.TestCase):
    BEANSTALKD_INSTANCE = None
    BEANSTALKD_HOST = 'localhost'
    BEANSTALKD_PORT = 11411

    def base_setup(self):
        print "Starting up the beanstalkd instance...",
        self.BEANSTALKD_INSTANCE = subprocess.Popen([
            'beanstalkd', '-p', str(self.BEANSTALKD_PORT)])
        time.sleep(5)
        print "done."

    def base_teardown(self):
        print "Shutting down the beanstalkd instance...",
        self.BEANSTALKD_INSTANCE.terminate()
        print "done."


class BasicSpec(BaseSpec):
    def setUp(self):
        self.base_setup()
        self.conn = beanstalkc.Connection(
            host='localhost', port=self.BEANSTALKD_PORT)

    def tearDown(self):
        self.base_teardown()

    def it_should_connect_to_beanstalkd(self):
        print self.conn
        raise Exception('producing output')
