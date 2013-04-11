RunnerBean
==========

A simple tool for creating long-running Python workers listening for Beanstalk jobs.

Jobs should be posted as a JSON object, with each key mapping to a callable's
keyword arguments.

Usage::

    import logging
    from RunnerBean import Runner

    def print_message(recipient, message):
        """accepts a job with the following structure:
        {
            'recipient': 'joe bloggs',
            'message': 'Hello world!',
        }
        """
        print recipient, message

    if __name__ == '__main__':
        runner = Runner(print_message,
                        tubes="messages", # string or list of tubes to listen on
                        host='0.0.0.0', # beanstalkd host
                        port=11300, # beanstalkd port
                        loglevel=logging.DEBUG, # set log level, default: ERROR
                        logfile='runnerbean.log' # set the logfile
                        )
        runner.run()
