RunnerBean
==========

A simple tool for creating long-running Python workers listening for Beanstalk jobs.

Jobs should be posted as a JSON object, with each key mapping to a callable's 
keyword arguments.

Usage::

    from RunnerBean import Runner

    def print_message(message):
        """accepts a job with the following structure: 
        {
            'message': 'Hello world!',
        }
        """
        print message

    if __name__ == '__main__':
        runner = Runner(print_message, tubes="messages")
        runner.run()
    
