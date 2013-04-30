RunnerBean
==========

A simple tool for creating long-running Python workers listening for Beanstalk jobs.

Jobs can be posted as YAML or JSON objects, with keys mapping to the callable's
keyword arguments. Passing ``parse=False`` to the ``__init__`` method will
disable YAML parsing, and the callable will be provided a single string
argument containing the job's body.

Returning ``True`` from the callable will be seen as a success and will delete the
job from the queue. Returning ``False`` or ``None`` will be seen as a failure, and
the job will be buried for later inspection.

If listening on multiple tubes, add the argument ``__tubes__`` to the method to
receive the tube name when the callable is executed.

Usage::

    import logging
    from RunnerBean import Runner

    def print_message(recipient, message, __tube__):
        # accepts a job with the following structure:
        """
        message: Hello world!
        recipient: joe bloggs
        """
        print recipient, message

        print __tube__ #= 'messages'

        return True # this deletes the job from the tube

    if __name__ == '__main__':
        runner = Runner(print_message,
                        parse=True, # default; job body should be parsed as YAML
                        tubes="messages", # string or list of tubes to listen on
                        host='0.0.0.0', # beanstalkd host
                        port=11300, # beanstalkd port
                        loglevel=logging.DEBUG, # set log level, default: ERROR
                        logfile='runnerbean.log' # set the logfile
                        )
        runner.run()
