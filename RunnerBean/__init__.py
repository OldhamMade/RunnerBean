import inspect
import logging

import beanstalkc
from resolver import resolve as resolve_import

from yaml import load
try:
    from yaml import CSafeLoader as Loader
except ImportError:
    from yaml import SafeLoader as Loader


class RunnerException(Exception):
    pass


class TimeoutReachedException(Exception):
    """Raised when a timeout is reached"""
    pass


class Runner(object):
    _server = None
    _tubes = ['default']
    _debug = False

    log = None

    def __init__(self, callable_,
                 host='0.0.0.0', port=11300, tubes=None, parse=True,
                 loglevel=logging.ERROR, logfile='runnerbean.log'):
        """Configure the runner/worker.

        >>> from RunnerBean import Runner
        >>> def foo(job): pass
        >>> worker = Runner(foo, parse=False)

        Options:

          - **callable_** -- the python callable to execute when a job
            is successfully reserved
          - **host** -- the host of the beanstalkd server (default: 0.0.0.0)
          - **port** -- the port which the beanstalkd instance is bound to
            (default: 11300)
          - **tubes** -- the tubes which the runner should bind to, either
            as a string or list (default: [default])
          - **parse** -- parse the job's body as YAML (default: True)
          - **loglevel** -- set the output logging level (default: ERROR)
          - **logfile** -- define the logfile (default: ./runnerbean.log)

        """

        self._host = host
        self._port = port

        if parse:
            self._process = self._call_with_args
        else:
            self._process = self._call_with_job

        logging.basicConfig(filename=logfile, level=loglevel)
        self.log = logging.getLogger(__name__)

        if isinstance(callable_, basestring):
            self.callable = self.resolve(callable_)

        elif hasattr(callable_, '__call__'):
            self.callable = callable_

        else:
            raise RunnerException('Unable to use "{0}" as a callable'.format(callable_))

        self._process_argspec()

        if isinstance(tubes, (tuple, list, set)):
            self._tubes = list(tubes)

        elif isinstance(tubes, basestring):
            self._tubes = [str(tubes)]

        elif tubes is not None:
            raise RunnerException('"tubes" argument is not a valid type; received {0}, expected one of (iterable, string, unicode)'.format(type(tubes)))


    def __call__(self, timeout=None, parse=True):
        self.run(timeout=timeout, parse=parse)


    def run(self, timeout=None):
        """Start the runner/worker. The runner/worker will run forever unless a
        ``timeout`` is specified. The runner/worker can also be called directly:

        >>> worker() # alternative to: worker.run()

        Options:

          - **timeout** -- reserve with a timeout of *N* seconds. Once the timeout is
            reached the runner will exit by raising a ``TimeoutReachedException``.

        """
        self.log.info('Reserving on tubes ("{0}"):'.format('", "'.join(self._tubes)))

        if timeout:
            self.log.info('Reserving with a timeout of {0}s'.format(timeout))

        while 1:
            if timeout is not None:
                job = self.server.reserve(timeout=timeout)
            else:
                job = self.server.reserve()

            if not job:
                self.log.info('Reserve timeout reached. Ending run.')
                raise TimeoutReachedException('Reserve timeout of {0}s reached'.format(timeout))

            self.log.info('[{0}] Processing job with a time-left of {1}:'.format(
                job.jid,
                job.stats()['time-left']
                ))

            if not job.body:
                self._bury(job, "job's body is empty")
                continue

            if self._process(job):
                self.log.info('[{0}] executed successfully'.format(job.jid))
                continue

            self._bury(job, 'job was not sucessfully processed')


    def __del__(self):
        try:
            if self.server:
                self.server.close()
        except:
            pass


    def _call_with_args(self, job):
        try:
            data = load(job.body, Loader=Loader)
        except Exception as e:
            self._bury(job, 'body was not parsable/valid YAML')
            return False

        if not data:
            self._bury(job, 'parsed body was empty')
            return False

        try:
            keys = set(data.keys())
            args = set(self._expected_args)
        except AttributeError:
            keys = args = set([])

        if not self._accepts_kwargs and args - keys:
            self._bury(job, 'callable is missing args ({0})'.format(', '.join(args - keys)))
            return False

        try:
            self.log.debug('[{0}] executing job with args ({1})'.format(
                job.jid,
                ', '.join(keys)
                ))
            self.log.debug('[{0}] executing job with values ({1})'.format(
                job.jid,
                data,  # ', '.join('='.format(k, v) for k, v in data.iteritems())
                ))

            if '__tube__' in self._all_args:
                data['__tube__'] = job.stats()['tube']

            if self.callable(**data):
                try:
                    job.delete()
                    job.stats()
                except beanstalkc.CommandFailed:
                    # job.stats() should raise if the job was deleted successfully
                    # so continue as expected
                    return True

                self._bury(job, 'job processed but could not be deleted')

        except Exception as e:
            self._bury(job, str(e), True)

        return False


    def _call_with_job(self, job):
        try:
            self.log.debug('[{0}] executing job with job body of: {1}'.format(job.jid, job.body))

            if '__tube__' in self._all_args:
                result = self.callable(job.body, __tube__=job.stats()['tube'])
            else:
                result = self.callable(job.body)

            if result is True:
                try:
                    job.delete()
                    job.stats()
                except beanstalkc.CommandFailed:
                    # job.stats() should raise if the job was deleted successfully
                    # so continue as expected
                    return True

                self._bury(job, 'job processed but could not be deleted')

        except Exception as e:
            self._bury(job, str(e), True)

        return False


    def _bury(self, job, message, exc_info=False):
        if not exc_info:
            self.log.warning('[{0}] {1}; burying for later inspection'.format(job.jid, message))
        else:
            self.log.exception('[{0}] {1}; burying for later inspection'.format(job.jid, message))

        job.bury()


    def _process_argspec(self):
        self._accepts_kwargs = False
        self._all_args = []
        self._expected_args = []
        self._preset_args = []

        self.log.debug('Parsing argspec for "{0}":'.format(self.callable.__name__))

        try:
            argspec = inspect.getargspec(self.callable)
        except TypeError:
            try:
                argspec = inspect.getargspec(self.callable.__call__)
            except TypeError:
                raise RunnerException('could not parse argspec for "{0}"'.format(self.callable.__name__))

        self._accepts_kwargs = argspec.keywords is not None

        if self._accepts_kwargs:
            self.log.debug('callable "{0}" accepts keyword args'.format(self.callable.__name__))

        # skip the "self" arg for class methods
        if inspect.isfunction(self.callable):
            self._all_args = argspec.args
        else:
            self._all_args = argspec.args[1:]

        self.log.debug('args "{0}" will accept: {1}'.format(
            self.callable.__name__,
            ', '.join(self._all_args)))

        try:
            self._expected_args = self._all_args[:-len(argspec.defaults)]
        except TypeError:
            self._expected_args = self._all_args

        if not self._expected_args and not self._accepts_kwargs:
            raise RunnerException('No arguments expected for "{0}"'.format(self.callable.__name__))

        self.log.debug('args "{0}" expects: {1}'.format(
            self.callable.__name__,
            ', '.join(self._expected_args)))

        try:
            self._preset_args = self._all_args[-len(argspec.defaults):]
        except TypeError:
            self._preset_args = self._all_args

        self.log.debug('args "{0}" accepts which have default values: {1}'.format(
            self.callable.__name__,
            ', '.join(self._preset_args)))


    def _get_connection(self):
        if not self._server:
            self._server = beanstalkc.Connection(host=self._host,
                                                 port=self._port)

            if self._tubes:
                self._server.ignore('default')
                for tube in self._tubes:
                    self._server.watch(tube)

        return self._server

    server = property(_get_connection)


    def resolve(self, callable):
        self.log.debug('Attempting to resolve "{0}"...'.format(callable))

        func = resolve_import(callable)

        if not func:
            raise ImportError('Could not import "{0}"'.format(callable))

        self.log.debug('Found callable "{0}"'.format(func.__name__))

        return func
