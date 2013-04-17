import os
import sys
import inspect
import logging

import beanstalkc
from resolver import resolve as resolve_import

try:
    from simplejson import loads as decode
except ImportError:
    from json import loads as decode



class Runner(object):
    _server = None
    _tubes = ['default']
    _debug = False

    log = None

    def __init__(self, callable,
                 host='0.0.0.0', port=11300, tubes=None,
                 loglevel=logging.ERROR, logfile='runnerbean.log'):

        self._host = host
        self._port = port

        logging.basicConfig(filename=logfile, level=loglevel)
        self.log = logging.getLogger(__name__)

        if isinstance(callable, basestring):
            self.callable = self.resolve(callable)

        elif hasattr(callable, '__call__'):
            self.callable = callable

        else:
            raise Exception('"%s" is not a callable' % self.callable.__name__)

        self._process_argspec()

        if not self._expected_args:
            raise Exception('no arguments expected for "%s"' % self.callable.__name__)

        if isinstance(tubes, (tuple, list, set)):
            self._tubes = list(tubes)

        elif isinstance(tubes, basestring):
            self._tubes = [str(tubes)]

        elif tubes is not None:
            raise Exception('"tubes" argument is not a valid type (iterable, string, unicode)')


    def __call__(self, timeout=None):
        self.run(timeout=timeout)


    def run(self, timeout=None):
        self.log.info('Reserving on tubes (%s):' % self._tubes)
        if timeout:
            self.log.info('  [setting timeout to %ds]' % timeout)

        while 1:

            if timeout:
                job = self.server.reserve(timeout=timeout)
            else:
                job = self.server.reserve()

            if not job:
                self.log.info('  Reserve timeout reached. Ending run.')
                raise SystemExit()

            self.log.info('  Processing job "%s":' % job.jid)

            if not job.body:
                self.log.warning('    [%s] body missing; burying job for later inspection' % job.jid)
                job.bury()
                continue

            try:
                data = decode(job.body)

            except Exception as e:
                self.log.warning('    [%s] body was not parsable JSON; burying for later inspection' % job.jid)
                self.log.warning('      %s' % e)
                job.bury()
                continue

            if not data:
                self.log.warning('    [%s] parsed body was empty; burying job for later inspection' % job.jid)
                job.bury()
                continue

            keys = set(data.keys())
            args = set(self._expected_args)

            if args - keys:
                self.log.warning('    [%s] is missing keys (%s); burying job for later inspection' % (job.jid, ', '.join(args - keys)))
                job.bury()
                continue

            try:
                self.log.debug('    [%s] executing job with args (%s)' % (job.jid, ', '.join(keys)))

                if self.callable(**data):
                    self.log.info('    [%s] executed successfully' % job.jid)
                    try:
                        job.delete()
                        job.stats()
                        self.log.debug('    [%s] could not be deleted' % jid)
                    except beanstalkc.CommandFailed:
                        continue

            except Exception as e:
                self.log.exception('    [%s] exception raised during execution; burying for later inspection' % job.jid)
            self.log.warning('    [%s] was not executed successfully; burying job for later inspection' % job.jid)
            job.bury()



    def __del__(self):
        try:
            if self.server:
                self.server.close()
        except:
            pass


    def _process_argspec(self):
        self._accepts_kwargs = False
        self._all_args = []
        self._expected_args = []
        self._preset_args = []

        self.log.debug('Parsing argspec for "%s":' % self.callable.__name__)

        try:
            argspec = inspect.getargspec(self.callable)
        except TypeError:
            try:
                argspec = inspect.getargspec(self.callable.__call__)
            except TypeError:
                raise Exception('could not parse argspec for "%s"' % self.callable.__name__)

        self._accepts_kwargs = argspec.keywords != None

        self.log.debug('  accepts keyword args: %s' % self._accepts_kwargs)

        # skip the "self" arg for class methods
        if inspect.isfunction(self.callable):
            self._all_args = argspec.args
        else:
            self._all_args = argspec.args[1:]

        self.log.debug('  all arguments accepted: %s' % self._all_args)

        try:
            self._expected_args = self._all_args[:-len(argspec.defaults)]
        except TypeError:
            self._expected_args = self._all_args

        self.log.debug('  expected arguments: %s' % self._expected_args)

        try:
            self._preset_args = self._all_args[-len(argspec.defaults):]
        except TypeError:
            self._preset_args = self._all_args

        self.log.debug('  args with default value: %s' % self._preset_args)


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


    def resolve(callable):
        self.log.debug('Attempting to resolve "%s"...' % callable)

        func = resolve_import(callable)

        if not func:
            raise ImportError()

        self.log.debug('Found "%s"' % func.__name__)

        return func
