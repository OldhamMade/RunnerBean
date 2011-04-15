import os
import sys
import inspect

import beanstalkc
from resolver import resolve as resolve_import

try:
    from simplejson import loads as decrypt
except ImportError:
    from json import loads as decrypt


class Runner(object):
    _server = None
    _tubes = ['default']
    _debug = False

    def __init__(self, callable, host='0.0.0.0', port=11300, tubes=None, debug=False):
        if isinstance(callable, basestring):
            self.callable = resolve(callable)

        elif hasattr(callable, '__call__'):
            self.callable = callable

        else:
            raise Exception('"callable" is not a callable')

        self._process_argspec()

        if not self._expected_args:
            raise Exception('no arguments expected for callable')

        self._host = host
        self._port = port

        if isinstance(tubes, (tuple, list, set)):
            self._tubes = list(tubes)

        elif isinstance(tubes, basestring):
            self._tubes = [str(tubes)]

        elif tubes is not None:
            raise Exception('"tubes" is not a valid type (iterable, string, unicode)')

        self._debug = debug


    def __call__(self, timeout=None):
        self.run(timeout=timeout)


    def run(self, timeout=None):
        while 1:
            if self._debug:
                print 'Reserving',
                if timeout:
                    print 'with timeout of %ds' % timeout,
                print '...'

            if timeout:
                job = self.server.reserve(timeout=timeout)
            else:
                job = self.server.reserve()

            if not job:
                if self._debug:
                    print 'No job found; skipping'
                continue

            if self._debug:
                print 'Found job: %s' % job.jid

            if not job.body:
                if self._debug:
                    print '  - burying job due to missing body'
                job.bury()
                continue

            try:
                data = decrypt(job.body)
            except Exception as e:
                if self._debug:
                    print '  - burying job due to malformed body (not parsable as json)'
                    print e
                job.bury()
                continue

            print dir(data)

            if not data or set(dict(data).keys()) != self._expected_args:
                if self._debug:
                    print '  - burying job due to missing keys:',
                    print 'expected (%s),' % ', '.join(self._expected_args),
                    print 'found (%s),' % ', '.join(data.keys())
                #job.bury()
                continue

            try:
                if self.callable(**data):
                    if self._debug:
                        print '  - job executed successfully'
                    job.delete()
                    if self._debug:
                        print '  - job deleted'
            except:
                if self._debug:
                    print '  - exception while processing job, burying for later inspection'
                job.bury()
                continue

            if self._debug:
                print 'Moving to next job...'
                print
                

    def __del__(self):
        try:
            self.server.close()
        except:
            pass


    def _process_argspec(self):
        self._accepts_kwargs = False
        self._all_args = []
        self._expected_args = []
        self._preset_args = []

        try:
            argspec = inspect.getargspec(self.callable)
        except TypeError:
            return

        self._accepts_kwargs = argspec.keywords != None

        # skip the "self" arg for class methods
        if inspect.ismethod(self.callable):
            self._all_args = argspec.args[1:]
        else:
            self._all_args = argspec.args

        try:
            self._expected_args = self._all_args[:-len(argspec.defaults)]
        except TypeError:
            self._expected_args = self._all_args

        try:
            self._preset_args = self._all_args[-len(argspec.defaults):]
        except TypeError:
            self._preset_args = self._all_args


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
    func = resolve_import(callable)

    if not func:
        raise ImportError()

    return func
