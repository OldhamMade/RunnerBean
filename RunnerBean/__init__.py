import os
import sys

import beanstalkc
from resolver import resolve as resolve_import
from yaml import safe_load as decrypt


class Runner(object):
    debug = False

    def __init__(self, callable, host='0.0.0.0', port=11300, tubes=None, debug=False):
        if isinstance(callable, basestring):
            self.callable = resolve(callable)

        elif hasattr(callable, '__call__'):
            self.callable = callable

        else:
            raise Exception('"callable" is not a callable')
        
        self._host = host
        self._port = port
        self._tubes = tubes
        self.debug = debug


    def __call__(self, timeout=None):
        self.run(timeout=timeout)


    def run(self, timeout=None):
        while 1:
            if self.debug:
                print 'Reserving',
                if timeout:
                    print 'with timeout of %ds' % timeout,
                print '...'

            if timeout:
                job = self.server.reserve(timeout=timeout)
            else:
                job = self.server.reserve()

            if self.debug:
                print 'Found job: %s' % job.id

            if not job or not job.body:
                if self.debug:
                    print '  - burying job due to missing body'
                job.bury()
                continue

            try:
                data = decrypt(job.body)
            except:
                if self.debug:
                    print '  - burying job due to malformed body (not parsable by PyYAML)'
                job.bury()
                continue

            if not data or set(data.keys()) != self.expected_keys:
                if self.debug:
                    print '  - burying job due to missing keys:',
                    print 'espected (%s),' % ', '.join(self.expected_keys),
                    print 'found (%s),' % ', '.join(data.keys())
                job.bury()
                continue

            try:
                if self.callable(**data):
                    if self.debug:
                        print '  - job executed successfully'
                    job.delete()
                    if self.debug:
                        print '  - job deleted'
            except:
                if self.debug:
                    print '  - exception while processing job, burying for later inspection'
                job.bury()
                continue

            if self.debug:
                print 'Moving to next job...'
                print
                

    def __del__(self):
        self.server.close()

    def _get_connection(self):
        if not self._server:
            self._server = beanstalkc.Connection(host=self._host,
                                                 port=self._port)

            if self._tube:
                self._server.watch(self._tube)
                self._server.ignore('default')

        return self._server

    server = property(_get_connection)
            


def resolve(callable):
    func = resolve_import(callable)

    if not func:
        raise ImportError()

    return func
