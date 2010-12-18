#! /usr/bin/env python

"""WSGI server interface to mw-render and mw-zip/mw-post"""

import errno
import os
import re
import shutil
import signal
import StringIO
import subprocess
import time
import urllib2
try:
    from hashlib import md5
except ImportError:
    from md5 import md5
try:
    import json
except ImportError:
    import simplejson as json

from mwlib import filequeue, log, utils, wsgi, _version
from mwlib.metabook import calc_checksum

# ==============================================================================

log = log.Log('mwlib.serve')

# ==============================================================================

def no_job_queue(job_type, collection_id, args):
    """Just spawn a new process for the given job"""
    
    if os.name == 'nt':
        kwargs = {}
    else:
        kwargs = {'close_fds': True}
    try:
        log.info('queueing %r' % args)
        subprocess.Popen(args, **kwargs)
    except OSError, exc:
        raise RuntimeError('Could not execute command %r: %s' % (
            args[0], exc,
        ))


# ==============================================================================

collection_id_rex = re.compile(r'^[a-z0-9]{16}$')

def make_collection_id(data):
    sio = StringIO.StringIO()
    for key in (
        _version.version,
        'base_url',
        'script_extension',
        'template_blacklist',
        'template_exclusion_category',
        'print_template_prefix',
        'print_template_pattern',
        'login_credentials',
    ):
        sio.write(repr(data.get(key)))
    mb = data.get('metabook')
    if mb:
        sio.write(calc_checksum(json.loads(mb)))
    return md5(sio.getvalue()).hexdigest()[:16]

# ==============================================================================

def json_response(fn):
    """Decorator wrapping result of decorated function in JSON response"""
    
    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)
        if isinstance(result, wsgi.Response):
            return result
        return wsgi.Response(
            content=json.dumps(result),
            headers={'Content-Type': 'application/json'},
        )
    return wrapper

# ==============================================================================

def lock(filename):
    if not hasattr(os, 'O_EXLOCK'):
        # this OS does not support file-locks via os.open(),
        # pretend we've got the lock
        return None
    fd = os.open(filename, os.O_CREAT|os.O_EXLOCK)
    return fd

def unlock(fd):
    if fd is None:
        return
    os.close(fd)

# ==============================================================================

class Application(wsgi.Application):
    metabook_filename = 'metabook.json'
    error_filename = 'errors'
    status_filename = 'status'
    output_filename = 'output'
    pid_filename = 'pid'
    zip_filename = 'collection.zip'
    mwpostlog_filename = 'mw-post.log'
    mwziplog_filename = 'mw-zip.log'
    mwrenderlog_filename = 'mw-render.log'
    
    def __init__(self, cache_dir,
        mwrender_cmd, mwrender_logfile,
        mwzip_cmd, mwzip_logfile,
        mwpost_cmd, mwpost_logfile,
        queue_dir,
        default_writer='rl',
        report_from_mail=None,
        report_recipients=None,
    ):
        self.cache_dir = utils.ensure_dir(cache_dir)
        self.mwrender_cmd = mwrender_cmd
        self.mwrender_logfile = mwrender_logfile
        self.mwzip_cmd = mwzip_cmd
        self.mwzip_logfile = mwzip_logfile
        self.mwpost_cmd = mwpost_cmd
        self.mwpost_logfile = mwpost_logfile
        if queue_dir:
            self.queue_job = filequeue.FileJobQueuer(utils.ensure_dir(queue_dir))
        else:
            self.queue_job = no_job_queue
        self.default_writer = default_writer
        self.report_from_mail = report_from_mail
        self.report_recipients = report_recipients
        self.new_collection_lockfile = os.path.join(self.cache_dir, 'new_collection.lock')
    
    def dispatch(self, request):
        if request.method != 'POST':
            return self.http405(permitted_methods='POST')
        try:
            command = request.post_data['command']
        except KeyError:
            return self.http500()
        try:
            method = getattr(self, 'do_%s' % command)
        except AttributeError:
            return self.http500()
        
        collection_id = request.post_data.get('collection_id')
        if collection_id is None:
            lockfile = lock(self.new_collection_lockfile)
        else:
            if not self.check_collection_id(collection_id):
                return self.http404()
            lockfile = lock(self.get_path(collection_id, 'lock'))
        try:
            try:
                return method(request.post_data)
            except Exception, exc:
                return self.error_response('error executing command %r: %s' % (
                    command, exc,
                ))
        finally:
            unlock(lockfile)
    
    @json_response
    def error_response(self, error, **kwargs):
        if isinstance(error, str):
            error = unicode(error, 'utf-8', 'ignore')
        elif not isinstance(error, unicode):
            error = unicode(repr(error), 'ascii')
        self.send_report_mail('error response', error=error, **kwargs)
        return {'error': error}
    
    def send_report_mail(self, subject, **kwargs):
        if not (self.report_from_mail and self.report_recipients):
            return
        utils.report(
            system='mwlib.serve',
            subject=subject,
            from_email=self.report_from_mail,
            mail_recipients=self.report_recipients,
            write_file=False,
            **kwargs
        )
    
    def get_collection_dirs(self, collection_id):
        assert len(collection_id) > 3, 'invalid collection ID'
        return (self.cache_dir, collection_id[0], collection_id[:2], collection_id)
    
    def get_collection_dir(self, collection_id):
        return os.path.join(*self.get_collection_dirs(collection_id))
    
    def check_collection_id(self, collection_id):
        """Return True iff collection with given ID exists"""
        
        if not collection_id or not collection_id_rex.match(collection_id):
            return False
        collection_dir = self.get_collection_dir(collection_id)
        if not os.path.exists(collection_dir):
            return False
        return True
    
    def new_collection(self, post_data):
        collection_id = make_collection_id(post_data)
        collection_dirs = self.get_collection_dirs(collection_id)
        for i in range(len(collection_dirs)):
            p = os.path.join(*collection_dirs[:i + 1])
            if os.path.isdir(p):
                continue
            try:
                log.info('Creating directory %r' % p)
                os.mkdir(p)
            except OSError, exc:
                if getattr(exc, 'errno') not in (errno.EEXIST, errno.EISDIR):
                    raise
        return collection_id
    
    def get_path(self, collection_id, filename, ext=None):
        p = os.path.join(self.get_collection_dir(collection_id), filename)
        if ext is not None:
            p += '.' + ext[:10]
        return p
    
    @json_response
    def do_render(self, post_data):
        metabook_data = post_data.get('metabook')
        collection_id = post_data.get('collection_id')
        if not (metabook_data or collection_id):
            return self.error_response('POST argument metabook or collection_id required')
        if metabook_data and collection_id:
            return self.error_response('Specify either metabook or collection_id, not both')
        try:
            base_url = post_data['base_url']
            writer = post_data.get('writer', self.default_writer)
        except KeyError, exc:
            return self.error_response('POST argument required: %s' % exc)
        writer_options = post_data.get('writer_options', '')
        template_blacklist = post_data.get('template_blacklist', '')
        template_exclusion_category = post_data.get('template_exclusion_category', '')
        print_template_prefix = post_data.get('print_template_prefix', '')
        print_template_pattern = post_data.get('print_template_pattern', '')
        login_credentials = post_data.get('login_credentials', '')
        force_render = bool(post_data.get('force_render'))
        script_extension = post_data.get('script_extension', '')
        language = post_data.get('language', '')
        
        if not collection_id:
            collection_id = self.new_collection(post_data)
        
        log.info('render %s %s' % (collection_id, writer))
        
        response = {
            'collection_id': collection_id,
            'writer': writer,
            'is_cached': False,
        }
        
        pid_path = self.get_path(collection_id, self.pid_filename, writer)
        if os.path.exists(pid_path):
            log.info('mw-render already running for collection %r' % collection_id)
            return response
        
        output_path = self.get_path(collection_id, self.output_filename, writer)
        if os.path.exists(output_path):
            if force_render:
                log.info('removing rendered file %r (forced rendering)' % output_path)
                utils.safe_unlink(output_path)
            else:
                log.info('re-using rendered file %r' % output_path)
                response['is_cached'] = True
                return response
        
        error_path = self.get_path(collection_id, self.error_filename, writer)
        if os.path.exists(error_path):
            log.info('removing error file %r' % error_path)
            utils.safe_unlink(error_path)
            force_render = True
        
        status_path = self.get_path(collection_id, self.status_filename, writer)
        if os.path.exists(status_path):
            if force_render:
                log.info('removing status file %r (forced rendering)' % status_path)
                utils.safe_unlink(status_path)
            else:
                log.info('status file exists %r' % status_path)
                return response
        
        if self.mwrender_logfile:
            logfile = self.mwrender_logfile
        else:
            logfile = self.get_path(collection_id, self.mwrenderlog_filename, writer)
        
        args = [
            self.mwrender_cmd,
            '--logfile', logfile,
            '--error-file', error_path,
            '--status-file', status_path,
            '--writer', writer,
            '--output', output_path,
            '--pid-file', pid_path,
        ]
        
        zip_path = self.get_path(collection_id, self.zip_filename)
        if not force_render and os.path.exists(zip_path):
            log.info('using existing ZIP file to render %r' % output_path)
            args.extend(['--config', zip_path])
            if writer_options:
                args.extend(['--writer-options', writer_options])
            if template_blacklist:
                args.extend(['--template-blacklist', template_blacklist])
            if template_exclusion_category:
                args.extend(['--template-exclusion-category', template_exclusion_category])
            if print_template_prefix:
                args.extend(['--print-template-prefix', print_template_prefix])
            if print_template_pattern:
                args.extend(['--print-template-pattern', print_template_pattern])
            if language:
                args.extend(['--language', language])
        else:
            log.info('rendering %r' % output_path)
            metabook_path = self.get_path(collection_id, self.metabook_filename)
            if metabook_data:
                f = open(metabook_path, 'wb')
                f.write(metabook_data)
                f.close()
            args.extend([
                '--metabook', metabook_path,
                '--config', base_url,
                '--keep-zip', zip_path,
            ])
            if writer_options:
                args.extend(['--writer-options', writer_options])
            if template_blacklist:
                args.extend(['--template-blacklist', template_blacklist])
            if template_exclusion_category:
                args.extend(['--template-exclusion-category', template_exclusion_category])
            if print_template_prefix:
                args.extend(['--print-template-prefix', print_template_prefix])
            if print_template_pattern:
                args.extend(['--print-template-pattern', print_template_pattern])
            if login_credentials:
                args.extend(['--login', login_credentials])
            if script_extension:
                args.extend(['--script-extension', script_extension])
            if language:
                args.extend(['--language', language])
        
        self.queue_job('render', collection_id, args)
        
        return response
    
    def read_status_file(self, collection_id, writer):
        status_path = self.get_path(collection_id, self.status_filename, writer)
        try:
            f = open(status_path, 'rb')
            return json.loads(f.read())
            f.close()
        except (IOError, ValueError):
            return {'progress': 0}
    
    @json_response
    def do_render_status(self, post_data):
        try:
            collection_id = post_data['collection_id']
            writer = post_data.get('writer', self.default_writer)
        except KeyError, exc:
            return self.error_response('POST argument required: %s' % exc)
            
        log.info('render_status %s %s' % (collection_id, writer))
        
        output_path = self.get_path(collection_id, self.output_filename, writer)
        if os.path.exists(output_path):
            return {
                'collection_id': collection_id,
                'writer': writer,
                'state': 'finished',
            }
        
        error_path = self.get_path(collection_id, self.error_filename, writer)
        if os.path.exists(error_path):
            text = unicode(open(error_path, 'rb').read(), 'utf-8', 'ignore')
            if text.startswith('traceback\n'):
                metabook_path = self.get_path(collection_id, self.metabook_filename)
                if os.path.exists(metabook_path):
                    metabook = unicode(open(metabook_path, 'rb').read(), 'utf-8', 'ignore')
                else:
                    metabook = None
                self.send_report_mail('rendering failed',
                    collection_id=collection_id,
                    writer=writer,
                    error=text,
                    metabook=metabook,
                )
            return {
                'collection_id': collection_id,
                'writer': writer,
                'state': 'failed',
                'error': text,
            }
        
        return {
            'collection_id': collection_id,
            'writer': writer,
            'state': 'progress',
            'status': self.read_status_file(collection_id, writer),
        }
    
    @json_response
    def do_render_kill(self, post_data):
        try:
            collection_id = post_data['collection_id']
            writer = post_data.get('writer', self.default_writer)
        except KeyError, exc:
            return self.error_response('POST argument required: %s' % exc)
        
        log.info('render_kill %s %s' % (collection_id, writer))
        
        pid_path = self.get_path(collection_id, self.pid_filename, writer)
        killed = False
        try:
            pid = int(open(pid_path, 'rb').read())
            os.kill(pid, signal.SIGKILL)
            killed = True
        except (OSError, ValueError, IOError):
            pass
        return {
            'collection_id': collection_id,
            'writer': writer,
            'killed': killed,
        }
    
    def do_download(self, post_data):
        try:
            collection_id = post_data['collection_id']
            writer = post_data.get('writer', self.default_writer)
        except KeyError, exc:
            log.ERROR('POST argument required: %s' % exc)
            return self.http500()
        
        try:
            log.info('download %s %s' % (collection_id, writer))
        
            output_path = self.get_path(collection_id, self.output_filename, writer)
            status = self.read_status_file(collection_id, writer)
            response = wsgi.Response(content=open(output_path, 'rb'))
            os.utime(output_path, None)
            if 'content_type' in status:
                response.headers['Content-Type'] = status['content_type'].encode('utf-8', 'ignore')
            else:
                log.warn('no content type in status file')
            if 'file_extension' in status:
                response.headers['Content-Disposition'] = 'inline; filename=collection.%s' %  (
                    status['file_extension'].encode('utf-8', 'ignore'),
                )
            else:
                log.warn('no file extension in status file')
            return response
        except Exception, exc:
            log.ERROR('exception in do_download(): %r' % exc)
            return self.http500()
    
    @json_response
    def do_zip_post(self, post_data):
        try:
            metabook_data = post_data['metabook']
            base_url = post_data['base_url']
        except KeyError, exc:
            return self.error_response('POST argument required: %s' % exc)
        template_blacklist = post_data.get('template_blacklist', '')
        template_exclusion_category = post_data.get('template_exclusion_category', '')
        print_template_prefix = post_data.get('print_template_prefix', '')
        print_template_pattern = post_data.get('print_template_pattern', '')
        login_credentials = post_data.get('login_credentials', '')
        script_extension = post_data.get('script_extension', '')
        
        pod_api_url = post_data.get('pod_api_url', '')
        if pod_api_url:
            result = json.loads(urllib2.urlopen(pod_api_url, data="any").read())
            post_url = result['post_url']
            response = {
                'state': 'ok',
                'redirect_url': result['redirect_url'],
            }
        else:
            try:
                post_url = post_data['post_url']
            except KeyError:
                return self.error_response('POST argument required: post_url')
            response = {'state': 'ok'}
        
        collection_id = self.new_collection(post_data)
        
        log.info('zip_post %s %s' % (collection_id, pod_api_url))
        
        pid_path = self.get_path(collection_id, self.pid_filename, 'zip')
        zip_path = self.get_path(collection_id, self.zip_filename)
        if os.path.exists(zip_path):
            log.info('POSTing ZIP file %r' % zip_path)
            if self.mwpost_logfile:
                logfile = self.mwpost_logfile
            else:
                logfile = self.get_path(collection_id, self.mwpostlog_filename)
            args = [
                self.mwpost_cmd,
                '--logfile', logfile,
                '--posturl', post_url,
                '--input', zip_path,
                '--pid-file', pid_path,
            ]
        else:
            log.info('Creating and POSting ZIP file %r' % zip_path)
            if self.mwzip_logfile:
                logfile = self.mwzip_logfile
            else:
                logfile = self.get_path(collection_id, self.mwziplog_filename)
            metabook_path = self.get_path(collection_id, self.metabook_filename)
            f = open(metabook_path, 'wb')
            f.write(metabook_data)
            f.close()
            args = [
                self.mwzip_cmd,
                '--logfile', logfile,
                '--metabook', metabook_path,
                '--config', base_url,
                '--posturl', post_url,
                '--output', zip_path,
                '--pid-file', pid_path,
            ]
            if template_blacklist:
                args.extend(['--template-blacklist', template_blacklist])
            if template_exclusion_category:
                args.extend(['--template-exclusion-category', template_exclusion_category])
            if print_template_prefix:
                args.extend(['--print-template-prefix', print_template_prefix])
            if print_template_pattern:
                args.extend(['--print-template-pattern', print_template_pattern])
            if login_credentials:
                args.extend(['--login', login_credentials])
            if script_extension:
                args.extend(['--script-extension', script_extension])
        
        self.queue_job('post', collection_id, args)
        
        return response
    

# ==============================================================================

def get_collection_dirs(cache_dir):
    """Generator yielding full paths of collection directories"""

    for dirpath, dirnames, filenames in os.walk(cache_dir):
        for d in dirnames:
            if collection_id_rex.match(d):
                yield os.path.join(dirpath, d)

def purge_cache(max_age, cache_dir):
    """Remove all subdirectories of cache_dir whose mtime is before now-max_age
    
    @param max_age: max age of directories in seconds
    @type max_age: int
    
    @param cache_dir: cache directory
    @type cache_dir: basestring
    """
    
    now = time.time()
    for path in get_collection_dirs(cache_dir):
        if now - os.stat(path).st_mtime < max_age:
            continue
        try:
            log.info('removing directory %r' % path)
            shutil.rmtree(path)
        except Exception, exc:
            log.ERROR('could not remove directory %r: %s' % (path, exc))
    
def clean_up(cache_dir):
    """Look for PID files whose processes have not finished/erred but ceised
    to exist => remove cache directorie.
    """

    for path in get_collection_dirs(cache_dir):
        for e in os.listdir(path):
            if '.' not in e:
                continue
            parts = e.split('.')
            if parts[0] != Application.pid_filename:
                continue
            ext = parts[1]
            if not ext:
                continue
            pid_file = os.path.join(path, e)
            try:
                pid = int(open(pid_file, 'rb').read())
            except ValueError:
                log.ERROR('PID file %r with invalid contents' % pid_file)
                continue
            except IOError, exc:
                log.ERROR('Could not read PID file %r: %s' % (pid_file, exc))
                continue
            
            try:
                os.kill(pid, 0)
            except OSError, exc:
                if exc.errno == 3: # No such process
                    log.warn('Have dangling PID file %r' % pid_file)
                    os.unlink(pid_file)
                    error_file = os.path.join(path, '%s.%s' % (Application.error_filename, ext))
                    if not os.path.exists(error_file):
                        open(error_file, 'wb').write('Process died.\n')

