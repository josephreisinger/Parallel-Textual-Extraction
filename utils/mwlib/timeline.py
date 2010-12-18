#! /usr/bin/env python

# Copyright (c) 2007-2009 PediaPress GmbH
# See README.txt for additional licensing information.

"""implement http://meta.wikimedia.org/wiki/EasyTimeline
"""

import os
import tempfile
try:
    from hashlib import md5
except ImportError:
    from md5 import md5

    
_basedir = None
def _get_global_basedir():
    global _basedir
    if not _basedir:
        _basedir = tempfile.mkdtemp(prefix='timeline-')
        import atexit
        import shutil
        atexit.register(shutil.rmtree, _basedir)
    return _basedir

def drawTimeline(script, basedir=None):
    if isinstance(script, unicode):
        script = script.encode('utf8')
    if basedir is None:
        basedir = _get_global_basedir()
        
    m=md5()
    m.update(script)
    ident = m.hexdigest()

    pngfile = os.path.join(basedir, ident+'.png')
    
    if os.path.exists(pngfile):
        return pngfile

    scriptfile = os.path.join(basedir, ident+'.txt')
    open(scriptfile, 'w').write(script)
    et = os.path.join(os.path.dirname(__file__), "EasyTimeline.pl")
    
    err = os.system("perl %s -P /usr/bin/ploticus -T /tmp/ -i %s" % (et, scriptfile))
    if err != 0:
        return None

    svgfile = os.path.join(basedir, ident+'.svg')

    if os.path.exists(svgfile):
        os.unlink(svgfile)

    if os.path.exists(pngfile):
        return pngfile

    return None

    
