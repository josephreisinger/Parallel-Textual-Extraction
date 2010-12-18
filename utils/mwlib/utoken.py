#! /usr/bin/env python

# Copyright (c) 2007-2009 PediaPress GmbH
# See README.txt for additional licensing information.

# unified/universal token

import sys
import re
import _uscan as _mwscan
import htmlentitydefs

def walknode(node, filt=lambda x: True):
    if not isinstance(node, token):
        for x in node:
            for k in walknode(x):
                if filt(k):
                    yield k
        return
    
    if filt(node):
        yield node
        
    if node.children:
        for x in node.children:
            for k in walknode(x):
                if filt(k):
                    yield k
            
def show(node, out=None, indent=0, verbose=False):
    if node is None:
        return
    
    if out is None:
        out = sys.stdout

    if not isinstance(node, token):
        for x in node:
            show(x, out=out, indent=indent, verbose=verbose)
        return

    out.write("%s%r\n" % ("    "*indent, node))
    
    children = node.children
    if children:
        for x in children:
            show(x, out=out, indent=indent+1, verbose=verbose)
            
class token(object):
    caption = ''
    vlist = None
    target = None
    level = None
    children = None
    tagname = None
    ns = None
    lineprefix = None
    interwiki = None
    langlink = None
    namespace = None
    blocknode = False
    
    t_end = 0
    t_text = 1
    t_entity = 2
    t_special = 3
    t_magicword = 4
    t_comment = 5
    t_2box_open = 6
    t_2box_close = 7
    t_http_url = 8
    t_break = 9
    t_begintable = t_begin_table = 10
    t_endtable = t_end_table = 11
    t_html_tag = 12
    t_singlequote = 13
    t_pre = 14
    t_section = 15
    t_endsection = t_section_end = 16
    
    t_item = 17
    t_colon = 18
    t_semicolon = 19
    t_hrule = 20
    t_newline = 21
    t_column = 22
    t_row = 23
    t_tablecaption = 24
    t_urllink = 25

    
    t_html_tag_end = 100
    
    token2name = {}
    _text = None

    @staticmethod
    def join_as_text(tokens):
        return u"".join([x.text or u"" for x in tokens])
    
    def _get_text(self):
        if self._text is None and self.source is not None:
            self._text = self.source[self.start:self.start+self.len]
        return self._text
    
    def _set_text(self, t):
        self._text = t

    text = property(_get_text, _set_text)
    
    def __init__(self, type=None, start=None, len=None, source=None, text=None, **kw):
        self.type = type
        self.start = start
        self.len = len
        self.source = source
        if text is not None:
            self.text = text
            
        self.__dict__.update(kw)

    def __repr__(self):
        r = [self.token2name.get(self.type, self.type)]
        if self.text is not None:
            r.append(repr(self.text)[1:])
        if self.vlist:
            r.append(" vlist=")
            r.append(repr(self.vlist))
        if self.target:
            r.append(" target=")
            r.append(repr(self.target))
        if self.level:
            r.append(" level=")
            r.append(repr(self.level))
        if self.tagname:
            r.append(" tagname=")
            r.append(repr(self.tagname))
        if self.ns is not None:
            r.append(" ns=")
            r.append(repr(self.ns))
        if self.lineprefix is not None:
            r.append(" lineprefix=")
            r.append(self.lineprefix)
        if self.interwiki:
            r.append(" interwiki=")
            r.append(repr(self.interwiki))
        if self.langlink:
            r.append(" langlink=")
            r.append(repr(self.langlink))
        if self.type==self.t_complex_style:
            r.append(repr(self.caption))
        return u"".join(r)

    def show(self, out=None):
        show(self, out=out)
    
token2name = token.token2name
for d in dir(token):
    if d.startswith("t_"):
        token2name[getattr(token, d)] = d
del d, token2name

def _split_tag(txt):
    m=re.match(" *(\w+)(.*)", txt)
    assert m is not None, "could not match tag name"
    name = m.group(1)
    values = m.group(2)
    return name, values
    
def _analyze_html_tag(t):
    text = t.text
    selfClosing = False
    if text.startswith(u"</"):
        name = text[2:-1]
        isEndToken = True
    elif text.endswith("/>"):
        name = text[1:-2]
        selfClosing = True
        isEndToken = False # ???
    else:
        name = text[1:-1]
        isEndToken = False

    name, values = _split_tag(name)
    from mwlib.parser import parseParams
    t.vlist = parseParams(values)
    name = name.lower()

    if name=='br' or name=='references':
        isEndToken = False

    t.tagname = name
    t.tag_selfClosing = selfClosing
    t.tag_isEndToken = isEndToken
    if isEndToken:
        t.type = t.t_html_tag_end

def dump_tokens(text, tokens):
    for type, start, len in tokens:
        print type, repr(text[start:start+len])
           
def scan(text):
    text += u"\0"*32    
    return _mwscan.scan(text)

def resolve_entity(e):
    if e[1]=='#':
        try:
            if e[2]=='x' or e[2]=='X':
                return unichr(int(e[3:-1], 16))
            else:
                return unichr(int(e[2:-1]))
        except ValueError:
            return e        
    else:
        try:
            return unichr(htmlentitydefs.name2codepoint[e[1:-1]])
        except KeyError:
            return e
                         
class _compat_scanner(object):
    from mwlib.tagext import default_registry as tagextensions
    allowed_tags = None

    def _init_allowed_tags(self):
        from mwlib.parser import _get_tags
        self.allowed_tags = _get_tags()

        self.allowed_tags.update("table td tr".split())
        
#         tmp = list(self.allowed_tags)
#         tmp.sort()
#         print "ALLOWED:", " ".join(tmp)
        
        
    def __call__(self, text):
        if self.allowed_tags is None:
            self._init_allowed_tags()

        if isinstance(text, str):
            text = unicode(text)
            
        tokens = scan(text)

        res = []

        def g():
            return text[start:start+tlen]

        i = 0
        numtokens = len(tokens)
        while i < numtokens:
            type, start, tlen = tokens[i]

            if type==token.t_begintable:
                txt = g()
                count = txt.count(":")
                if count:
                    res.append(token(type=token.t_colon, start=start, len=count, source=text))
                tlen -= count
                start += count
                    
                
                
            t = token(type=type, start=start, len=tlen, source=text)

            if type==token.t_entity:
                t.text = resolve_entity(g())
                t.type = token.t_text
                res.append(t)
            elif type==token.t_html_tag:
                s = g()

                _analyze_html_tag(t)
                isEndToken = t.tag_isEndToken
                closingOrSelfClosing = isEndToken or t.tag_selfClosing
                tagname = t.tagname
                
                if tagname in self.tagextensions or tagname in ('imagemap', 'gallery'):
                    if closingOrSelfClosing:
                        i+=1
                        continue
                    res.append(t)
                    
                    i+=1
                    text_start = None
                    text_end = None
                    end_token = None
                    
                    while i<numtokens:
                        type, start, tlen = tokens[i]
                        if text_start is None:
                            text_start = start
                            
                        if type==token.t_html_tag:
                            this_tag = token(type=type, start=start, len=tlen, source=text)
                            _analyze_html_tag(this_tag)
                            if this_tag.tagname == tagname:
                                end_token = this_tag
                                break
                        text_end = start+tlen
                        
                        i+=1

                    if text_end:
                        res.append(token(type=token.t_text, start=text_start, len=text_end-text_start, source=text))
                        
                    if end_token:
                        res.append(end_token)
                    
                elif tagname=="nowiki":
                    i+=1
                    if isEndToken or t.tag_selfClosing:
                        continue
                    while i<numtokens:
                        type, start, tlen = tokens[i]
                        if type==token.t_html_tag:
                            tt = token(type=type, start=start, len=tlen, source=text)
                            _analyze_html_tag(tt)
                            if tt.tagname=="nowiki":
                                break

                        
                        if type==token.t_entity:
                            res.append(token(type=token.t_text, start=start, len=tlen, source=text, text=resolve_entity(g())))
                        else:
                            res.append(token(type=token.t_text, start=start, len=tlen, source=text, text=text[start:start+tlen]))
                        i+=1
                else:
                    if tagname in self.allowed_tags:
                        res.append(t)
                    else:
                        res.append(token(type=token.t_text, start=start, len=tlen, source=text))
            else:
                res.append(t)
            i+=1

        return res
        
compat_scan = _compat_scanner()

def tokenize(input, name="unknown"):
    assert input is not None, "must specify input argument in tokenize"
    return compat_scan(input)
