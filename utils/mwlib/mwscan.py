#! /usr/bin/env python

# Copyright (c) 2007-2009 PediaPress GmbH
# See README.txt for additional licensing information.

import sys
import re
import _mwscan
import htmlentitydefs

class token(object):
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
    t_begin_table = 10
    t_end_table = 11
    t_html_tag = 12
    t_singlequote = 13
    t_pre = 14
    t_section = 15
    t_section_end = 16
    t_item = 17
    t_colon = 18
    t_semicolon = 19
    t_hrule = 20
    t_newline = 21
    t_column = 22
    t_row = 23
    t_tablecaption = 24
    t_urllink = 25

    token2name = {}

for d in dir(token):
    token2name = token.token2name
    if d.startswith("t_"):
        token2name[getattr(token, d)] = d
del d

def _split_tag(txt):
    m=re.match(" *(\w+)(.*)", txt)
    assert m is not None, "could not match tag name"
    name = m.group(1)
    values = m.group(2)
    return name, values
    
def dump_tokens(text, tokens):
    for type, start, len in tokens:
        print type, repr(text[start:start+len])
           
def scan(text):
    text += u"\0"*32    
    tokens = _mwscan.scan(text)
    return scan_result(text, tokens)

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
                         

class scan_result(object):
    def __init__(self, source, toks):
        self.source = source
        self.toks = toks
        
    def rawtext(self, (type, start, tlen)):
        return self.source[start:start+tlen]

    def text(self, t):
        r=self.rawtext(t)
        if t[0] == token.t_entity:
            return resolve_entity(r)
        else:
            return r

    def dump(self, out=None):
        if out is None:
            out = sys.stdout
        for x in self:
            out.write("%s\n" % self.repr(x))

            
        
    def repr(self, t):
        return "(%s, %r)" % (token.token2name.get(t[0]), self.rawtext(t))


    def __len__(self):     
        return len(self.toks)

    def __iter__(self):
        return iter(self.toks)

    def __getitem__(self, idx):
        return self.toks[idx]



class _compat_scanner(object):
    from mwlib.tagext import default_registry as tagextensions
    allowed_tags = None
    
    class ignore: pass
    tok2compat = {
        token.t_text: "TEXT",
        token.t_special: "SPECIAL",
        token.t_2box_open: "[[",
        token.t_2box_close: "]]",
        token.t_http_url: "URL",
        token.t_break: "BREAK",
        token.t_singlequote: "SINGLEQUOTE", 
        token.t_pre: "PRE",
        token.t_section: "SECTION",
        token.t_section_end: "ENDSECTION",
        token.t_magicword: ignore,
        token.t_comment: ignore,
        token.t_end: ignore,
        token.t_item: "ITEM",
        token.t_colon: "EOLSTYLE",
        token.t_semicolon: "EOLSTYLE",
        token.t_newline: "\n",
        token.t_begin_table: "BEGINTABLE",
        token.t_end_table: "ENDTABLE",
        token.t_column: "COLUMN",
        token.t_row: "ROW",
        token.t_tablecaption: "TABLECAPTION",
        token.t_urllink: "URLLINK",
        }


    def _init_allowed_tags(self):
        from mwlib.parser import _get_tags
        self.allowed_tags = _get_tags()
        
    def __call__(self, text):
        if self.allowed_tags is None:
            self._init_allowed_tags()

        if isinstance(text, str):
            text = unicode(text)
            
        tokens = scan(text)
        scanres = scan_result(text, tokens)


        res = []

        def g():
            return text[start:start+tlen]
        a = lambda x: res.append((x,g()))


        ignore = self.ignore
        tok2compat = self.tok2compat

        i = 0
        numtokens = len(tokens)
        while i < numtokens:
            type, start, tlen = tokens[i]
            n=tok2compat.get(type)
            if n is ignore:
                pass
            elif n is not None:
                a(n)
            elif type==token.t_entity:
                res.append(("TEXT", resolve_entity(g())))
            elif type==token.t_hrule:
                res.append((self.tagtoken("<hr />"), g()))
            elif type==token.t_html_tag:
                s = g()

                tt = self.tagtoken(s)
                isEndToken = isinstance(tt, EndTagToken)
                closingOrSelfClosing = isEndToken or tt.selfClosing
                
                if tt.t in self.tagextensions or tt.t in ('imagemap', 'gallery'):
                    if closingOrSelfClosing:
                        i+=1
                        continue
                    tagname = tt.t
                    
                    res.append((tt, s))
                    i+=1
                    text_start = None
                    text_end = None
                    end_token = None
                    
                    while i<numtokens:
                        type, start, tlen = tokens[i]
                        if text_start is None:
                            text_start = start
                            
                        if type==token.t_html_tag:
                            tt = self.tagtoken(g())
                            if tt.t==tagname:
                                end_token = (tt, g())
                                break
                        text_end = start+tlen
                        
                        i+=1

                    if text_end:
                        res.append(("TEXT", text[text_start:text_end]))
                        
                    if end_token:
                        res.append(end_token)
                    

                        
                elif tt.t=="nowiki":
                    i+=1
                    if isEndToken or tt.selfClosing:
                        continue
                    while i<numtokens:
                        type, start, tlen = tokens[i]
                        if type==token.t_html_tag:
                            tt = self.tagtoken(g())
                            if tt.t=="nowiki":
                                break
                        res.append(("TEXT", scanres.text((type, start, tlen))))                    
                        i+=1
                elif tt.t in ["font", "noinclude", 'caption']:
                    pass
                elif tt.t=="table":
                    if isEndToken:
                        res.append(("ENDTABLE", g()))
                    else:
                        res.append(("BEGINTABLE", g()))
                elif tt.t in ['th', 'td']:
                    if isEndToken:
                        pass
                    else:
                        res.append(("COLUMN", g()))
                elif tt.t=='tr':
                    if isEndToken:
                        pass
                    else:
                        res.append(("ROW", g()))
                else:
                    if tt.t in self.allowed_tags:
                        res.append((tt, s))
                    else:
                        res.append(("TEXT", s))
            else:
                a(type)
            i+=1


        return res

    def tagtoken(self, text):
        selfClosing = False
        if text.startswith(u"</"):
            name = text[2:-1]
            klass = EndTagToken
            isEndToken = True
        elif text.endswith("/>"):
            name = text[1:-2]
            klass = TagToken
            selfClosing = True
            isEndToken = False # ???
        else:
            name = text[1:-1]
            klass = TagToken
            isEndToken = False

        name, values = _split_tag(name)
        from mwlib.parser import paramrx
        values = dict(paramrx.findall(values))
        name = name.lower()

        if name=='br' or name=='references':
            isEndToken = False
            klass = TagToken

        r = klass(name, text)
        r.selfClosing = selfClosing
        r.values = values
        return r

        
        
compat_scan = _compat_scanner()

class _BaseTagToken(object):
    def __eq__(self, other):
        if isinstance(other, basestring):
            return self.t == other
        if isinstance(other, self.__class__):
            return self.t == other.t
        return False

    def __ne__(self, other):
        return not(self==other)
    
    def __hash__(self):
        return hash(self.t)

class TagToken(_BaseTagToken):
    values = {}
    selfClosing=False

    def __init__(self, t, text=''):
        self.t = t
        self.text = text

    def __repr__(self):
        return "<Tag:%r %r>" % (self.t, self.text)

class EndTagToken(_BaseTagToken):
    def __init__(self, t, text=''):
        self.t = t
        self.text = text
        
    def __repr__(self):
        return "<EndTag:%r>" % (self.t,)

def tokenize(input, name="unknown"):
    assert input is not None, "must specify input argument in tokenize"
    return compat_scan(input)
