#! /usr/bin/env python
#! -*- coding:utf-8 -*-

# Copyright (c) 2007, PediaPress GmbH
# See README.txt for additional licensing information.

import re
import os
import string

class Scripts(object):

    def __init__(self):
        scripts_filename = os.path.join(os.path.dirname(__file__), 'scripts.txt')
        self.script2code_block = {} 
        self.code_block2scripts = []
        self.readScriptFile(scripts_filename)
        
    def readScriptFile(self, fn):
        try:
            f = open(fn)
        except IOError:
            raise Exception('scripts.txt file not found at: %r' % fn)
        for line in f.readlines():
            res = re.search('([A-Z0-9]+)\.\.([A-Z0-9]+); (.*)' , line.strip())
            if res:
                start_block, end_block, script = res.groups()
                self.script2code_block[script.lower()] = (int(start_block, 16), int(end_block, 16))
                self.code_block2scripts.append((int(start_block, 16), int(end_block, 16), script))        

    def getCodePoints(self, script):
        code_block = self.script2code_block.get(script.lower())
        if not code_block:
            return (0, 0)
        else:
            return code_block

    def getScriptsForCodeBlock(self, code_block):
        scripts = set()
        for start_block, end_block, script in self.code_block2scripts:
            if start_block <= code_block[0] <= end_block:
                scripts.add(script)
            if start_block <= code_block[1] <= end_block:
                scripts.add(script)
                break
        return scripts

    def getScriptsForCodeBlocks(self, code_blocks):
        scripts = set()
        for code_block in code_blocks:
            scripts = scripts.union(self.getScriptsForCodeBlock(code_block))
        return scripts
    
class FontSwitcher(object):

    def __init__(self):
        self.scripts = Scripts()        
        self.default_font = None
        self.code_points2font = []


    def unregisterFont(self, unreg_font_name):
        registered_entries = []
        i = 0
        for block_start, block_end, font_name in self.code_points2font:
            if unreg_font_name == font_name:
                registered_entries.append(i)
            i += 1
        registered_entries.reverse()
        for entry in registered_entries:
            self.code_points2font.pop(entry)
        
        
    def registerFont(self, font_name, code_points=[]):
        """ Register a font to certain scripts or a range of code point blocks"""
        if not code_points:
            return
        for block in code_points:
            if isinstance(block, basestring):
                block = self.scripts.getCodePoints(block)
            block_start, block_end = block
            self.code_points2font.insert(0, (block_start, block_end, font_name))

    def registerDefaultFont(self, font_name=None):
        self.default_font = font_name

    def getFont(self, ord_char):
        for block_start, block_end, font_name in self.code_points2font:            
            if block_start <= ord_char <= block_end:
                return font_name
        return self.default_font

    def getFontList(self, txt):
        txt_list = []
        last_font = None
        last_txt = []
        for c in txt:
            ord_c = ord(c)
            if ord_c <= 32 or ord_c == 127:
                if ord_c != 9:
                    c =' '
                if last_font:
                    font = last_font
                else:
                    font = self.default_font
            else:
                font = self.getFont(ord_c)
            if font != last_font and last_txt:
                txt_list.append((''.join(last_txt), last_font))
                last_txt = []
            last_txt.append(c)
            last_font = font
        if last_txt:
            txt_list.append((''.join(last_txt), last_font))
        return txt_list
                

if __name__ == '__main__':
    _scripts = Scripts()

    blocks = [
        (9472, 9580),
        #(4352, 4607),
        #(12592, 12687),
        ]

    scripts = _scripts.getScriptsForCodeBlocks(blocks)
    print scripts
    for script in scripts:
        print _scripts.getCodePoints(script)
