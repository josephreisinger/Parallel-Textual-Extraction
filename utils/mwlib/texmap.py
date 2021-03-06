#! /usr/bin/env python

# Copyright (c) 2007-2009 PediaPress GmbH
# See README.txt for additional licensing information.

import re

def convertSymbols(latexsource):
    def repl(mo):
        name=mo.group(0)
        return symbolMap.get(name, name)

    latexsource = texcmd.sub(repl, latexsource)
    return latexsource

texcmd = re.compile(r"\\[a-zA-Z]+")

symbolMap = {'\\Bbb': '\\mathbb',
             '\\Complex': '\\mathbb{C}',
             '\\Dagger': '\\ddagger',
             '\\Darr': '\\Downarrow',
             '\\Harr': '\\Leftrightarrow',
             '\\Larr': '\\Leftarrow',
             '\\Lrarr': '\\Leftrightarrow',
             '\\N': '\\mathbb{N}',
             '\\O': '\\emptyset',
             '\\R': '\\mathbb{R}',
             '\\Rarr': '\\Rightarrow',
             '\\Reals': '\\mathbb{R}',
             '\\Uarr': '\\Uparrow',
             '\\Z': '\\mathbb{Z}',
             '\\alef': '\\aleph',
             '\\alefsym': '\\aleph',
             '\\and': '\\land',
             '\\ang': '\\angle',
             '\\arccos': '\\mathop{\\mathrm{arccos}}',
             '\\arccot': '\\mathop{\\mathrm{arccot}}',
             '\\arccsc': '\\mathop{\\mathrm{arccsc}}',
             '\\arcsec': '\\mathop{\\mathrm{arcsec}}',
             '\\bold': '\\mathbf',
             '\\bull': '\\bullet',
             '\\clubs': '\\clubsuit',
             '\\cnums': '\\mathbb{C}',
             '\\dArr': '\\Downarrow',
             '\\darr': '\\downarrow',
             '\\diamonds': '\\diamondsuit',
             '\\empty': '\\emptyset',
             '\\exist': '\\exists',
             '\\ge': '\\geq',
             '\\hAar': '\\Leftrightarrow',
             '\\harr': '\\leftrightarrow',
             '\\hearts': '\\heartsuit',
             '\\image': '\\Im',
             '\\infin': '\\infty',
             '\\isin': '\\in',
             '\\lArr': '\\Leftarrow',
             '\\lang': '\\langle',
             '\\larr': '\\leftarrow',
             '\\le': '\\leq',
             '\\lrArr': '\\Leftrightarrow',
             '\\lrarr': '\\leftrightarrow',
             '\\natnums': '\\mathbb{N}',
             '\\ne': '\\neq',
             '\\or': '\\lor',
             '\\part': '\\partial',
             '\\plusmn': '\\pm',
             '\\rArr': '\\Rightarrow',
             '\\rang': '\\rangle',
             '\\rarr': '\\rightarrow',
             '\\real': '\\Re',
             '\\reals': '\\mathbb{R}',
             '\\sdot': '\\cdot',
             '\\sect': '\\S',
             '\\sgn': '\\mathop{\\mathrm{sgn}}',
             '\\spades': '\\spadesuit',
             '\\sub': '\\subset',
             '\\sube': '\\subseteq',
             '\\supe': '\\supseteq',
             '\\thetasym': '\\vartheta',
             '\\uArr': '\\Uparrow',
             '\\uarr': '\\uparrow',
             '\\weierp': '\\wp',
             '\\Alpha': 'A{}',
             '\\Beta': 'B{}',
             '\\Epsilon': 'E{}',
             '\\Zeta': 'Z{}',
             '\\Eta': 'H{}',             
             '\\Iota': 'I{}',
             '\\Kappa' : 'K{}',           
             '\\Mu': 'M{}',
             '\\Nu': 'N{}',
             '\\Rho': 'P{}',
             '\\Tau': 'T{}',
             '\\Chi': 'C{}',
             }
