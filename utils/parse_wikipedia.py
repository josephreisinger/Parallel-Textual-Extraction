import sys
import codecs
import mwlib.mwapidb

from mwlib.uparser import parseString

from textformatter import TextFormatter

def parse_raw_mediawiki(title, text):
    formatter = TextFormatter()
    node = parseString(title=title, raw=text)
    formatter.__init__()
    formatter.format(node)
    return formatter.getArticleText()

# x = client.getExpandedText("Isaac Newton")
# print x.encode('utf8','replace')
# x = client.getFormattedArticleText("Barack Obama")

# print x.encode('utf8','replace')
# print type(x)

