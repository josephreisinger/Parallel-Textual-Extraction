# ATTRIBUTE PEDIAPRESS GOD DAMN IT

import logging
log = logging.getLogger(__name__)

from codecs import getwriter
from cStringIO import StringIO
from types import ObjectType

QUOTE_BEGIN = u'QUOTE:'
QUOTE_END = u'END QUOTE.'

class TextFormatter(object):
    """
    Formats Wikipedia articles based on their ASTs. Stores the text content
    in a dictionary where each key is a section index and the corresponding
    value is the content for that section. The text before any section (the
    "intro" section) has index ``(1,)``. The text immediately after the first
    section heading has index ``(2,)``. The text within a nested section has
    index ``(2,1)``. The text in the next top-level section has index ``(3,)``,
    and so forth.
    
    After formatting, use the ``getText()`` method to retrieve the contents, or
    the ``writeTo()`` method to write out the contents.
    """
    
    # Nodes that we want to ignore.
    IGNORE_NODES = (
        'Magic',
        'PreFormatted',
        'Cell',
        'Row',
        'Table',
        'Math',
        'URL',
        'Timeline',
    )
    
    # Section names to ignore.
    IGNORE_SECTIONS = (
        'References',
        'See also',
        'External links',
    )
    
    # Tag nodes whose contents we wish to ignore (lower case).
    IGNORE_TAGS = (
        's',
        'del',
        'ref',
    )
    
    def __init__(self):
        self.content = {}       # Dictionary of section content, indexed by
                                # section; each is a list of unicode strings.
        self.section = (1,)     # The current section index.
        self.titles = {}        # Dictionary of section titles, indexed by
                                # section.
        self.levels = [1]       # The current section levels and counts.
        self.levelOffset = None # The offset from 0 of the top-level level
                                # index. This is set upon formatting the first
                                # section. (Should generally be 2.)
        self.list_number = 0    # Contains the current list item number if
                                # inside a numbered list. Otherwise 0.
        self.titles[self.section] = 'Introduction'
    
    def writeTo(self, out):
        """
        Writes the formatted text to the given output stream.
        """
        # Get an ordered list of section indices.
        sections = sorted(self.content.keys())
        
        # Write each section.
        for section in sections:
            content = self.content[section]
            
            # Write section numbers.
            # out.write(u'Section %s: ' % '.'.join((str(i) for i in section)))
            
            # Write the section title.
            # out.write(u'%s\n\n' % self.titles[section])
            
            # Write each string in this section's content buffer.
            for s in content:
                out.write(s)
    
    def getArticleText(self):
        """
        Retrieves the content as one big unicode string.
        """        
        # Get an ordered list of section indices.
        sections = sorted(self.content.keys())
        
        # Write each section.
        sbuf = StringIO()
        ubuf = getwriter('utf-8')(sbuf)
        for section in sections:
            content = self.content[section]
            
            # Write section numbers.
            # ubuf.write(u'Section %s: ' % '.'.join((str(i) for i in section)))
            
            # Write the section title.
            # ubuf.write(u'%s\n\n' % self.titles[section])
            
            # Write each string in this section's content buffer.
            for s in content:
                ubuf.write(s)
        
        # Get value and close buffers.
        text = ubuf.getvalue()
        ubuf.close()
        sbuf.close()
        
        return text
    
    def getArticleContent(self):
        """Returns a dictionary of (title, text) pairs indexed by section
        number."""
        content = {}
        
        # Get an ordered list of section indices.
        sections = sorted(self.content.keys())
        
        # Write each section.
        for section in sections:
            
            # String buffers for this section's content.
            sbuf = StringIO()
            ubuf = getwriter('utf-8')(sbuf)
            
            # Write each string in this section's content buffer.
            for s in self.content[section]:
                ubuf.write(s)
            
            # Set the content for this section.
            content[section] = (self.titles[section], ubuf.getvalue())
            
            # Close buffers.
            ubuf.close()
            sbuf.close()
        
        return content
    
    def _buffer(self):
        """
        Returns the content buffer for the current section.
        """
        # Make sure we have a buffer.
        if self.section not in self.content:
            self.content[self.section] = []
        return self.content[self.section]
    
    def _format(self, s):
        """
        Format the given string to the current section's content.
        """
        if self.section not in self.content:
            self.content[self.section] = []
        self.content[self.section].append(s)

    def format(self, obj):
        """
        Format the given node.
        """
        # Try to find an applicable format method in the node hierarchy.
        cls = obj.__class__
        while cls != ObjectType and cls.__name__ not in self.IGNORE_NODES:
            m = getattr(self, 'format' + cls.__name__, None)
            if m: return m(obj)
            cls = cls.__bases__[0]
        
        # We reached the supertype object, so give up.
        if cls == ObjectType:
            log.warn("No method to format object of type:", obj.__class__.__name__)

    def formatSection(self, obj):
        """
        Format the given section. The first child is the title, the rest are the
        nodes contained in this section. Some sections such as "References" are
        ignored.
        """
        # Check if ignore.
        title = obj.children[0].asText().strip()
        if title in self.IGNORE_SECTIONS: return
        
        # Set the offset if not already set.
        if self.levelOffset is None:
            self.levelOffset = obj.level - 1
        
        # Get the level of the last one and the new one, respectively.
        lastLevel = len(self.levels)
        sectionLevel = obj.level - self.levelOffset
        
        # If this section is nested, add a new level with one section.
        if sectionLevel > lastLevel:
            self.levels.append(1)
        
        # If same level, increment count for this level.
        elif sectionLevel == lastLevel:
            self.levels[-1] += 1
        
        # If popping back to a lower level, kill the rest and increment.
        else:
            self.levels = self.levels[0:sectionLevel]
            self.levels[-1] += 1
        
        # Set the section number and title.
        self.section = tuple(self.levels)
        self.titles[self.section] = title
        
        # Format the nested nodes.
        for x in obj.children[1:]:
            self.format(x)

    def formatNode(self, n):
        for x in n:
            self.format(x)

    def formatTagNode(self, t):
        """
        Format a "tag" node, which is an XML-style wikitext tag. The ``caption``
        attribute contains the name of the tag, and the children are the nodes
        within the tag.
        """
        # If a tag to ignore, don't format.
        if t.caption.lower() in self.IGNORE_TAGS: return
        
        # If quote, quote it.
        if t.caption.lower() == 'blockquote':
            return self.formatQuote(x)
        
        # Otherwise, print contents.
        for x in t:
            self.format(x)

    def formatNamedURL(self, obj):
        """
        Format a node that contains an external link with text. Only the text
        for the link should be formatted.
        """
        # If no children, then this is a numeric reference link, so ignore it.
        if obj.children:
            for x in obj.children:
                self.format(x)

    def formatParagraph(self, obj):
        """
        Format a paragraph node.
        """
        for x in obj:
            self.format(x)
        self._format(u'\n\n')
    
    # Ignore image links.
    def formatImageLink(self, obj):
        """Format an image link (does nothing)."""
        pass
    
    def formatLink(self, obj):
        """
        Format a link of some sort. Only the text of the link should be
        included.
        """
        if obj.children:
            
            # Remember our spot in the content buffer.
            buf = self._buffer()
            pos = len(buf)
            
            # Format the link text.
            for c in obj.children:
                self.format(c)
                
            # Go back and remove an initial # mark (for a section link).
            if buf[pos].startswith(u'#'):
                buf[pos] = buf[pos][1:]

    def formatText(self, t):
        """
        Format some text, which is contained in the ``caption`` attribute.
        """
        self._format(t.caption)

    def formatArticle(self, a):
        """
        Format the entire article, whose name is found in the ``caption``
        attribute and whose children are the contents. This should be the root
        node in the tree.
        """
        for x in a:
            self.format(x)
        
    def formatStyle(self, s):
        """
        Format a node with special formatting. The caption contains the
        wikitext format notation (e.g. "''" for bold), and the children are
        the formatted text contents.
        """
        for x in s:
            self.format(x)

    def formatItem(self, item):
        """
        Format an item in a list. Make sure to prefix with the item number
        if it exists.
        """
        if self.list_number:
            self._format(u'%d' % self.list_number)
        for x in item:
            self.format(x)
        self._format(u'\n')
        if self.list_number:
            self.list_number += 1

    def formatItemList(self, lst):
        """
        Format an itemized list, either ordered or not.
        """
        # Remember the old list number, then initialize it to 1 if need be.
        old_number = self.list_number
        if lst.numbered: self.list_number = 1
        self._format(u'\n')
        for x in lst:
            self.format(x)
        self._format(u'\n\n')
        self.list_number = old_number
    
    def formatQuote(self, quote):
        """
        Format a node as a quotation.
        """
        self._format(u'%s ' % QUOTE_BEGIN)
        for x in quote:
            self.format(quote)
        self._format(u'%s ' % QUOTE_END)

