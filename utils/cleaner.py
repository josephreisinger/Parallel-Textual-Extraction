import re
from parse_wikipedia import *

def strip_html(text):
    """
    ##
    # Removes HTML markup from a text string.
    #
    # @param text The HTML source.
    # @return The plain text.  If the HTML source contains non-ASCII
    #     entities or character references, this is a Unicode string.
    """
    def fixup(m):
        text = m.group(0)
        if text[:1] == "<":
            return "" # ignore tags
        if text[:2] == "&#":
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        elif text[:1] == "&":
            import htmlentitydefs
            entity = htmlentitydefs.entitydefs.get(text[1:-1])
            if entity:
                if entity[:2] == "&#":
                    try:
                        return unichr(int(entity[2:-1]))
                    except ValueError:
                        pass
                else:
                    # return unicode(entity, "utf8")
                    return unicode(entity, "iso-8859-1")
        return text # leave as is
    return re.sub("(?u)(?s)<[^>]*>|&#?\w+;", fixup, text)


def clean_document(buffer):
    # TODO: this part is really dirty. Need to adjust all these hacks at some point.
    # clean = strip_html(buffer).replace('==',' ').replace('[[',' ').replace(']]',' ').replace('|',' ').replace('[',' ').replace(']',' ')
    clean = buffer.replace('==',' ').replace('[[',' ').replace(']]',' ').replace('|',' ').replace('[',' ').replace(']',' ')
    # clean = strip_html(clean)  # do this twice since wikitext hides extra html
    clean = clean.replace('*','').replace('{','').replace('}','').replace('=','').replace(',','').replace('.','').replace(':',' ').replace('\n',' ')
    clean = clean.replace(';','').replace('(',' ').replace(')',' ').replace('?',' ').replace('"',' ').replace('\t',' ').replace('\'',' ').replace('/','')

    return clean

link_catcher = re.compile('\[\[.*?\]\]')
def re_extract_links(buffer, current_title=None):
    return re.findall(link_catcher, buffer)

def re_extract_categories(buffer, current_title):
    if current_title.startswith('Category:'):
        # print 'extract category page [%s]' % current_title.encode('utf8','replace')
        return re_extract_category_page_categories(buffer.strip())
    else:
        return re_extract_normal_page_categories(buffer)

# On category pages, we want to only extract category links at the beginning of
# the line (these are basically the is-a relations; other category links might
# be extraneous)
category_page_category_catcher = re.compile('^\[\[Category:.*?\]\]', re.MULTILINE)
def re_extract_category_page_categories(buffer):
    print '****%s****' % buffer.encode('utf8','replace')
    return re.findall(category_page_category_catcher, buffer)

normal_page_category_catcher = re.compile('\[\[Category:.*?\]\]')
def re_extract_normal_page_categories(buffer):
    return re.findall(normal_page_category_catcher, buffer)

def replace_named_reference(s):
    if s.group(0).find('|') >= 0:
        return s.group(0).split('|')[1].replace(']]','')
    else:
        return s.group(0).replace('[[','').replace(']]','')

image_catcher = re.compile('\[\[Image.*?\]\]')
ref_catcher = re.compile('\[\[.*?\]\]')
table_catcher = re.compile('\{*?\}')
section_catcher = re.compile('\{\{.*?\}\}')

def strip_wiki_stuff(buffer):
    # [[Image:K1200gt600.jpg|thumb|right|K1200GT]]
    # [[differential (mechanics)|differential]]
    # {| class=&quot;wikitable&quot; }
    # {{main|History of BMW}}
    buffer = re.sub(image_catcher, '', buffer)
    buffer = re.sub(ref_catcher, replace_named_reference, buffer)
    buffer = re.sub(section_catcher, '', buffer)
    buffer = re.sub(table_catcher, '', buffer)

    return buffer
    

def clean_document_keep_punctuation(buffer, title, space_punctuation=False):
    # TODO: this part is really dirty. Need to adjust all these hacks at some point.
    # clean = strip_html(buffer).replace('\n', ' ')
    clean = buffer.replace('\n', ' <CR> ')
    clean = strip_wiki_stuff(clean)
    clean = clean.replace('==',' ').replace('[[',' ').replace(']]',' ').replace('|',' ').replace('[',' ').replace(']',' ')
    # clean = strip_html(clean)  # do this twice since wikitext hides extra html
    clean = clean.replace('*','').replace('{','').replace('}','').replace('=','').replace('\n',' ').replace('\t',' ').replace('/','')

    if space_punctuation:
        clean = clean.replace(';',' ; ').replace('(',' ( ').replace(')',' ) ').replace('?',' ? ').replace('"',' " ').replace(',',' , ').replace('.',' . ').replace(':',' : ')
    # clean = clean.replace(';',' ; ').replace('(',' ( ').replace(')',' ) ').replace('?',' ? ').replace('"',' " ').replace('\t',' ').replace('\'',' ').replace('/','')

    return re.sub('\s+', ' ', clean)

interior_capital = re.compile('.+[A-Z]')
interior_number = re.compile('[a-z,A-Z]+\d')
start_number = re.compile('^\d')
start_dash = re.compile('^\-')
start_dollar = re.compile('^\$')
#interior_non_alpha = re.compile('[a-z,A-Z]+\d')
non_content = re.compile('[http|url|htm|www|php|text\-align]')

def match_non_content(s):
    """
    removes words that don't indicate syntactic or semantic content
    """
    # return not re.match(interior_capital, s) and not re.match(non_content, s) and not re.match(interior_number, s)
    return not re.match(start_number, s) and not re.match(start_dash, s) and not re.match(start_dollar, s)

def filter_non_content(words):
    return filter(match_non_content, words)


def match_non_alpha(w):
    return not re.search(non_alpha, w)

def filter_strict(words):
    return filter(match_non_content, filter(match_non_alpha, words))



def clean_wikipedia_documents(f, BannedArticleTypes, filter_extraneous=False,
        space_punctuation=False):
    return map_over_wikipedia_documents(clean_document_keep_punctuation,
            f, BannedArticleTypes, filter_extraneous, parse_mediawiki=True,
            space_punctuation=space_punctuation) 

def clean_gigaword_documents(f, space_punctuation=True):
    return map_over_gigaword_documents(clean_document_keep_punctuation, f,
            space_punctuation=space_punctuation)

def extract_links(f, BannedArticleTypes):
    return map_over_wikipedia_documents(re_extract_links, f, BannedArticleTypes,
            parse_mediawiki=False) 

def extract_categories(f, BannedArticleTypes):
    return map_over_wikipedia_documents(re_extract_categories, f, BannedArticleTypes,
            parse_mediawiki=False) 

re_redirect = re.compile('\#redirect|\#REDIRECT')

def map_over_wikipedia_documents(function, f, BannedArticleTypes,
        filter_extraneous=False, parse_mediawiki=True, space_punctuation=False): 
    """
    This generator returns cleaned documents read from the input stream 
    one at a time.
    """
    inside_body = False  # are we inside the body text or not
    buffer = []

    current_title = ''

    while True:
        line = f.readline()
        
        if line:
            if line.find('<title>') >= 0:
                current_title = strip_html(line.strip())
            if line.find('<text') >= 0:
                inside_body = True

            if inside_body:
                buffer.append(line)

            if line.find('</text>') >= 0:
                inside_body = False

                if not [1 for x in BannedArticleTypes if current_title and current_title.startswith(x)]:
                    # print strip_html(strip_html(''.join(buffer))).encode('utf8','ignore')
                    # print (''.join(buffer)).encode('utf8','replace')
                    temp = strip_html(strip_html(''.join(buffer))) 

                    flags = set()
                    if re.findall(re_redirect, temp[:20]):
                        flags.add('REDIRECT')

                    try:
                        if parse_mediawiki:
                            temp = parse_raw_mediawiki(current_title, temp)
                    except (IndexError, ImportError, RuntimeError):
                        # print (''.join(buffer)).encode('utf8','ignore')
                        sys.stderr.write('-------------------- failed to MW parse [%s]\n' % current_title.encode('utf8','ignore'))
                        buffer = []
                        continue
                    clean = function(temp, current_title,
                            space_punctuation=space_punctuation)

                    if filter_extraneous:
                        clean = ' '.join(filter_non_content(clean.split(' ')))
                    yield (current_title, clean, flags)

                # Reset for the next document
                buffer = []
        else:
            f.close()
            break

def map_over_gigaword_documents(function, f, space_punctuation=True):
    """
    This generator returns cleaned documents read from the input stream 
    one at a time.

    <DOC id="NYT_ENG_19960701.0002" type="story" >
    <HEADLINE>
    CATCH A TROUT AND RELEASE IT INTO A PAN
    </HEADLINE>
    <DATELINE>
     (BC-FOOD-OWEN-COLUMN-SPI)
     </DATELINE>
     <TEXT>

    """
    inside_body = False  # are we inside the body text or not
    inside_title = False
    buffer = []

    current_title_buffer = []
    current_title = ''

    while True:
        line = f.readline()
        
        if line:
            if line.find('<HEADLINE>') >= 0:
                inside_title = True
            if line.find('<TEXT>') >= 0:
                inside_body = True

            assert not (inside_title and inside_body)

            if inside_title:
                current_title_buffer.append(line)
            if inside_body:
                buffer.append(line)

            if line.find('</HEADLINE>') >= 0:
                inside_title = False
                current_title = u''.join(current_title_buffer).replace('\n',' ').replace('<HEADLINE>','').replace('</HEADLINE>','').strip()
                current_title_buffer = [] 

            if line.find('</TEXT>') >= 0:
                inside_body = False

                try:
                    # print strip_html(strip_html(''.join(buffer))).encode('utf8','ignore')
                    # print (''.join(buffer)).encode('utf8','replace')
                    temp = strip_html(strip_html(u''.join(buffer))) 
                    clean = function(temp, current_title,
                            space_punctuation=space_punctuation)
                except (IndexError, ImportError):
                    # print (''.join(buffer)).encode('utf8','ignore')
                    sys.stderr.write('-------------------- failed to parse [%s]\n' % current_title.encode('utf8','ignore'))
                    buffer = []
                    continue

                yield (current_title, clean, [])

                # Reset for the next document
                buffer = []
        else:
            f.close()
            break
