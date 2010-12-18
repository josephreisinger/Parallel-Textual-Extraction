from mwlib.namespace_langs import lang_ns_data as _lang_ns_data

NS_MEDIA          = -2
NS_SPECIAL        = -1
NS_MAIN           =  0
NS_TALK           =  1
NS_USER           =  2
NS_USER_TALK      =  3
NS_PROJECT        =  4
NS_PROJECT_TALK   =  5
NS_FILE           =  6
NS_IMAGE          =  6
NS_FILE_TALK      =  7
NS_IMAGE_TALK     =  7
NS_MEDIAWIKI      =  8
NS_MEDIAWIKI_TALK =  9
NS_TEMPLATE       = 10
NS_TEMPLATE_TALK  = 11
NS_HELP           = 12
NS_HELP_TALK      = 13
NS_CATEGORY       = 14
NS_CATEGORY_TALK  = 15

namespace_maps = {}

class nsmapper(dict):
    pass

def splitname(name, defaultns=0, nsmap=None):
    if nsmap is None:
        nsmap = namespace_maps['default']

    if name.startswith(':'):
        name = name[1:]

    name = name.replace(" ", "_")

    
    res = None
    
    if ":" in name:
        ns, partial_name = name.split(":", 1)
        ns = nsmap.get(ns.lower(), None)
        
        if ns is not None:
            name = partial_name
        else:
            ns = defaultns
    else:
        ns = defaultns

    if ns==0:
        res = (ns, name, name)
    else:
        res = (ns, name, "%s:%s" % (nsmap.ns2name[ns], name))
        
    return res
              
def add_namespace_map(key, lang, project_name, extras={}):
    ns_data = _lang_ns_data[lang]
    
    res = nsmapper()    
    res.ns2name = ns2name = {}
    
    def insertstring(k, v):
        res[k.replace(" ", "_").lower()] = v
        ns2name[v] = k.replace(" ", "_")
        
    def insert(k, v):
        if isinstance(k, basestring):
            insertstring(k,v)
        else:
            for x in k:
                insertstring(x, v)
        
    for k, v in zip(ns_data, _lang_ns_data_keys):
        insert(k,v)

    insert(project_name, NS_PROJECT)
    
    if '%s' in ns_data[-1]:
        insert(ns_data[-1] % project_name, NS_PROJECT_TALK)
    else:
        insert(ns_data[-1], NS_PROJECT_TALK)
    for k, v in extras.items():
        insert(k,v)

    insert("", 0)
    
        
    namespace_maps[key] = res

_lang_ns_data_keys = [
    NS_TALK, NS_USER, NS_USER_TALK, NS_FILE, NS_FILE_TALK,
    NS_MEDIAWIKI, NS_MEDIAWIKI_TALK, NS_TEMPLATE, NS_TEMPLATE_TALK,
    NS_HELP, NS_HELP_TALK, NS_CATEGORY, NS_CATEGORY_TALK, NS_SPECIAL, NS_MEDIA
]

add_namespace_map('enwiki', 'en', u'Wikipedia',
        {u'Portal': 100, u'Portal_Talk': 101})
add_namespace_map('dewiki', 'de', u'Wikipedia',
        {u'Portal': 100, u'Portal_Diskussion': 101})
for lang in _lang_ns_data:
    add_namespace_map('%s+en_mw' % lang, lang, u'MediaWiki', namespace_maps['enwiki'])
del lang

namespace_maps['default'] = nsmapper(namespace_maps['enwiki'].items() + namespace_maps['dewiki'].items())
namespace_maps['default'].ns2name = namespace_maps['enwiki'].ns2name


# external wikis:

dummy_interwikimap = {
    'wikipedia': 'wikipedia',
    'w': 'wikipedia',
    'wiktionary': 'wiktionary',
    'wikt': 'wiktionary',
    'wikinews': 'wikinews',
    'n': 'wikinews',
    'wikibooks': 'wikibooks',
    'b': 'wikibooks',
    'wikiquote': 'wikiquote',
    'q': 'wikiquote',
    'wikisource': 'wikisource',
    's': 'wikisource',
    'wikispecies': 'wikispecies',
    'species': 'wikispecies',
    'v': 'wikiversity',
    'wikimedia': 'wikimedia',
    'foundation': 'wikimedia',
    'commons': 'commons',
    'meta': 'meta',
    'm': 'meta',
    'incubator': 'incubator',
    'mw': 'mw',
    'mediazilla': 'mediazilla',
    
    'wikitravel': 'wikitravel',
}
