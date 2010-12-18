#import psyco
import logging, os
import codecs 
from bz2 import *
import re
from ufo import *
from string import lower
from collections import defaultdict
from sim_utils import parse_lda_entry

ClientRegistry.Port = 62580 # port to connect to the mothership
Mapper.Port         = 62581 # port for the RPC server on the child

# Output level
logger.setLevel( logging.INFO )

MinDocLength = 100

# MinContextOccurranceThreshold = 0
# MinPerWordUniqueContextThreshold = 0
# HeadWordsFile = 'wikipedia-1000w.headwords50.trimmed'
# HeadWordsFile = 'wikipedia-headwords-17k.txt'
# HeadWordsFile = 'wordsim-353.heads.bz2'
HeadWordsFile='contextVectorWords.cut50.txt.bz2'
#HeadWordsFile = 'evocation.heads.bz2'
#HeadWordsFile = 'turk.heads'
# HeadWordsFile = 'usim.heads'
# SourceDataType = 'gigaword'
# TermFreqFile = 'gigaword-term-doc-freq-5v.txt.bz2'
SourceDataType = 'wikipedia-strict'
TermFreqFile = 'wikipedia-term-doc-freq-5v.txt.bz2'
MinDocFreq = 5

# Choosing 'combined' can result in space savings during the mapper step, and
# results in one set of features per head word, rather than one set per context
OutputType = 'combined'
sys.stderr.write('combining outputs\n')
# OutputType = 'occurrence'
assert OutputType in ['combined', 'occurrence']

# ContextCountsFile = 'context-count-wikipedia-1000w-min10-1lr-wikipedia-1000w.headwords50.trimmed.txt'
ContextCountsFile = None
# StopWordsFile = 'empty.txt.bz2'
StopWordsFile = 'stopwords2.txt.bz2'
# StopWordsFile = 'stopwords.txt.bz2'

# ContextSize = 25
ContextSize = 5
ContextType = 'uni'  # full contexts is just the previous ContextSize words
#ContextType = 'raw'  # full contexts is just the previous ContextSize words
# ContextSize = 1
# ContextType = 'lr'  # currently lr or uni
assert ContextType in ['uni', 'lr', 'raw']

Phase = 2  # Phase 1 counts the context occurrences

BannedArticleTypes = ['Image:', 'Wikipedia:', 'Template:', 'Category:']


if Phase == 1:
    BZ2ShardedMothership.OutputFile = 'context-count-wikipedia-%dw-min%s-%d%s-%s.txt.bz2' % (MinDocLength,\
                                                          MinContextOccurranceThreshold,\
                                                          ContextSize, ContextType,\
                                                          HeadWordsFile)
else:
    BZ2ShardedMothership.OutputFile = '%s-%dw-min%df-%s-%d%s-%s-contexts.txt.bz2' % (SourceDataType, MinDocLength,\
                                                          MinDocFreq,\
                                                          # MinPerWordUniqueContextThreshold,\
                                                          OutputType,\
                                                          ContextSize, ContextType,\
                                                          HeadWordsFile)


class MyMapper(Mapper):
    def initialize(self, args):
        """ 
        Read in the set of headwords
        """
        self.head_words = set()
        logger.info('Reading in head words...')
        # reader = codecs.getreader('utf8')(open(HeadWordsFile))
        # reader = codecs.open(HeadWordsFile, 'r', 'utf8', errors='replace')
        reader = codecs.getreader('utf8')(BZ2File(HeadWordsFile))
        for line in reader.readlines():
            word = line.strip().split('\t')[0]
            self.head_words.add(word)
        reader.close()

        self.stop_words = set()
        logger.info('Reading in stop words...')
        reader = codecs.getreader('utf8')(BZ2File(StopWordsFile))
        for line in reader.readlines():
            word = line.replace('\n', '')

            if word not in self.head_words:
                self.stop_words.add(word)
        reader.close()
   
        if Phase == 2:
            self.good_contexts = set()
            if ContextCountsFile:
                logger.info('Reading in good contexts...')
                reader = codecs.getreader('utf8')(open(ContextCountsFile))
                for line in reader.readlines():
                    context = line.split('\t')[0]
                    self.good_contexts.add(context)
                reader.close()

            # Read in the term frequency information
            logger.info('Reading in term freq...')
            self.unigram_term_freq = defaultdict(int)
            self.unigram_doc_freq = defaultdict(int)
            reader = codecs.getreader('utf8')(BZ2File(TermFreqFile))
            for line in reader.readlines():
                (word, tf, df) = line.split('\t')
                self.unigram_term_freq[word] = int(tf)
                self.unigram_doc_freq[word] = int(df)
            reader.close()

    def map(self, token):
        # Stick these in here to hide them from ungoliant
        # from utils.cleaner import *
        from utils import get_document_iterator

        logger.info('Mapping token [%r]' % token)

        combined_counts = defaultdict(lambda: defaultdict(int))
        for (doc_count, (current_title, document, _)) in get_document_iterator(SourceDataType, token):
            words = document.replace('<CR>', ' ').split()
            # print current_title, words
            if len(words) > MinDocLength:
                words = filter(lambda x: x not in self.stop_words, words)
                for (i,w) in enumerate(words):
                    if w in self.head_words:
                        try:
                            if ContextType == 'lr':
                                context = '_'.join(words[max(0,i-ContextSize):i] + '_<>_' +
                                        words[i+1:min(i+1+ContextSize, len(words))])
                            elif ContextType == 'uni':
                                buffer = defaultdict(int)
                                for ww in [x for x in words[max(0,i-ContextSize-1):i] if self.unigram_doc_freq[x] >= MinDocFreq]:
                                    buffer[ww] += 1
                                for ww in [x for x in words[i+1:min(i+ContextSize+1, len(words))] if self.unigram_doc_freq[x] >= MinDocFreq]:
                                    buffer[ww] += 1
                            elif ContextType == 'raw':
                                buffer = words[max(0,i-ContextSize):i]
                                buffer.append('###%s###' % words[i])
                                buffer.extend(words[i+1:min(i+ContextSize, len(words))])

                            if Phase == 1:
                                self.output('%s\t%s' % (context, w))
                            elif Phase == 2:
                                assert not self.good_contexts
                                if OutputType == 'combined':
                                    assert not ContextType == 'raw'
                                    for k,v in buffer.iteritems():
                                        combined_counts[w][intern(k.encode('ascii'))] += v
                                elif OutputType == 'occurrence':
                                    if ContextType == 'raw':
                                        context = u' '.join([k.decode('utf8') for k in buffer])
                                        self.output(u'%s\t%s' % (w, context))
                                    else:
                                        context = u'\t'.join([u'%s:%d' % (k.decode('utf8'),v) for k,v in buffer.iteritems()])
                                        self.output(u'%s\t%s' % (w, context))
                        except UnicodeEncodeError:
                            sys.stderr.write('FAILED\n')

            if doc_count % 100 == 0:
                logger.info('Processed %d documents' % doc_count)

        # Do intermediate combining for space efficiency
        if OutputType == 'combined':
            for w, contexts in combined_counts.iteritems():
                context = u'\t'.join([u'%s:%d' % (k,v) for k,v in contexts.iteritems()])
                self.output(u'%s\t%s' % (w, context))


        # Return success
        return True

    def reduce(self, data_heap):
        """
        Sum-reducer, for all the occurances of word/context, just make a count. This is where we can
        do things like thresholding.
        """
        if Phase == 1:
            self.phase_one_reduce(data_heap)
        elif Phase == 2:
            self.phase_two_reduce(data_heap)


    def phase_one_reduce(self, data_heap):
        current = None
        occurrences = set()
        while data_heap:
            (context, _, word) = heappop(data_heap).partition('\t')
            if current and context != current:
                if len(occurrences) >= MinContextOccurranceThreshold:
                    self.output('%s\t%s' % (current, len(occurrences)))
                else:
                    #self.output('POOP %s\t%s' % (current, len(occurrences)))
                    pass
                occurrences = set()
            
            current = context
            occurrences.add(word)

    def phase_two_reduce(self, data_heap):
        """
        This one collects word/context pairs and outputs to the LDA format
        """
        current = None
        occurrences = defaultdict(int)
        while data_heap:
            if OutputType == 'occurrence':
                self.output(heappop(data_heap))
            else:
                tokens = heappop(data_heap).strip().split('\t')
                word, contexts = tokens[0], map(parse_lda_entry, tokens[1:])

                if current and word != current:
                    self.output('%s\t%s' % (current, '\t'.join(['%s:%d' % (k,v)
                        for (k,v) in occurrences.iteritems()])))
                    occurrences = defaultdict(int)

                current = word
                for w,c in contexts:
                    occurrences[w] += c

        self.output('%s\t%s' % (current, '\t'.join(['%s:%d' % (k,v) for (k,v) in occurrences.iteritems()])))
                
UFOMapper     = MyMapper
UFOMothership = BZ2ShardedMothership

if __name__ == '__main__':
    #psyco.full()
    start_ufo(UFOMapper, UFOMothership)
