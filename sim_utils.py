"""
A bunch of helper classes for computing various distributional similarity
measures with various feature types.
"""
import sys
from random import random
from math import sqrt
from itertools import chain
from collections import defaultdict

def parse_lda_entry(v):
    x = v.split(':')
    f = ':'.join(x[:-1])
#    try:
#        count = int(x[-1])
#    except:
#        sys.stderr.write('FAIL\n')
#        count = 0
    count = int(x[-1])
    return (f,count)


class SimilarityComputation:
    def compute_feature_vector(self, fv):
        return {}
    
    def dist(self, g1, g2):
        """
        This is the generalized distance for, e.g., weighted jaccard
        """
        top = 0
        bottom = 0
        for f in set(g1.keys()).union(g2.keys()):
            if f not in g1:
                bottom += g2[f]
            elif f not in g2:
                bottom += g1[f]
            else:
                bottom += max(g1[f], g2[f])
                top    += min(g1[f], g2[f])

        assert bottom > 0
        return top / float(bottom)

class RandomSimilarity(SimilarityComputation):
    def dist(self, g1, g1ks, g2, g2ks):
        return random()

class JaccardSimilarity(SimilarityComputation):
    def compute_feature_vector(self, fv):
        assert False  # implement feature merge
        return dict([(f.split(':')[0], 1) for f in fv[1:]])
    
    def dist(self, g1, g1ks, g2, g2ks):
        """ 
        This is a simpler subclassed distance for plain-ol jaccard
        """
        return len(g1ks.intersection(g2ks)) / float(len(g1ks.union(g2ks)))

class TFWeightedJaccardSimilarity(SimilarityComputation):
    def compute_feature_vector(self, fv):
        return dict([(f.split(':')[0], float(f.split(':')[1])) for f in fv[1:]])

class TFIDFWeightedJaccardSimilarity(SimilarityComputation):
    def __init__(self, RawFeatureVectors, AdditionalFeatureVectorFiles=[]):
        self.feature_idf = defaultdict(int)
        self.raw_feature_pipe = defaultdict(list)

        self.guy_to_features = {}

        sys.stderr.write('Computing idf...\n')
        if AdditionalFeatureVectorFiles:
            for (gv_id, gv) in enumerate(chain(*AdditionalFeatureVectorFiles)):
                gv = gv.strip('\n').split('\t')
                target = gv[0]
                assert not self.raw_feature_pipe.has_key(target)
                for (f, count) in map(parse_lda_entry, gv[1:]):
                    self.feature_idf[intern(f)] += count
                    self.raw_feature_pipe[target].append((intern(f), count))

                if gv_id % 100 == 0:
                    sys.stderr.write('  (additional) processed %d\n' % gv_id)

        sys.stderr.write('...on main part...\n')
        for (gv_id, gv) in enumerate(RawFeatureVectors):
            gv = gv.strip('\n').split('\t')
            target = gv[0]
            # assert not self.raw_feature_pipe.has_key(target)
            for (f, count) in map(parse_lda_entry, gv[1:]):
                self.feature_idf[intern(f)] += count

            if gv_id % 100 == 0:
                sys.stderr.write('  (main) processed %d\n' % gv_id)

    def compute_feature_vector(self, word, fv):
        features = {}
        for f in fv:
            if type(f) == tuple:
                (feature_name, value) = f
            else:
                (feature_name, value) = parse_lda_entry(f)

            if self.feature_idf.has_key(feature_name):
                features[intern(feature_name)] = float(value) / self.feature_idf[feature_name]
            #else:
            #    sys.stderr.write('skipped %s\n' % feature_name)

        return features

class TTestWeightedJaccardSimilarity(SimilarityComputation):
    def __init__(self, RawFeatureVectors, AdditionalFeatureVectorFiles=None):
        self.p_word = {}   # These two are for ttest
        self.p_attrib = defaultdict(float)
        self.total_attribs = 0

        to_kill = {}
        if AdditionalFeatureVectorFiles:
            # to_kill = find_heads_to_overwrite(AdditionalFeatureVectors)
            to_kill = set()
            sys.stderr.write('Not killing anyone!\n')

        self.guy_to_features = {}

        self.raw_feature_pipe = defaultdict(list)
        sys.stderr.write('Computing ttest (only over base!)...\n')
        # for gv in chain(RawFeatureVectors, AdditionalFeatureVectors):
        #for gv in AdditionalFeatureVectors:
        #    gv = gv.strip('\n').split('\t')
        #    self.p_word[gv[0]] = (len(gv) - 1)
        for (gv_id, gv) in enumerate(chain(*AdditionalFeatureVectorFiles)):
            gv = gv.strip('\n').split('\t')
            target = gv[0]
            assert not self.raw_feature_pipe.has_key(target)
            self.p_word[target] = (len(gv) - 1)
            for (f, count) in map(parse_lda_entry, gv[1:]):
                self.p_attrib[intern(f)] += count
                self.total_attribs += count
                self.raw_feature_pipe[target].append((intern(f), count))

            if gv_id % 100 == 0:
                sys.stderr.write('  (main) processed %d\n' % gv_id)
        sys.stderr.write('...on main part...\n')
        for (gv_id, gv) in enumerate(RawFeatureVectors):
            gv = gv.strip('\n').split('\t')
            target = gv[0]
            if target not in to_kill:
                # assert not self.raw_feature_pipe.has_key(target)
                self.p_word[target] = (len(gv) - 1)
                for (f, count) in map(parse_lda_entry, gv[1:]):
                    self.p_attrib[intern(f)] += count
                    self.total_attribs += count
                    # self.raw_feature_pipe[target].append((intern(f), count))

            if gv_id % 100 == 0:
                sys.stderr.write('  (main) processed %d\n' % gv_id)

        sys.stderr.write('normalizing...\n')
        # Now normalize
        for word in self.p_word.keys():
            self.p_word[word] /= float(self.total_attribs)
        for f in self.p_attrib.iterkeys():
            self.p_attrib[f] /= float(self.total_attribs)

        # RawFeatureVectors.seek(0)
        # AdditionalFeatureVectors.seek(0)

        # Make this opportunistic
        # sys.stderr.write('Caching headword features...\n')
        # First load the feature vectors for the headwords, we'll use the information
        # gathered there to filter
        # self.universe_features = set()
        # for fv in chain(RawFeatureVectors, AdditionalFeatureVectors):
        #     fv = fv.strip('\n').split('\t')
        #     word = fv[0]
        #     if word in HeadWords:
        #         self.guy_to_features[word] = self.compute_feature_vector(word, fv[1:])
        #         # self.universe_features.update(features.keys())


    def compute_feature_vector(self, word, fv):
        features = {}
        for f in fv:
            if type(f) == tuple:
                (feature_name, value) = f
            else:
                (feature_name, value) = parse_lda_entry(f)
            if self.p_attrib.has_key(feature_name):
                a = self.p_word[word]*self.p_attrib[intern(feature_name)]
                if a == 0:
                    features[intern(feature_name)] = 0
                    sys.stderr.write('HAK\n')
                else:
                    features[intern(feature_name)] = (float(value) / self.total_attribs - a) / sqrt(a)
            else:
                pass
        if not features:
            sys.stderr.write('GAH no features for %s\n' % (word))

        return features


def load_similarity_metric(Similarity, RawFeatureVectors, SecondaryRawFeatureVectorFiles=None):
    assert Similarity in ['jaccard', 'tf_weighted_jaccard', 'tfidf_weighted_jaccard', 'ttest_weighted_jaccard', 'random']

    if Similarity == 'jaccard':
        return JaccardSimilarity()
    elif Similarity == 'tf_weighted_jaccard':
        return TFWeightedJaccardSimilarity(RawFeatureVectors, SecondaryRawFeatureVectorFiles)
    elif Similarity == 'tfidf_weighted_jaccard':
        return TFIDFWeightedJaccardSimilarity(RawFeatureVectors, SecondaryRawFeatureVectorFiles)
    elif Similarity == 'ttest_weighted_jaccard':
        return TTestWeightedJaccardSimilarity(RawFeatureVectors, SecondaryRawFeatureVectorFiles)
    elif Similarity == 'random':
        return RandomSimilarity()

def find_heads_to_overwrite(new):
    """
    removing the overlapped originals, e.g. ask.4ZZZ would
    replace ask
    """
    heads_to_kill = set()
    for line in new:
        line = line.strip('\n').split('\t')
        heads_to_kill.add(line[0].split('ZZZ')[0])
        sys.stderr.write('Going to kill [%s] b/c of %s\n' %
                (line[0].split('ZZZ')[0], line[0]))

    new.seek(0)  # go back to the start
    return heads_to_kill

def merge_datasets(original, new):
    """
    Given a set of sparse LDA-style context vectors, merge it with the original set
    of context vectors, removing the overlapped originals, e.g. ask.4ZZZ would
    replace ask
    """

    assert False  # not using
    good_contexts = set()
    sys.stderr.write('loading good contexts...\n')
    # for o in original:
    #     good_contexts.update([x[0] for x in map(parse_lda_entry, o[1:])])
    sys.stderr.write('nope\n')

    sys.stderr.write('loaded %d good contexts.\n' % len(good_contexts))

    # TODO: for now no renaming

    heads_to_kill = set()
    for line in new:
        heads_to_kill.add(line[0].split('ZZZ')[0])
        sys.stderr.write('Going to kill [%s] b/c of %s\n' %
                (line[0].split('ZZZ')[0], line[0]))

    context_filter = bad_context_stripper(good_contexts)
    # return [[x[0]]+filter(context_filter, x[1:]) for x in new] + [x for x in original if x[0] not in heads_to_kill]
    return new + [x for x in original if x[0] not in heads_to_kill]


def bad_context_stripper(good_contexts):
    def _f(x):
        (h,_) = parse_lda_entry(x)
        return h in good_contexts



def group(lst, n):
    return [lst[i:i+n:] for i in range(0, len(lst), n)]
