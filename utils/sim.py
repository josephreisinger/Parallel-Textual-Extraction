"""
A bunch of helper classes for computing various distributional similarity
measures with various feature types.
"""
import sys
from random import random
from math import sqrt

class SimilarityComputation:
    def compute_feature_vector(self, fv):
        return {}
    
    def dist(self, g1, g1ks, g2, g2ks):
        """
        This is the generalized distance for, e.g., weighted jaccard
        """
        top = 0
        bottom = 0
        for f in g1ks.union(g2ks):
            if not f in g1ks:
                bottom += g2[f]
            elif not f in g2ks:
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
    def __init__(self, RawFeatureVectors):
        self.feature_idf = {}

        # First compute the idf if necessary
        sys.stderr.write('Computing idf...\n')
        for gv in RawFeatureVectors:
            for v in gv[1:]:
                (f, _, score) = v.partition(':')
                self.feature_idf.setdefault(f, 0)
                self.feature_idf[f] += 1

    def compute_feature_vector(self, fv):
        features = {}
        for f in fv[1:]:
            (key, _, value) = f.partition(':')
            features[key] = float(value) / self.feature_idf[key]

        return features

class TTestWeightedJaccardSimilarity(SimilarityComputation):
    def __init__(self, RawFeatureVectors):
        self.p_word = {}   # These two are for ttest
        self.p_attrib = {} 
        self.total_attribs = 0

        sys.stderr.write('Computing ttest...\n')
        for gv in RawFeatureVectors:
            self.p_word[gv[0]] = (len(gv) - 1)
            for v in gv[1:]:
                (f, _, count) = v.partition(':')
                self.p_attrib.setdefault(f, 0)
                self.p_attrib[f] += int(count)
                self.total_attribs += int(count)
        # Now normalize
        for gv in RawFeatureVectors:
            self.p_word[gv[0]] /= float(self.total_attribs)
        for f in self.p_attrib.iterkeys():
            self.p_attrib[f] /= float(self.total_attribs)

    def compute_feature_vector(self, fv):
        features = {}
        for f in fv[1:]:
            (key, _, value) = f.partition(':')
            a = self.p_word[fv[0]]*self.p_attrib[key]
            features[key] = (float(value) / self.total_attribs - a) / sqrt(a)
            # sys.stderr.write('%f\n' % features[key])

        return features


def load_similarity_metric(Similarity, RawFeatureVectors):
    assert Similarity in ['jaccard', 'tf_weighted_jaccard',
    'tfidf_weighted_jaccard', 'ttest_weighted_jaccard', 'random']

    if Similarity == 'jaccard':
        return JaccardSimilarity()
    elif Similarity == 'tf_weighted_jaccard':
        return TFWeightedJaccardSimilarity(RawFeatureVectors)
    elif Similarity == 'tfidf_weighted_jaccard':
        return TFIDFWeightedJaccardSimilarity(RawFeatureVectors)
    elif Similarity == 'ttest_weighted_jaccard':
        return TTestWeightedJaccardSimilarity(RawFeatureVectors)
    elif Similarity == 'random':
        return RandomSimilarity()

def merge_datasets(original, new):
    """
    Given a set of sparse LDA-style context vectors, merge it with the original set
    of context vectors, removing the overlapped originals, e.g. ask.4ZZZ would
    replace ask
    """

    # TODO: for now no renaming

    heads_to_kill = set()
    for line in new:
        heads_to_kill.add(line[0].split('ZZZ')[0])
        sys.stderr.write('Going to kill [%s] b/c of %s\n' %
                (line[0].split('ZZZ')[0], line[0]))

    return new + [x for x in original if x[0] not in heads_to_kill]

