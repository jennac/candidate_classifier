# Author: Peter Prettenhofer <peter.prettenhofer@gmail.com>
#         Olivier Grisel <olivier.grisel@ensta.org>
#         Mathieu Blondel <mathieu@mblondel.org>
#         Lars Buitinck <L.J.Buitinck@uva.nl>
# License: Simplified BSD

import logging
import numpy as np
from optparse import OptionParser
import sys, csv, re, math, operator
from time import time
from pylab import *
from pylab import clf as clearfig

from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.linear_model import RidgeClassifier
from sklearn.svm import LinearSVC
from sklearn.linear_model import SGDClassifier
from sklearn.linear_model import Perceptron
from sklearn.naive_bayes import BernoulliNB, MultinomialNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestCentroid
from sklearn.utils.extmath import density
from sklearn import metrics

# Display progress logs on stdout
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')


# parse commandline arguments
op = OptionParser()
op.add_option("--full",action="store_true", dest="full",help="make full")
op.add_option("--report",
              action="store_true", dest="print_report",
              help="Print a detailed classification report.")
op.add_option("--chi2_select",
              action="store", type="int", dest="select_chi2",
              help="Select some number of features using a chi-squared test")
op.add_option("--confusion_matrix",
              action="store_true", dest="print_cm",
              help="Print the confusion matrix.")
op.add_option("--top10",
              action="store_true", dest="print_top10",
              help="Print ten most discriminative terms per class"
                   " for every classifier.")

(opts, args) = op.parse_args()
if len(args) > 0:
    op.error("this script takes no arguments.")
    sys.exit(1)

print __doc__
op.print_help()
print


###############################################################################
# Load some categories from the training set
"""
categories = [
    'alt.atheism',
    'talk.religion.misc',
    'comp.graphics',
    'sci.space',
]
"""
# Uncomment the following to do the analysis on all the categories
#categories = None

class SiteData:
    def __init__(self, filename, categories, candidate_data_dict):
        self.data = []
        self.target = []
        self.link = []
        self.target_names = set()

        category_dict = dict((categories[i],i) for i in range(len(categories)))
        transform = lambda cl: 'TrueCombined' if cl=='ChildCombined' or cl=='ParentCombined' else cl
        csvr = csv.DictReader(open(filename))
        for l in csvr:
            if not candidate_data_dict.has_key(l['uid']):
                continue
            candidate_data = dict(candidate_data_dict[l['uid']])
            candidate_data.update({'uid':l['uid'],'link':l['link'],'sitetext':l['sitetext']})
            self.data.append(repr(candidate_data))
            self.target.append(category_dict[transform(l['class'])])
            self.target_names.add(transform(l['class']))
            self.link.append(l['link'])
        self.data.append(repr({'uid': '',
                               'link': 'websitemywebsite',
                               'name': '',
                               'state': '',
                               'office_level': '',
                               'electoral_district_type': '',
                               'electoral_district_name': '',
                               'sitetext': ''}
                          ))
        self.target.append(2)
        self.target_names.add('grooon')
        self.link.append('nothing')

#dict_columns = ('name','electoral_district_type','electoral_district_name','state','office_level')
#partial_candidate_dict = dict((l['identifier'],dict((dc, l[dc]) for dc in dict_columns)) for l in csv.DictReader(open('twitter/twittercands.csv')))
#full_candidate_dict = dict((l['identifier'],dict((dc, l[dc]) for dc in dict_columns)) for l in csv.DictReader(open('twitter/fulltwittercands.csv')))


translate_level = {
    'country': 'state',
    'administrativeArea1': 'state',
    'administrativeArea2': 'county',
    'locality': 'city',
    'regional': 'state',
    'special': 'state'
}

#TODO REWRITE
partial_candidate_dict = {}
for row in csv.DictReader(open('fb/facebook_training.csv', 'rU')):
    partial_candidate_dict[row['UID']] = {
        'name': row['Candidate Name'],
        'electoral_district_type': row['type'],
        'electoral_district_name': row['name'],
        'state': row['State'],
        'office_level': translate_level[row['level']]
    }

full_candidate_dict = {}
for row in csv.DictReader(open('fb/facebook_nosocial.csv', 'rU')):
    full_candidate_dict[row['UID']] = {
        'name': row['Candidate Name'],
        'electoral_district_type': row['type'],
        'electoral_district_name': row['name'],
        'state': row['State'],
        'office_level': translate_level[row['level']]
    }


categories = [
        'FalseCombined',
        'TrueCombined',
        'grooon',
        ]
print "Loading 20 newsgroups dataset for categories:"
print categories if categories else "all"

data_train = SiteData('twitter/TRAINING_TWITTER.csv', categories, partial_candidate_dict)
data_test = SiteData('twitter/TEST_TWITTER.csv', categories, partial_candidate_dict)

if opts.full:
    data_full = []
    num_full = 20
    for file_counter in range(num_full):
        data_full.append(SiteData('twitter/srsplit/SPLIT_{}_fulltwittersearch_results_combined.csv'.format(file_counter), categories, full_candidate_dict))
"""
data_train = fetch_20newsgroups(subset='train', categories=categories,
                               shuffle=True, random_state=42)

data_test = fetch_20newsgroups(subset='test', categories=categories,
                              shuffle=True, random_state=42)
"""
print 'data loaded'
import conversions as conv
from utffile import utffile
special_terms = []
vocabulary = []
basic_vectorizer = TfidfVectorizer(sublinear_tf=True, max_df=0.5, use_idf=False,
                             stop_words='english')
basic_analyze = basic_vectorizer.build_analyzer()
with utffile('searchterms.csv') as f:
    for s in f:
        if s.startswith('<'):
            special_terms.append(s.strip('<>'))
        else:
            vocabulary.append(s.decode('utf-8').strip())

def analyze(s):
    d=eval(s)
    special_keys = []
    name = d['name']
    electoral_district_type = d['electoral_district_type']
    electoral_district_name = d['electoral_district_name']
    state = d['state']
    link = d['link']
    text = d['sitetext'].lower().decode('utf-8')
    name, last,first = conv.clean_name(name)
    for v in vocabulary:
        special_keys += [conv.search_to_feature_key(v)]*text.count(v.lower())
        text.replace(v.lower(),'')
    special_keys += [conv.search_to_feature_key('name')]*text.count(name.lower())
    special_keys += [conv.search_to_feature_key('last')]*text.count(last.lower())
    special_keys += [conv.search_to_feature_key('first')]*text.count(first.lower())
    special_keys += [conv.search_to_feature_key('lastfor')]*text.count(last.lower()+' for')
    special_keys += [conv.search_to_feature_key('lastfor')]*text.count(last.lower()+'for')
    special_keys += [conv.search_to_feature_key('lastfor')]*text.count(last.lower()+'4')
    special_keys += [conv.search_to_feature_key('votelast')]*text.count('vote'+last.lower())
    special_keys += [conv.search_to_feature_key('forstate')]*text.count('for '+state.lower())
    special_keys += [conv.search_to_feature_key('reelectlast')]*text.count('reelect ' +name.lower())
    special_keys += [conv.search_to_feature_key('reelectlast')]*text.count('reelect ' +last.lower())
    special_keys += [conv.search_to_feature_key('reelectlast')]*text.count('re elect ' +name.lower())
    special_keys += [conv.search_to_feature_key('reelectlast')]*text.count('re elect ' +last.lower())
    special_keys += [conv.search_to_feature_key('reelectlast')]*text.count('re-elect ' +name.lower())
    special_keys += [conv.search_to_feature_key('reelectlast')]*text.count('re-elect ' +last.lower())
    special_keys += [conv.search_to_feature_key('electlast')]*text.count('elect ' +name.lower())
    special_keys += [conv.search_to_feature_key('electlast')]*text.count('elect ' +last.lower())
    special_keys += [conv.search_to_feature_key('votelast')]*text.count('vote '+last.lower())
    special_keys += [conv.search_to_feature_key('voteforlast')]*text.count('vote for '+last.lower())
    special_keys += [conv.search_to_feature_key('voteforlast')]*text.count('votefor'+last.lower())
    special_keys += [conv.search_to_feature_key('voteforlast')]*text.count('vote4'+last.lower())
    text.replace(name.lower(),'')
    text.replace(last.lower(),'')
    text.replace(first.lower(),'')
    special_keys += [conv.search_to_feature_key('electoral_district_type')]*sum(text.count(edt.lower()) for edt in conv.district_type_dict[electoral_district_type])
    special_keys += [conv.search_to_feature_key('officename')]*sum(text.count(on.lower()) for on in conv.office_names)
    special_keys += [conv.search_to_feature_key('electoral_district_name')]*text.count(electoral_district_name.lower())
    special_keys += [conv.search_to_feature_key('state')]*text.count(state.lower())
    name_key = conv.search_to_feature_key('name')
    last_key = conv.search_to_feature_key('last')
    first_key = conv.search_to_feature_key('first')
    #print 'name keys ',special_keys.count(name_key),'last keys ', special_keys.count(last_key), 'first keys ', special_keys.count(first_key)
    return basic_analyze(text) + special_keys





#categories = data_train.target_names    # for case categories == None

print "%d documents (training set)" % len(data_train.data)
print "%d documents (testing set)" % len(data_test.data)
print "%d categories" % len(categories)
print

# split a training set and a test set
y_train, y_test = data_train.target, data_test.target

print "Extracting features from the training dataset using a sparse vectorizer"
t0 = time()
vectorizer = TfidfVectorizer(sublinear_tf=True, analyzer=analyze, use_idf=False, max_df=0.5,
                             stop_words='english')
X_train = vectorizer.fit_transform(data_train.data)
print "done in %fs" % (time() - t0)
print "n_samples: %d, n_features: %d" % X_train.shape
print

print "Extracting features from the test dataset using the same vectorizer"
t0 = time()
X_test = vectorizer.transform(data_test.data)
print "done in %fs" % (time() - t0)
print "n_samples: %d, n_features: %d" % X_test.shape
print

if opts.full:
    print "Extracting features from the full dataset using the same vectorizer"
    t0 = time()
    X_full = []
    for file_counter in range(num_full):
        X_full.append(vectorizer.transform(data_full[file_counter].data))
        print "n_samples: %d, n_features: %d" % X_full[-1].shape
    print "done in %fs" % (time() - t0)
    print
if opts.select_chi2:
    print ("Extracting %d best features by a chi-squared test" %
           opts.select_chi2)
    t0 = time()
    ch2 = SelectKBest(chi2, k=opts.select_chi2)
    X_train = ch2.fit_transform(X_train, y_train)
    X_test = ch2.transform(X_test)
    if opts.full:
        for file_counter in range(num_full):
            X_full[file_counter] = ch2.transform(X_full[file_counter])
    print "done in %fs" % (time() - t0)


def trim(s):
    """Trim string to fit on terminal (assuming 80-column display)"""
    return s if len(s) <= 80 else s[:77] + "..."


# mapping from integer feature name to original token string
feature_names = vectorizer.get_feature_names()


###############################################################################
# Benchmark classifiers
if opts.full:
    full_predictions = [[] for i in range(num_full)]
    full_df = [[] for i in  range(num_full)]
test_predictions = []
test_df = []
def benchmark(clf):
    print 80 * '_'
    print "Training: "
    print clf
    t0 = time()
    clf.fit(X_train, y_train)
    train_time = time() - t0
    print "train time: %0.3fs" % train_time

    t0 = time()
    pred = clf.predict(X_test)
    try:
        df = map(lambda c: c[1], clf.decision_function(X_test))
    except:
        try:
            df = map(lambda c: c[1], clf.predict_proba(X_test))
        except:
            df = pred
    test_predictions.append(pred)
    test_df.append(df)
    test_time = time() - t0
    print "test time:  %0.3fs" % test_time

    score = metrics.f1_score(y_test, pred)
    print "f1-score:   %0.3f" % score
    if opts.full:
        t0 = time()
        for file_counter in range(num_full):
            print X_full[file_counter].shape[0]
            full_pred = clf.predict(X_full[file_counter])
            try:
                df = map(lambda c: c[1], clf.decision_function(X_full[file_counter]))
            except:
                try:
                    df = map(lambda c: c[1], clf.predict_proba(X_full[file_counter]))
                except:
                    df = full_pred
            full_predictions[file_counter].append(full_pred)
            full_df[file_counter].append(df)
        full_time = time()-t0
        print "full time: {full_time:0.3f}s".format(full_time=full_time)

    if hasattr(clf, 'coef_'):
        print "dimensionality: %d" % clf.coef_.shape[1]
        print "density: %f" % density(clf.coef_)

        if opts.print_top10:
            print "top 50 keywords per class:"
            for i, category in enumerate(categories):
                top10 = np.argsort(clf.coef_[i])[-50:]
                print "%s: %s" % (
                    category, " ".join(np.array(feature_names)[top10]))
                print clf.coef_[i][top10]
        print

    if opts.print_report:
        print "classification report:"
        print metrics.classification_report(y_test, pred,
                                            target_names=categories)

    if opts.print_cm:
        print "confusion matrix:"
        print metrics.confusion_matrix(y_test, pred)

    print
    clf_descr = str(clf).split('(')[0]
    return clf_descr, score, train_time, test_time

header = []
results = []
for clf, name in ((RidgeClassifier(tol=1e-3,class_weight={0:1, 1:5, 2:.0001}), "Ridge Classifier"),
                  (Perceptron(n_iter=50), "Perceptron"),
                  (KNeighborsClassifier(n_neighbors=10), "kNN")
                  ):
    print 80 * '='
    print name
    header.append(name)
    results.append(benchmark(clf))

for penalty in ["l2", "l1"]:
    print 80 * '='
    print "%s penalty" % penalty.upper()
    # Train Liblinear model
    header.append('LinearSVC '+penalty.upper())
    results.append(benchmark(LinearSVC(loss='l2', penalty=penalty,
                                            dual=False, tol=1e-5)))

    # Train SGD model
    header.append('SGDClassifier '+penalty.upper())
    results.append(benchmark(SGDClassifier(alpha=.0001, n_iter=150,
                                          penalty=penalty)))

# Train SGD with Elastic Net penalty
print 80 * '='
print "Elastic-Net penalty"
header.append('SGDClassifier ' + penalty.upper())
results.append(benchmark(SGDClassifier(alpha=.0001, n_iter=150,
                                      penalty="elasticnet")))

# Train NearestCentroid without threshold
print 80 * '='
print "NearestCentroid (aka Rocchio classifier)"
header.append('NearestCentroid')
results.append(benchmark(NearestCentroid()))

# Train sparse Naive Bayes classifiers
print 80 * '='
print "Naive Bayes"
header.append('MultinomialNB')
header.append('BernoulliNB')
results.append(benchmark(MultinomialNB(alpha=.01)))
results.append(benchmark(BernoulliNB(alpha=.01)))


class L1LinearSVC(LinearSVC):

    def fit(self, X, y):
        # The smaller C, the stronger the regularization.
        # The more regularization, the more sparsity.
        self.transformer_ = LinearSVC(penalty="l1",
                                      dual=False, tol=1e-3)
        X = self.transformer_.fit_transform(X, y)
        return LinearSVC.fit(self, X, y)

    def predict(self, X):
        X = self.transformer_.transform(X)
        return LinearSVC.predict(self, X)

print 80 * '='
print "LinearSVC with L1-based feature selection"
header.append('L1LinearSVC')
results.append(benchmark(L1LinearSVC()))


# make some plots
"""
indices = np.arange(len(results))

results = [[x[i] for x in results] for i in xrange(4)]

clf_names, score, training_time, test_time = results

pl.title("Score")
pl.barh(indices, score, .2, label="score", color='r')
pl.barh(indices + .3, training_time, .2, label="training time", color='g')
pl.barh(indices + .6, test_time, .2, label="test time", color='b')
pl.yticks(())
pl.legend(loc='best')
pl.subplots_adjust(left=.25)

for i, c in  zip(indices, clf_names):
    pl.text(-.3, i, c)

pl.show()
"""
aggregate_confusions = {}
aggregate_score_confusions = {}
num_preds = len(test_predictions)
for i in range(num_preds+1):
    aggregate_confusions[i] = np.zeros((3,3))
for i in map(lambda x:x/10., range(12)):
    aggregate_score_confusions[i] = np.zeros((3,3))
conditional_probabilities = {}
for i in range(12):
    conditional_probabilities[i] = np.zeros((3,3))

def get_fit(data):
    X = arange(data.size)
    sorted_data = np.sort(data)
    u = float(sum(data))/data.size
    s = float(sum(map(lambda x: (x-u)**2,data)))/data.size
    if s == 0:
        def pdf(t):
            try:
                return map(lambda t_elem: 1 if t_elem == u else 0,t)
            except TypeError:
                return 1 if t == u else 0
        return pdf
    return lambda t: exp(-(t-u)**2/(2*s))/np.sqrt(2*np.pi*s)

def class_prob(conditional_probabilties, class_probabilities, vector, fits, score):
    alpha = .001
    class_probs = []
    denominator = sum(class_probabilities[i]*reduce(operator.mul, ((conditional_probabilities[j][i][vector[j]] + alpha) for j in range(len(vector))))*fits[i](score) for i in range(len(class_probabilities)))
    for i in range(len(class_probabilities)):
        numerator = class_probabilities[i]*reduce(operator.mul, ((conditional_probabilities[j][i][vector[j]] + alpha) for j in range(len(vector))))*fits[i](score)
        class_probs.append(float(numerator)/denominator)
    return class_probs

with open('twitter/test_classifier_outs.csv','w') as clouts, (open('twitter/full_classifier_outs.csv','w') if opts.full else open('twitter/dummy')) as flouts:
    csvw = csv.writer(clouts)
    csvw.writerow(['trueprob','sum']+header + ['class','average_score','uid','link','office_level','sitetext'])
    df_avgs = map(lambda a:sum(a)/len(a),zip(*test_df))
    test_predictions.append(df_avgs)
    test_predictions.append(data_test.data)
    test_predictions.append(data_test.target)
    test_predictions.append(data_test.link)
    fits = []
    score_dists = []
    for i in range(3):
        score_dists.append(np.array(df_avgs)[np.array(map(lambda x:x==i,data_test.target))])
        fits.append(get_fit(score_dists[i]))
        #hist(score_dists[i],normed=1)
        plot(arange(min(score_dists[i]),max(score_dists[i]),.01),fits[i](arange(min(score_dists[i]),max(score_dists[i]),.01)))
        savefig('hist{i}.png'.format(i=i))
        #clearfig()
    for i in range(12):
        tps = test_predictions[i]
        for j in range(3):
            class_slice = tps[np.array(map(lambda x:x==j,data_test.target))]
            for k in range(3):
                conditional_probabilities[i][j][k] = float(len(class_slice[np.array(map(lambda x:x==k, class_slice))]))/float(len(class_slice))
    class_probabilities = [len(filter(lambda x:x==i,data_test.target)) for i in range(3)]
    for r in zip(*test_predictions):
        cps = class_prob(conditional_probabilities, class_probabilities, r[:-4],fits, r[-4])
        csvw.writerow(['{0:1.5f}'.format(cps[1]),sum(r[:-4])]+map(str,r[:-4]) + [r[-2],r[-4],eval(r[-3])['uid'],r[-1],eval(r[-3])['office_level'],eval(r[-3])['sitetext']])
        for i in range(num_preds+1):
            cl = sum(r[:num_preds]) >= i
            aggregate_confusions[i][r[-2]][cl] += 1
        for i in map(lambda x:x/10., range(12)):
            cl = r[-4] >= i
            aggregate_score_confusions[i][r[-2]][cl] += 1
    if opts.full:
        csvfull = csv.writer(flouts)
        csvfull.writerow(['trueprobs','sum']+header + ['average_score','uid','link','office_level'])
        for file_counter in range(num_full):
            print len(zip(*full_predictions[file_counter]))
            fulldf_avgs = map(lambda a:sum(a)/len(a),zip(*full_df[file_counter]))
            print len(fulldf_avgs)
            full_predictions[file_counter].append(fulldf_avgs)
            full_predictions[file_counter].append(data_full[file_counter].link)
            full_predictions[file_counter].append(data_full[file_counter].data)
            print len(zip(*full_predictions[file_counter]))
            try:
                for r in zip(*full_predictions[file_counter]):
                    cps = class_prob(conditional_probabilities, class_probabilities, r[:-3],fits, r[-3])
                    csvfull.writerow(['{0:1.5f}'.format(cps[1]),sum(map(lambda x: -1 if x==2 else x,r[:-3]))]+map(str,r[:-3]) + [r[-3],eval(r[-1])['uid'],r[-2],eval(r[-1])['office_level']])
            except Exception as error:
                import pdb;pdb.set_trace()
for k,v in aggregate_confusions.iteritems():
    print k
    print np.array_repr(v,precision=0,suppress_small=True)
keys = aggregate_score_confusions.keys()
keys.sort()
for k in keys:
    v = aggregate_score_confusions[k]
    print k
    print np.array_repr(v,precision=0,suppress_small=True)
