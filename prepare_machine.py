import nltk
import pymorphy2
from tqdm import tqdm

class PrepareMachine:

    def __init__(self, data):
        self.data = data
        
        
    def tokenize(self, reg):
        tqdm.pandas(desc=f'{"tokenize":<22}')
        #self.data = self.data.progress_apply(lambda s: s.lower())
        tokenizer = nltk.tokenize.RegexpTokenizer(reg)
        self.data = self.data.progress_apply(lambda s: tokenizer.tokenize(s.lower()))
        return self.data
    
    
    def remove_numbers(self):
        tqdm.pandas(desc=f'{"remove numbers":<22}')
        del_num = lambda s: [w for w in s if not w.isdigit()]
        self.data = self.data.progress_apply(del_num)
        return self.data
    
    
    def remove_stop_words(self, stop_words):
        tqdm.pandas(desc=f'{f"remove stop words {len(stop_words)}":<22}')
        del_stop_words = lambda s: [w for w in s if w not in stop_words]
        self.data = self.data.progress_apply(del_stop_words)
        return self.data

    
    def remove_1char_words(self):
        tqdm.pandas(desc=f'{"remove 1 char words":<22}')
        del_1char_words = lambda s: [w for w in s if len(w) > 1 or w in ('r', 'c')]
        self.data = self.data.progress_apply(del_1char_words)
        return self.data
    
    
    def lemmatizing(self):
        tqdm.pandas(desc=f'{"lemmatizing":<22}')
        lemmatizer = nltk.stem.WordNetLemmatizer()

        lemm = lambda s: [lemmatizer.lemmatize(w) for w in s]
        self.data = self.data.progress_apply(lemm)
        return self.data

    
    def stemming(self):
        stemmer = nltk.stem.porter.PorterStemmer()

        def stemm(s):
            return [stemmer.stem(w) for w in s]
        
        self.data = self.data.apply(stemm)
        return self.data
    
    
    def normalization(self):
        tqdm.pandas(desc=f'{"normalizing":<22}')
        morph = pymorphy2.MorphAnalyzer()
        get_normal = lambda s: [morph.parse(w)[0].normal_form for w in s]
        #morph = Mystem()
        #get_normal = lambda s: [morph.lemmatize(w)[0] for w in s]
        self.data = self.data.progress_apply(get_normal)
        return self.data
     
    
    def remove_hapaxes(self):
        tqdm.pandas(desc=f'{"remove hapaxes":<22}')
        hapaxes = nltk.FreqDist(self.data.str.cat().split()).hapaxes()
        del_hapaxes = lambda s: [w for w in s if w not in hapaxes]
        self.data = self.data.progress_apply(del_hapaxes)
        return self.data
