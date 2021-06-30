import pandas as pd
import numpy as np
import time
import warnings
warnings.filterwarnings("ignore")

class myTime():
    def __init__(self):
        self.sTime = time.time()
        self.cTime = time.time()

    def show(self,msg):
        print(f'Time Elapsed {msg} ___ ',time.time()-self.cTime,' __ ',time.time()-self.sTime)
        self.cTime = time.time()

class pipeline():
    def __init__(self,d):
        self.T = myTime()
        self.data = pd.read_csv(d)

    def clean(self):
        self.data = self.data[self.data['UserID'].notna()]
        self.T.show('UserID null Removed')
        self.data.drop(['webClientID','City','Country'],axis=1,inplace=True)
        self.transformTime()
        self.T.show('Time transformed to UNIX')
        self.improve()

    def transformTime(self):
        t = []
        start = int(str(pd.to_datetime(['2018-05-07 00:00:00.000']).astype(int))[12:31])
        end = int(str(pd.to_datetime(['2018-05-27 00:00:00.000']).astype(int))[12:31])
        for date in self.data['VisitDateTime']:
            if type(date) == str:
                if len(date) == 23:
                    t.append(int(str(pd.to_datetime([date]).astype(int))[12:31]))
                else:t.append(int(date))
            else:t.append(int(np.random.uniform(low=start,high=end,size=None)))

        self.data['VisitTime'] = t
        self.data.drop(['VisitDateTime'],axis=1,inplace=True)
        
    def improve(self):
        self.data['ProductID'].fillna('Product101',inplace=True)
        self.T.show('Product ID improved')
        self.data.replace(
         ['click','pageload',np.nan,'android','windows','chrome os','mac os x','ios','ubuntu','linux','fedora','tizen'],
         ['CLICK','PAGELOAD','NULL','Android','Windows','Chrome OS','Mac OS X','iOS','Ubuntu','Linux','Fedora','Tizen'],
         inplace=True)
        self.T.show('Activity improved')

    def seeNull(self):
        print(self.data.head(10))
        for col in self.data.columns:
            print(col,self.data[col].isna().sum())


if __name__=='__main__':

    pipe = pipeline('demo.csv')
    pipe.clean()
    pipe.seeNull()
    print(pipe.data['Activity'].unique())
    print(pipe.data['OS'].unique())
    pipe.data.to_csv('p1.csv')
