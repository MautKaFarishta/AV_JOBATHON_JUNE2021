import pandas as pd
import numpy as np
import time
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime,date

class myTime():
    def __init__(self):
        self.sTime = time.time()
        self.cTime = time.time()

    def show(self,msg):
        print(f'{msg}, Time Elapsed  ___ ',time.time()-self.cTime,'Total time __ ',time.time()-self.sTime)
        self.cTime = time.time()

class pipeline():
    def __init__(self,d,uD,currDate = '2018-05-28 00:00:00'):
        self.T = myTime()
        self.data = pd.read_csv(d)
        self.userData = pd.read_csv(uD)
        self.T.show('Data Aqquired')
        d = datetime.strptime(currDate,'%Y-%m-%d %H:%M:%S')
        self.nowDate = time.mktime(d.timetuple())

    def executePipe(self):
        self.clean()
        # self.create()

    def temp(self):# To be deleted later
        uData = pd.read_csv('uData.csv')
        data = pd.read_csv('pro.csv')

        return uData,data

    def create(self):
        uData,data = self.temp()
        self.T.show('Data Aquired')
        final = pd.DataFrame(columns=['UserID','No_of_days_Visited_7_Days','No_Of_Products_Viewed_15_Days','User_Vintage','Most_Viewed_product_15_Days','Most_Active_OS','Recently_Viewed_Product','Pageloads_last_7_days','Clicks_last_7_days'])

        usr_group = data.groupby('UserID')

        for uID,uD in usr_group:
            ls = self.getUserInfo(uID,uD)

    def getUserInfo(self):
        pass

    def clean(self):
        self.removeUnwantedData()
        self.T.show('Unwanted Data Removed')
        self.transformTime()
        self.T.show('Time transformed to UNIX')
        self.improve()
        self.T.show('Activity improved')
        self.cleanUserData()
        self.T.show('User Data Cleaned')

    def cleanUserData(self):
        def get_vintage(x):
            d0 = date(int(x[:4]), int(x[5:7]), int(x[8:10]))
            d1 = date(2018, 5, 28)
            delta = d1 - d0
            return delta.days

        self.userData['Signup Date'] = self.userData['Signup Date'].apply(get_vintage)

    def removeUnwantedData(self):
        self.data = self.data[self.data['UserID'].notna()]
        self.data.drop(['webClientID','City','Country'],axis=1,inplace=True)
        self.userData.drop(['User Segment'],axis=1,inplace=True)

    def transformTime(self):
        def longFunc(date):
            start = 1525651200
            end = 1527379199
            if type(date) == str:
                if len(date) == 23:
                    # return str(pd.to_datetime([date]).astype(int))[12:22]

                    d = datetime.strptime(date[:-4],'%Y-%m-%d %H:%M:%S')
                    return time.mktime(d.timetuple())
                else:return date[:10]
            else:return 0

        self.data['VisitDateTime'] = self.data['VisitDateTime'].apply(longFunc)
        self.data['VisitDateTime'] = self.data['VisitDateTime'].apply(int)


        # t = []
        # start = int(str(pd.to_datetime(['2018-05-07 00:00:00.000']).astype(int))[12:31])
        # end = int(str(pd.to_datetime(['2018-05-27 00:00:00.000']).astype(int))[12:31])
        # for date in self.data['VisitDateTime']:
        #     if type(date) == str:
        #         if len(date) == 23:
        #             t.append(int(str(pd.to_datetime([date]).astype(int))[12:31]))
        #         else:t.append(int(date))
        #     else:t.append(int(np.random.uniform(low=start,high=end,size=None)))

        # self.data['VisitTime'] = t
        # self.data.drop(['VisitDateTime'],axis=1,inplace=True)
        
    def improve(self):
        self.data['ProductID'].fillna('NO',inplace=True)
        self.T.show('Product ID improved')
        self.data.replace(
         ['click','pageload',np.nan,'android','windows','chrome os','mac os x','ios','ubuntu','linux','fedora','tizen'],
         ['CLICK','PAGELOAD','NULL','Android','Windows','Chrome OS','Mac OS X','iOS','Ubuntu','Linux','Fedora','Tizen'],
         inplace=True)

    def seeNull(self):
        print(self.data.head(10))
        for col in self.data.columns:
            print(col,self.data[col].isna().sum())


if __name__=='__main__':

    pipe = pipeline('data/VisitorLogsData.csv','data/userTable.csv')
    pipe.executePipe()
    pipe.data[['VisitDateTime','ProductID','UserID','Activity','Browser','OS']].to_csv('pro.csv')
    pipe.userData[['UserID','Signup Date']].to_csv('uData.csv')
