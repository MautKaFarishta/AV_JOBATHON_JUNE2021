import pandas as pd
from datetime import datetime
import time

uData = pd.read_csv('uData.csv')
data = pd.read_csv('pro.csv')
sample = pd.read_csv('s_sub.csv')

final = pd.DataFrame(columns=['UserID','No_of_days_Visited_7_Days','No_Of_Products_Viewed_15_Days','User_Vintage','Most_Viewed_product_15_Days','Most_Active_OS','Recently_Viewed_Product','Pageloads_last_7_days','Clicks_last_7_days'])
d = datetime.strptime('2018-05-27 23:59:59','%Y-%m-%d %H:%M:%S')
nowDate = time.mktime(d.timetuple())


usr_group = data.groupby('UserID')

def getNDays(n,now):
  days = [now]
  for _ in range(7):
    now -= 86400
    days.append(now)

  days.reverse()
  return days

def getLastDays(n,s):
  return s-n*86400 , s

#No of clicks and pageloads in N days
def ActivityNDays(n,da):
    start,end = getLastDays(n,nowDate)
    lastNDays = list(da['VisitDateTime'] > start) and \
    list(da['VisitDateTime'] < end)
    d = da[lastNDays]
    if 'CLICK' not in d['Activity'].value_counts().keys():
        c = 0
    else:c=d['Activity'].value_counts()['CLICK']

    if 'PAGELOAD' not in d['Activity'].value_counts().keys():
        p = 0
    else:p=d['Activity'].value_counts()['PAGELOAD']

    return c,p

#MOst Viewed product in last n days
def MostViewedInNDays(n,da):
    start,end = getLastDays(15,nowDate)
    if 'PAGELOAD' in da['Activity']:
        newDA = da.groupby('Activity').get_group('PAGELOAD')
    else:return 'Product101'
    if not newDA[newDA['VisitDateTime'] > start]['ProductID'].empty:
        if not newDA[newDA['VisitDateTime'] > start]['ProductID'].value_counts().idxmax() == 'NO':
            return newDA[newDA['VisitDateTime'] > start]['ProductID'].value_counts().idxmax()
        else:return 'Product101'
    else:return 'Product101'

#Number of products viewed in N days
def ProductsViewedInNDays(n,da):
    start,end = getLastDays(n,nowDate)
    viewed = list(da['VisitDateTime'] > start) and \
    list(da['VisitDateTime'] < end)
    b = list(da[viewed]['ProductID'].unique())
    # if 'Product101' in b:
    #     b.remove('Product101')
    return len(b)

#Number of times visited in last N days
def visitedLastNdays(n,u1):
    times = list(u1['VisitDateTime'])
    d = set()
    days = getNDays(n,nowDate)

    for t in times:
        i=0
        while i<8 and t > days[i]:
            i+=1
        if i!=0:d.add(i)        

    return len(d)

def getUserInfo(u,data):
    l = [];l.append(u)
    timesVisited = visitedLastNdays(7,data)
    productsViewed = ProductsViewedInNDays(15,data)
    vintage = uData[uData['UserID'] == u]['Signup Date'].max()
    mostViewed = MostViewedInNDays(15,data)
    os = data['OS'].value_counts().idxmax()
    recentlyViewed = data[data['VisitDateTime'] == data['VisitDateTime'].max()]['ProductID'].max()
    clicks,pgloads = ActivityNDays(7,data)

    l.append(timesVisited)
    l.append(productsViewed)
    l.append(vintage)
    l.append(mostViewed)
    l.append(os)
    l.append(recentlyViewed)
    l.append(pgloads)
    l.append(clicks)

    return l

c = 0
for user in sample['UserID']:
    ls = getUserInfo(user,usr_group.get_group(user))
    print(c,'__',ls)
    c+=1
    final.loc[len(final)] = ls

final.to_csv('S2.csv')
print(final)


