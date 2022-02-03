import os
import zipfile
import pandas as pd
import sys

sys.path.append('/mnt/c/Ironhack/Material/Repositorio/Final/src/')
import extraction_functions as ef

init = '/mnt/c/Ironhack/Material/Repositorio/Final/data/input'

ef.executor()
os.chdir(init)
temp = os.listdir()
temp1 = ''.join([str(item) for item in temp if zipfile.is_zipfile(item) == False])
path_string = f"{init}/{temp1}"
os.chdir(path_string)
files_list = os.listdir()

li = ef.account_pd()
tr = ef.log_activity_pd()
li.category = li.category.str.split(':')
li_final = li.category.apply(pd.Series).T
li_final.rename(columns={0:'Google Account ID',2:'Created on',3:'IP',4:'Language',5:'e-mail',6:'recovery mail',7:'phone'}, inplace=True)
li_final.drop([0, 2, 3], axis=0, inplace=True)
li_final.drop([1], axis=1, inplace=True)
li_final.reset_index(drop=True, inplace=True)
tr['date'] = tr.timestamp.apply(ef.ip_)
tr['ip'] = tr.ip.apply(ef.ip_)
tr['activity'] = tr.activity.apply(ef.ip_)
tr.drop(columns=['timestamp'], inplace=True)
li_final.rename(columns={'email':'e-mail'}, inplace=True)
li_final['e-mail'] = li_final['e-mail'].str.strip().str.lower()
personal_info = li_final.merge(ef.df_profile_extr, how='outer', on='e-mail')

profile_extr = ef.profile_extr()
df_profile_extr = pd.DataFrame(profile_extr, index=(0,))

activ_services = ef.activ_services()
activ_services.drop(columns=['Proxiedhost IP Address','Is Non-routable IP Address','Referer Product Name','Referer Sub-Product Name','Activity Type','Gmail Access Channel','Android Webview Package Name'], inplace=True)

activ_dispositives = ef.activ_dispositives()
activ_dispositives.drop(columns=['User Given Name'], inplace=True)
dispositives = activ_services.merge(activ_dispositives, how='outer', on='Gaia ID')
dispositives.rename(columns={'Device Model':'Model'}, inplace=True)
dispositives1 = dispositives.merge(ef.google_pl_sto_installs, how='outer', on='Model')

youtube_music = ef.youtube_music()
for i in range(len(youtube_music['URL de la canción'])):
    if '//' in youtube_music['URL de la canción'][i]:
        youtube_music['URL de la canción'][i] = youtube_music['URL de la canción'][i].split('//')[1]

for i in range(len(youtube_music['URL de la canción'])):
    if '/' in youtube_music['URL de la canción']:
        youtube_music['URL de la canción'][i] = youtube_music['URL de la canción'][i].split('/')[0]

chrome_browser_history = ef.chrome_browser_history()
for i in range(len(chrome_browser_history.url)):
    if '//' in chrome_browser_history.url[i]:
        chrome_browser_history.url[i] = chrome_browser_history.url[i].split('//')[1]

for i in range(len(chrome_browser_history.url)):
    if '/' in chrome_browser_history.url[i]:
        chrome_browser_history.url[i] = chrome_browser_history.url[i].split('/')[0]

chrome_bookmarks = ef.chrome_bookmarks()
dicc = {'url' : [], 'date': []}
for i in chrome_bookmarks:
    dicc["url"].append(i.get('href'))
    dicc["date"].append(i.get('add_date'))
df_chrome_bookmarks = pd.DataFrame(dicc)
for i in range(len(df_chrome_bookmarks.url)):
    if '//' in df_chrome_bookmarks.url[i]:
        df_chrome_bookmarks.url[i] = df_chrome_bookmarks.url[i].split('//')[1]

for i in range(len(df_chrome_bookmarks.url)):
    if '/' in df_chrome_bookmarks.url[i]:
        df_chrome_bookmarks.url[i] = df_chrome_bookmarks.url[i].split('/')[0]

google_pl_sto_dev = ef.google_pl_sto_dev()
google_pl_sto_dev.rename(columns={'device.mostRecentData.carrierName':'CarrierName',
                                  'device.mostRecentData.playStoreClientVersion':'ClientVersion',
                                  'device.mostRecentData.manufacturer':'Manufacturer',
                                  'device.mostRecentData.modelName':'Model',
                                  'device.mostRecentData.deviceName':'DeviceName',
                                  'device.mostRecentData.productName':'ProductName',
                                  'device.mostRecentData.retailBrand':'RetailBrand',
                                 'device.mostRecentData.totalMemoryBytes':'Memory',
                                 'device.mostRecentData.deviceIpCountry':'Country',
                                 'device.mostRecentData.userLocale':'LocalUser',
                                  'device.mostRecentData.nativePlatform':'Platform',
                                 'device.mostRecentData.buildFingerprint':'Fingerprint',
                                 'device.mostRecentData.androidSdkVersion':'AndroidVersion',
                                 'device.deviceRegistrationTime':'ActiveSince',
                                 'device.userAddedOnDeviceTime':'TimeLapse',
                                 'device.lastTimeDeviceActive':'LastTimeActive'}, inplace=True)
google_pl_sto_dev.drop(columns=['Platform'], inplace=True)
google_pl_sto_installs = ef.google_pl_sto_installs()
google_pl_sto_installs.rename(columns={'install.doc.documentType':'From',
                                  'install.doc.title':'Title',
                                  'install.firstInstallationTime':'InstallationTime',
                                  'install.deviceAttribute.model':'Model',
                                  'install.deviceAttribute.carrier':'CarrierName',
                                  'install.deviceAttribute.manufacturer':'Manufacturer',
                                  'install.lastUpdateTime':'UpdateTime'}, inplace=True)
google_shop_address = ef.google_shop_address()[2:]
addr_dicc = {'name': google_shop_address[0],
            'phone': google_shop_address[1],
            'address': google_shop_address[2]}
df_google_shop_address = pd.DataFrame(addr_dicc, index=(0,1,2))

maps_favorites = ef.maps_favorites()
maps_favorites_dicc = {"coordinates": [], 
                        'name': [], 
                        'address': []}
for i in range(len(maps_favorites)):
    maps_favorites_dicc["coordinates"].append(maps_favorites['features'][i]['geometry']['coordinates'])
    maps_favorites_dicc["name"].append(maps_favorites["features"][i]['properties']['name'])
    maps_favorites_dicc["address"].append(maps_favorites["features"][i]['properties']["address"])
df_maps_favorites = pd.DataFrame(maps_favorites_dicc)
for i in range(len(chrome_browser_history.url)):
    if '//' in chrome_browser_history.url[i]:
        chrome_browser_history.url[i] = chrome_browser_history.url[i].split('//')[1]

maps_own_sites = ef.maps_own_sites()
maps_own_sites_dicc = {"coordinates": [],
                       'address': [],
                        'name': [],
                       'title': [],
                       'date': []
                        }
for i in range(len(maps_own_sites_dicc)):
    maps_own_sites_dicc["coordinates"].append(maps_own_sites['features'][i]['geometry']['coordinates'])
    maps_own_sites_dicc["address"].append(maps_own_sites["features"][i]['properties']['Location']["Address"])
    maps_own_sites_dicc["name"].append(maps_own_sites["features"][i]['properties']['Location']['Business Name'])
    maps_own_sites_dicc["title"].append(maps_own_sites["features"][i]['properties']['Title'])
    maps_own_sites_dicc["date"].append(maps_own_sites["features"][i]['properties']['Updated'])
df_maps_own_sites = pd.DataFrame(maps_own_sites_dicc)
df_maps_own_sites.drop(columns=['title'], axis=1, inplace=True)
temp = pd.concat([df_maps_favorites, df_maps_own_sites])

pics_info_extractor = ef.pics_info_extractor()

drive_counter = ef.drive_counter()
drive_scanner = ef.drive_scanner()

#1
personal_info.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/personal_info.csv', index=False)
#2
tr.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/activity.csv', index=False)
#3
dispositives1.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/dispositives.csv', index=False)
#4
youtube_music.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/music.csv', index=False)
#5
chrome_browser_history.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/history.csv', index=False)
#6
df_chrome_bookmarks.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/favorites.csv', index=False)
#7
temp.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/locations.csv', index=False)
#8
pics_info_extractor.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/photos.csv', index=False)
#9
drive_counter.to_csv('/mnt/c/Ironhack/Material/Repositorio/Final/data/output/drive.csv', index=False)







