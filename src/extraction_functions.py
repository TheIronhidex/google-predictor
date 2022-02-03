import glob
import os
import zipfile
from time import sleep
from tqdm import tqdm
import json
import codecs
import html
import re
from bs4 import BeautifulSoup
import pandas as pd
import csv
from pandas import json_normalize
import numpy as np
import sys
import collections

init = '/mnt/c/Ironhack/Material/Repositorio/Final/data/input'

def zip_format():
    #Let's change the working directory
    os.chdir(init)
    #Let's move to the working directory.
    os.getcwd()
    #Let's check everything here with a .zip extension
    zip_list = glob.glob("*.zip")
    if len(zip_list) == 1:
        #Let's use list comprehension to extract the name of the file as a string
        zip_name = ''.join([str(item) for item in zip_list])
        zip_location = f'{init}/{zip_name}'
        return zip_location
    else:
        print('Too many .zip files detected OR there is no .zip at all')

def zip_extractor(*args, **kwargs):
    with zipfile.ZipFile(zip_format()) as zf:
        for member in tqdm(zf.infolist(), desc='Extracting '):
            try:
                zf.extract(member)
            except zipfile.error as e:
                pass
        zf.close()
        #os.system(f"rm -rf {zip_format()}")

def executor(*args, **kwarg):
    try:
        zip_extractor()
        print('Success!')
    except zipfile.error:
        raise ("It seems that there was an error during the extraction.")

def account_html():
    li = []
    tr = []
    url = f"{path_string}/Cuenta de Google"
    os.chdir(url)
    directory = os.getcwd()
    for filename in os.listdir(directory):
        if filename.endswith('.html'):
            fname = os.path.join(directory, filename)
            with open(fname, 'r') as file:
                beautifulSoupText = BeautifulSoup(file.read(), 'html.parser')
                for tag in beautifulSoupText.findAll('li'):
                    li.append(tag)
                
                for tag in beautifulSoupText.findAll('tr'):
                    tr.append(tag)
    return li, tr

def account_pd(*args, **kwargs):
    li = pd.DataFrame(data=(account_html()[0]), columns=['category'])
    return li

def log_activity_pd(*args, **kwargs):
    tr = pd.DataFrame(data=(account_html()[1]), columns = ['timestamp', 'ip', 'activity', 'agent'])
    tr.drop(['agent'], axis=1, inplace=True)
    return tr.iloc[1: , :]

def ip_(x):
    return x.text

def profile_extr():
    for i in files_list:
        if i == 'Perfil':
            local = f"{path_string}/{i}/Perfil.json"
    try:
        f = open(local)
        data = json.load(f)
        name = data['name']['formattedName']
        email = (data['emails'][0]['value']).lower()
        gender = data['gender']['type']
        profile_dict = {'name': name, 'email': email, 'gender': gender}
        return profile_dict
    
    except: NameError

def activ_services():
    for i in files_list:
        if i == 'Actividad de registro de accesos':
            local = f"{path_string}/{i}/Actividades_ una lista con los servicios de Google.csv"
    df = pd.read_csv(local)
    return df

def activ_dispositives():
    for i in files_list:
        if i == 'Actividad de registro de accesos':
            local = f"{path_string}/{i}/Dispositivos_ una lista con los dispositivos (por_.csv"
    df = pd.read_csv(local)
    return df

def youtube_music():
    for i in files_list:
        if i == 'YouTube y YouTube Music':
            local = f"{path_string}/{i}/canciones-biblioteca-musica/canciones-biblioteca-musica.csv"
    df = pd.read_csv(local)
    return df

def chrome_browser_history():
    for i in files_list:
        if i == 'Chrome':
            local = f"{path_string}/{i}/BrowserHistory.json"
    try:
        f = open(local)
        data = json.load(f)
        json_normalize(data)
        df = pd.DataFrame(data['Browser History'])
        return df
    except NameError:
        return

def chrome_bookmarks():
    urls = []
    for i in files_list:
        if i == 'Chrome':
            local = f"{path_string}/{i}/Bookmarks.html"
            with open(local, 'r') as file:
                beautifulSoupText = BeautifulSoup(file.read(), 'html.parser')
                for tag in beautifulSoupText.findAll('a'):
                    urls.append(tag)
    return urls

def google_pl_sto_dev():
    for i in files_list:
        if i == 'Google Play Store':
            local = f"{path_string}/{i}/Devices.json"
    try:
        f = open(local)
        data = json.load(f)
        temp = json_normalize(data)
        return temp
    except NameError:
        return

def google_pl_sto_installs():
    for i in files_list:
        if i == 'Google Play Store':
            local = f"{path_string}/{i}/Installs.json"
    try:
        f = open(local)
        data = json.load(f)
        temp = json_normalize(data)
        return temp
    except NameError:
        return

def google_shop_address():
    for i in files_list:
        if i == 'Google Shopping':
            local = f"{path_string}/{i}/Addresses/Addresses.txt"
    try:
        myfile = open(local, "rt")
        contents = myfile.read()
        temp = re.split(',|\n', contents)
        myfile.close()
        return temp
    except NameError:
        return

def maps_favorites():
    for i in files_list:
        if i == 'Maps':
            local = f"{path_string}/{i}/Mis sitios etiquetados/Sitios etiquetados.json"
    try:
        f = open(local)
        data = json.load(f)
        return data
    except NameError:
        return

def maps_own_sites():
    for i in files_list:
        if i == "Maps (Tus sitios)":
            local = f"{path_string}/{i}/Sitios guardados.json"
    try:
        f = open(local)
        data = json.load(f)
        return data
    except NameError:
        return

def folder_selector():
    for i in files_list:
        if i == "Google Fotos":
            local = f"{path_string}/{i}/"
            
    with os.scandir(local) as ficheros:
        ficheros = [fichero.name for fichero in ficheros if fichero.is_file() == False]
    return ficheros

def folder_iterator(*args, **kwargs):
    for i in files_list:
        if i == "Google Fotos":
            local = f"{path_string}/{i}/"
    directory = []
    for i in folder_selector():
        path = f"{local}{i}"
        directory.append(path)
    return directory

def csv_scanner(*args, **kwargs):
    csvs = []
    for j in folder_iterator():
        os.chdir(j)
        temp_ = glob.glob("*.json")
        csvs.append(temp_)
    #g1 = [i.replace("'", '') for i in csvs]
    return csvs

def merge_JsonFiles(*args, **kwargs):
    result = []
    for f1 in csv_scanner():
        for j in f1:
            f = open(j)
            data = json.load(f)
            result.append(data)
    return result

def pics_info_extractor(*args, **kwargs):
    result = merge_JsonFiles()
    title_ = [i["title"] for i in result]
    description_ = [i["description"] for i in result]
    imageViews_ = [i["imageViews"] for i in result]
    timestamp_ = [i["photoTakenTime"]['timestamp'] for i in result]
    dicct_ = {'title':title_,'description':description_,'imageViews':imageViews_,'timestamp':timestamp_}
    df = pd.DataFrame(dicct_, columns=('title','description','imageViews','timestamp'))
    return df

def drive_counter():
    for i in files_list:
        if i == "Drive":
            local = f"{path_string}/{i}/"

    os.chdir(local)
    cnt = collections.Counter()
    for filename in glob.glob("*"):
        name, ext = os.path.splitext(filename)
        cnt[ext] += 1
    df = pd.DataFrame(cnt, index=(0,))
    return df
