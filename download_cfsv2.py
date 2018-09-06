# -*- coding: utf-8 -*-
"""
Author: Dai Rufeng
This script is used to download cfsv2 data from https://nomads.ncdc.noaa.gov/
"""

import os
import datetime
import urllib
from bs4 import BeautifulSoup
import re
import requests

#%%
def datelist(startdate, enddate):
    """
    输入起止日期，生成日期序列，返回列表
    """
    start_date = datetime.date(*startdate)
    end_date = datetime.date(*enddate)

    date_list = []
    curr_date = start_date
    while curr_date != end_date:
        date_list.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    date_list.append("%04d%02d%02d" % (curr_date.year, curr_date.month, curr_date.day))
    
    return date_list

#start_date = (2018, 1, 1)
#end_date   = (2018, 6, 30)
#date_list  = datelist(start_date, end_date)
#print date_list[:10]

#%%
def checkdir(dir):
    """
    检查目录是否存在，如果不存在，新建该目录
    """
    if not os.path.exists(dir):
        os.makedirs(dir)
        print "Make dir %s"%dir

def makedatedir(local_dir, aim_date, HH="00"):
    """
    输入一个日期，检查并创建四层日期目录，分别为年，年月，年月日，年月日时,
    年月日为自然年月日，时(HH)为00,06,12,18，现在只下载00时
    格式为：2018,201801,20180101,2018010100    
    """
    yy       = aim_date[:4]
    yymm     = aim_date[:6]
    yymmdd   = aim_date[:8]
    yymmddHH = yymmdd + HH
    
    yy_dir     = os.path.join(local_dir, yy)
    yymm_dir   = os.path.join(yy_dir, yymm)
    yymmdd_dir = os.path.join(yymm_dir, yymmdd)
    yymmddHH_dir = os.path.join(yymmdd_dir, yymmddHH)
    
    checkdir(yy_dir)
    checkdir(yymm_dir)
    checkdir(yymmdd_dir)   
    checkdir(yymmddHH_dir)

#local_dir = u"E:\\python\\cfsv2\\6hourlydata"
#makedatedir(local_dir, "20180101")
#%%
def getyymmddHHdir(aim_date, HH="00"):
    """
    输入一个八位日期，
    """
    yy     = aim_date[:4]
    yymm   = aim_date[:6]
    yymmdd = aim_date[:8]
    yymmddHH = yymmdd + HH
    
    yy_dir     = os.path.join(local_dir, yy)
    yymm_dir   = os.path.join(yy_dir, yymm)
    yymmdd_dir = os.path.join(yymm_dir, yymmdd)     
    yymmddHH_dir = os.path.join(yymmdd_dir, yymmddHH)
        
    return yymmddHH_dir

#yymmddHH_dir = getyymmddHHdir("20180101", HH="00")
#print yymmddHH_dir
#%%          
def remotedirurl(aim_date, host_url, HH="00"):
    """
    按远程目录结构，拼接远程目录url,返回值为一个URL
    """
    yy       = aim_date[:4]
    yymm     = aim_date[:6]
    yymmdd   = aim_date[:8]
    yymmddHH = yymmdd + HH
#    host_url = "https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf"
    remote_dir_url = host_url + "/" + yy + "/" +yymm + "/" +yymmdd + "/" + yymmddHH + "/" 
    
    return remote_dir_url

#host_url  = "https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf"
#remote_dir_url = remotedirurl("20180101", host_url, HH="00")
#print remote_dir_url
#%%
def getfileurl(remote_dir_url):
    """
    获取该目录下所有文件的URL,以url列表输出；
    只下载60天数据（每天4个时次，每时次3个文件：grb2,md5,inv)    
    """
#   remote_dir_url = "https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf/2018/201801/20180101/2018010106/"
    htmlSource = urllib.urlopen(remote_dir_url).read()
    soup = BeautifulSoup(htmlSource, features='lxml')
    all_href = soup.find_all('a', href=re.compile("flxf"))
    dd_num = 0
    file_urls = []
    for item in all_href:
        if item.string:
#            print item.string
            file_url = remote_dir_url + item.string
            file_urls.append(file_url)
            dd_num += 1
            if dd_num > 720:             #只下载60天
                break
    
    return file_urls
#
#file_urls = getfileurl(remote_dir_url)
#print file_urls
#%%
def downloadfile(file_url, aim_date, HH="00"):
    """
    通过文件url,下载数据文件到指定目录
    """
    filename = file_url.split("/")[-1]
    yymmddHH_dir = getyymmddHHdir(aim_date, HH="00")
    localfilepath = os.path.join(yymmddHH_dir, filename)
    if not os.path.isfile(localfilepath):
        r = requests.get(file_url, stream=True)
        try:
            with open(localfilepath, "wb") as datafile:
                datafile.write(r.content)
                print "download %s"%filename
        except:
            pass
#file_url = "https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf/2018/201801/20180101/2018010100/flxf2018010100.01.2018010100.grb2"
#yymmddHH_dir = u"E:\\python\\cfsv2\\6hourlydata\\2018\\201801\\20180101\\2018010100"
#downloadfile(file_url, "20180101", HH="00")
        
#%%
local_dir = u"E:\\python\\cfsv2\\6hourlydata"
host_url  = "https://nomads.ncdc.noaa.gov/modeldata/cfsv2_forecast_6-hourly_9mon_flxf"
start_date = (2018, 1, 1)
end_date   = (2018, 6, 30)

date_list  = datelist(start_date, end_date)
for aim_date in date_list:
    makedatedir(local_dir, aim_date)
    remote_dir_url = remotedirurl(aim_date, host_url)
    file_urls = getfileurl(remote_dir_url)
    for file_url in file_urls:
        downloadfile(file_url, aim_date, HH="00")
        
    
