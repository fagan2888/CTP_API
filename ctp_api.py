# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 09:04:06 2020

@author: yuba316
"""

import sys
sys.path.append(r'D:\work\CTP_API\6.3.15_release_20191117\win')
import thostmduserapi as mdapi

import threading
import time as t
import datetime

import copy as c
import pandas as pd

import ctypes
import inspect

#%%

class CFtdcMdSpi(mdapi.CThostFtdcMdSpi): # 继承CTP接口的行情SPI类，改写其中的某些函数

    def __init__(self,tapi,category,df):
        mdapi.CThostFtdcMdSpi.__init__(self)
        self.tapi = tapi
        self.category = category
        self.df = df
            
    def OnFrontConnected(self) -> "void":
        loginfield = mdapi.CThostFtdcReqUserLoginField()
        loginfield.BrokerID="9999" # 华西期货：16333
        loginfield.UserID="162548"
        loginfield.Password="iamaman369"
        loginfield.UserProductInfo="python dll"
        self.tapi.ReqUserLogin(loginfield,0)
        
    def OnRspUserLogin(self, pRspUserLogin: 'CThostFtdcRspUserLoginField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> "void":
        ret=self.tapi.SubscribeMarketData([id.encode('utf-8') for id in self.category],len(self.category)) # 订阅行情

    def OnRtnDepthMarketData(self, pDepthMarketData: 'CThostFtdcDepthMarketDataField') -> "void":
        mdlist=([pDepthMarketData.UpdateTime,\
        pDepthMarketData.InstrumentID,\
        pDepthMarketData.PreClosePrice,\
        pDepthMarketData.PreSettlementPrice,\
        pDepthMarketData.BidPrice1,\
        pDepthMarketData.AskPrice1,\
        pDepthMarketData.BidVolume1,\
        pDepthMarketData.AskVolume1,\
        pDepthMarketData.LastPrice]) # 所要获取的行情数据内容
        print(mdlist)
        isExist = self.df[self.df['规范代码'] == pDepthMarketData.InstrumentID].index.tolist()
        if isExist: # 如果已有的合约出现了新的行情，则覆盖掉其原有的行情
            self.df.loc[isExist[0]] = mdlist
        else: # 若出现新的合约，则补在最下面一行
            self.df.loc[len(self.df)] = mdlist

#%%

class Thread(threading.Thread):
    
    def __init__(self,code,sleeptime,output,df,mduserapi):
        threading.Thread.__init__(self)
        self.code = code
        self.sleeptime = sleeptime
        self.output = output
        self.df = df
        self.mduserapi = mduserapi
        self.address = r'tcp://180.168.146.187:10131' # 华西期货：tcp://180.168.102.233:41168
    
    def run(self):
        mduserspi=CFtdcMdSpi(self.mduserapi,self.code,self.df)
        self.mduserapi.RegisterFront(self.address)
        self.mduserapi.RegisterSpi(mduserspi)
        self.mduserapi.Init()
        
        while True:
            t.sleep(self.sleeptime)
            start_time = datetime.datetime.now()
            outputDf = c.deepcopy(self.df)
            try:
                outputDf.to_excel(self.output,index=False)
            except IOError:
                print("文件正在被读取或打开，请稍后重试...")
            else:
                print("文件保存成功")
            end_time = datetime.datetime.now()
            print("单次输出数据的时滞为："+str(end_time-start_time)+"s")

#%%

class Update():
    
    def __init__(self,code,sleeptime,output,df):
        self.mduserapi = mdapi.CThostFtdcMdApi_CreateFtdcMdApi()
        self.thread = Thread(code,sleeptime,output,df,self.mduserapi)
    
    def startUpdate(self):
        self.thread.start()
    
    def __stopThread(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stopUpdate(self):
        
        self.mduserapi.Release()
        self.__stopThread(self.thread.ident, SystemExit)

#%%
'''
code = ['AP007']
df = pd.DataFrame(columns=['更新时间','规范代码','昨收价','昨结价','买价','卖价','买量','卖量','最新价'])
address = r'tcp://180.168.146.187:10131' # simnow 7*24小时的市场接口，可在非交易时间段使用

mduserapi=mdapi.CThostFtdcMdApi_CreateFtdcMdApi()    # 创建api实例
mduserspi=CFtdcMdSpi(mduserapi,code,df)              # 创建回调spi实例
mduserapi.RegisterFront(address)                     # 注册行情前置地址
mduserapi.RegisterSpi(mduserspi)                     # api中注册spi实例
mduserapi.Init()                                     # 初始化api
'''
code,sleeptime,output,df = ['AP007','TA011'],10,r"D:\work\CTP_API\output.xlsx",pd.DataFrame(columns=['更新时间','规范代码','昨收价','昨结价','买价','卖价','买量','卖量','最新价'])
updateThread = Update(code,sleeptime,output,df)
updateThread.startUpdate()
#updateThread.stopUpdate()