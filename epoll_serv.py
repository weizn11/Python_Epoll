#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/16 10:56
# @Author  : weizinan
# @File    : epoll_serv.py

import select
import socket
import logging
import time

class EpollServ(object):
    class ClientConnInfo(object):
        def __init__(self, cliAddr):
            self.connTimestamp   = time.time()
            self.cliAddr         = cliAddr

    def __init__(self, listenIp, listenPort, recvTimeo=None):
        super(EpollServ, self).__init__()

        self.listenIp             = listenIp
        self.listenPort           = listenPort
        self.recvTimeo            = recvTimeo
        self.listenSoc            = None
        self.epollObj             = None
        self.cliConnDic           = dict()
        self.logger               = self._init_logger("EpollServ", logging.INFO)

    ########################################################################################
    def _create_listen_socket(self, listenIp, listenPort):
        try:
            listenSoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        except Exception, e:
            self.logger.critical("Create listen socket failed. Exception: " + str(e))
            listenSoc = None
        else:
            try:
                listenSoc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 端口释放后可被立即使用
                listenSoc.bind((listenIp, listenPort))
                listenSoc.listen(socket.SOMAXCONN)
                listenSoc.setblocking(0)  # 设置非阻塞
            except Exception, e1:
                self.logger.critical("Setting socket failed. Exception: " + str(e1))
                listenSoc.close()
                listenSoc = None
            else:
                self.logger.info("Listening socket on %s:%d" % (listenIp, listenPort))

        return listenSoc

    ########################################################################################
    def pkt_handler(self, pkt):
        print pkt

    ########################################################################################
    def _recv_from_client(self, cliSoc):
        if self.cliConnDic.has_key(cliSoc) is False:
            return -1

        try:
            recvBuf = cliSoc.recv(65536)
        except Exception, e:
            return -2
        else:
            if recvBuf is None:
                return -3
            else:
                recvLen = len(recvBuf)

        try:
            self.pkt_handler(recvBuf)
        except Exception, e:
            self.logger.error("pkt_handler error. Exception: " + str(e))

        return recvLen

    ########################################################################################
    def _clean_conn_socket(self, cliSoc):
        try:
            self.epollObj.unregister(cliSoc.fileno())  # 注销socket描述字
            # cliSoc.shutdown(socket.SHUT_RDWR)
            cliSoc.close()
        except Exception, e:
            self.logger.error("Closed socket failed. Exception: " + str(e))
        finally:
            try:
                del self.cliConnDic[cliSoc]
            except Exception, e2:
                self.logger.error("Delete client socket from list failed. Exception: " + str(e2))
        return

    ########################################################################################
    def listen_service(self):
        while True:
            if self.listenSoc is None or self.epollObj is None:
                self.listenSoc = self._create_listen_socket(self.listenIp, self.listenPort)
                if self.listenSoc is None:
                    time.sleep(5)
                    continue
                try:
                    # 创建epoll对象
                    self.epollObj = select.epoll()
                except Exception, e:
                    self.logger.error("Create epoll object failed. Exception: " + str(e))
                    self.listenSoc.close()
                    self.listenSoc = None
                    self.epollObj = None
                    time.sleep(10)
                    continue
                else:
                    try:
                        # 注册感兴趣事件
                        self.epollObj.register(self.listenSoc.fileno(),
                                               select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
                    except Exception, e1:
                        self.logger.error("Registration epoll event failed. Exception: " + str(e1))
                        self.listenSoc.close()
                        self.listenSoc = None
                        self.epollObj.close()
                        self.epollObj = None
                        time.sleep(5)
                        continue

            try:
                # 监听epoll事件
                epollEventList = self.epollObj.poll(1)
            except Exception, e:
                self.logger.error("Wait for epoll events failed. Exception: " + str(e))
                self.epollObj.close()
                self.listenSoc.close()
                self.epollObj = None
                self.listenSoc = None
                time.sleep(5)
                continue

            # 处理事件
            for fdSoc, event in epollEventList:
                if fdSoc == self.listenSoc.fileno():
                    # 监听socket描述字相同，有新连接
                    try:
                        cliSoc, cliAddr = self.listenSoc.accept()
                        cliSoc.setblocking(0)  # 设置非阻塞
                        self.epollObj.register(cliSoc.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
                        self.cliConnDic[cliSoc] = self.ClientConnInfo(cliAddr)
                    except Exception, e:
                        self.logger.error("Accepted connection failed. Exception: " + str(e))
                        continue
                    else:
                        self.logger.info("Accepted a new connection from %s:%d" % (cliAddr[0], cliAddr[1]))
                elif event & select.EPOLLIN:
                    # 有数据可接收
                    for cliSoc, connInfo in self.cliConnDic.iteritems():
                        if cliSoc.fileno() == fdSoc:
                            # 匹配中触发事件的socket
                            recvLen = self._recv_from_client(cliSoc)
                            if recvLen <= 0:
                                self.logger.info("Disconnected from %s:%d" %
                                                 (connInfo.cliAddr[0], connInfo.cliAddr[1]))
                                self.logger.info("Closed connection at %s:%d" %
                                                 (connInfo.cliAddr[0], connInfo.cliAddr[1]))
                                self._clean_conn_socket(cliSoc)
                                break
                else:
                    # socket异常
                    for cliSoc, connInfo in self.cliConnDic.iteritems():
                        if cliSoc.fileno() == fdSoc:
                            # 匹配中触发事件的socket
                            self.logger.info("Connection error at %s:%d." %
                                              (connInfo.cliAddr[0], connInfo.cliAddr[1]))
                            self.logger.info("Closed connection at %s:%d" %
                                             (connInfo.cliAddr[0], connInfo.cliAddr[1]))
                            self._clean_conn_socket(cliSoc)
                            break

            # 关闭超时连接
            if self.recvTimeo is not None and self.recvTimeo >= 0:
                for cliSoc, connInfo in self.cliConnDic.iteritems():
                    if time.time() - self.cliConnDic[cliSoc].connTimestamp >= self.recvTimeo:
                        self.logger.info("Connection timeout at %s:%d." %
                                         (connInfo.cliAddr[0], connInfo.cliAddr[1]))
                        self.logger.info("Closed connection at %s:%d" %
                                         (connInfo.cliAddr[0], connInfo.cliAddr[1]))
                        self._clean_conn_socket(cliSoc)

    ########################################################################################
    def _init_logger(self, loggerName, logLevel):
        # 创建日志记录
        logger = logging.getLogger(loggerName)
        logger.setLevel(logLevel)

        logFormatter = logging.Formatter('[%(asctime)s] [%(levelname)s] -> %(message)s')
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        consoleHandler.setLevel(logLevel)
        logger.addHandler(consoleHandler)
        return logger







