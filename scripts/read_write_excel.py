#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/25 19:36
# @Author  : kangxiaoning

import re
import os
import xlrd
import xlwt
import pprint
import datetime
from collections import namedtuple

# configuration
BASE_DIR = u'D:\work'
# CDB capture information excel path
CAPTURE_INFO_EXCEL_PATH = os.path.join(BASE_DIR,
                                       u'capture',
                                       u'tb_cdb_capature_20171110_1045.xls')

# owner information excel path
CONTACT_INFO_EXCEL_PATH = os.path.join(BASE_DIR, 'metadata', 'owner_info.xlsx')

# report path
t = datetime.datetime.now().strftime('%Y%m%d_%H%M')
result_file = u'.'.join([u'cdb_info_' + t, u'xlsx'])
REPORT_PATH = os.path.join(BASE_DIR, u'result', result_file)

# old vip port and new vip port mapping
VIP_MAPPING_PATH = os.path.join(BASE_DIR, 'metadata', 'vip_port_mapping.xlsx')

HEADER = [u'一级业务',
          u'二级业务',
          u'替换前VIP',
          u'替换前PORT',
          u'替换后VIP',
          u'替换后PORT',
          u'CLIENT_IP',
          u'OWNER',
          u'OWNER_BACK']

OwnerInfo = namedtuple(u'OwnerInfo', [u'level_one',
                                      u'level_two',
                                      u'ip_address',
                                      u'owner',
                                      u'owner_backup'])

CaptureInfo = namedtuple(u'CaptureInfo', [u'virtual_ip',
                                          u'port',
                                          u'client_ip_addresses',
                                          u'slave_capture',
                                          u'status',
                                          u'update_time'])


def get_vip_port_mapping():
    workbook = xlrd.open_workbook(VIP_MAPPING_PATH)
    sheet = workbook.sheet_by_name('Sheet1')
    vip_port_mapping = {}
    for row_num in xrange(1, sheet.nrows):
        vip_port_info = sheet.row_values(row_num)
        old_vip = vip_port_info[0]
        old_port = vip_port_info[1]
        old_vip_port = (old_vip, int(old_port))
        new_vip = vip_port_info[2]
        new_port = vip_port_info[3]
        new_vip_port = (new_vip, int(new_port))
        vip_port_mapping[old_vip_port] = new_vip_port
    return vip_port_mapping


def get_contact_information():
    # This package(xlrd) presents all text strings as Python unicode objects.
    workbook = xlrd.open_workbook(CONTACT_INFO_EXCEL_PATH)
    sheet = workbook.sheet_by_name(u'Sheet1')
    contact_information = {}
    for row_num in xrange(1, sheet.nrows):
        try:
            owner_info = OwnerInfo._make(sheet.row_values(row_num))
            ip_address = owner_info.ip_address
            contact_information[ip_address] = owner_info
        except Exception as e:
            print(e)
            print(u'exception: '.format(sheet.row_values(row_num)))
    return contact_information


def get_detail_information():
    report_data = []
    contact_info = get_contact_information()
    vip_port_mapping = get_vip_port_mapping()
    workbook = xlrd.open_workbook(CAPTURE_INFO_EXCEL_PATH)
    sheet = workbook.sheet_by_name(u'Sheet1')
    ip_regx = re.compile(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}(?![\.\d])')
    for row_num in xrange(2, sheet.nrows):
        capture_info = CaptureInfo._make(sheet.row_values(row_num))
        ip_addresses = []
        row_data = str(capture_info.client_ip_addresses)
        for ip_address in ip_regx.findall(row_data):
            ip_addresses.append(ip_address)
        # print(ip_addresses)
        for ip_address in ip_addresses:
            if ip_address.startswith(u'20.'):
                client_ip = ip_address.replace(u'20.', u'10.', 1)
            elif ip_address.startswith(u'182.'):
                client_ip = ip_address.replace(u'182.', u'172.', 1)
            elif ip_address.startswith(u'110.'):
                client_ip = ip_address.replace(u'110.', u'100.', 1)
            else:
                client_ip = ip_address
            info = contact_info.get(client_ip)
            old_vip = capture_info.virtual_ip
            old_port = int(capture_info.port)
            new_vip_port = vip_port_mapping.get((old_vip, old_port), 'N/A')
            if new_vip_port != 'N/A':
                new_vip = new_vip_port[0]
                new_port = new_vip_port[1]
            else:
                new_vip = 'N/A'
                new_port = 'N/A'
            if info is not None:
                result = [info.level_one,
                          info.level_two,
                          old_vip,
                          old_port,
                          new_vip,
                          new_port,
                          client_ip,
                          info.owner,
                          info.owner_backup]
            else:
                result = [u'not-found',
                          u'not-found',
                          old_vip,
                          old_port,
                          new_vip,
                          new_port,
                          client_ip,
                          u'not-found',
                          u'not-found']
                print(client_ip)
            report_data.append(result)
    return report_data


def write_data_to_excel(data):
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet('Sheet1')
    # save header
    for col_num, col_data in enumerate(HEADER):
        sheet.write(0, col_num, col_data)
    # save data
    for row_num, row_data in enumerate(data):
        for col_num, col_data in enumerate(row_data):
            sheet.write(row_num + 1, col_num, col_data)
    workbook.save(REPORT_PATH)


def generate_excel():
    data = get_detail_information()
    write_data_to_excel(data)
    print('generate report successfully')


if __name__ == '__main__':
    generate_excel()
