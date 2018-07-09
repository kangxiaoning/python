#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/7 17:26
# @Author  : kangxiaoning

import os


def compare_ip():
    """
    对比两个IP文件差异，将差异打印出来
    """
    path_first = r'/home/dbo/workspace/tools/files/src_ip.txt'
    path_second = r'/home/dbo/workspace/tools/files/dst_ip.txt'

    def get_ip(path):
        ret = []
        with open(path) as f:
            for line in f:
                ip = line.strip()
                if ip and not ip.startswith('#'):
                    ret.append(ip)
        return set(ret)

    def diff(src_path, dst_path):
        src = get_ip(src_path)
        dst = get_ip(dst_path)
        name_src = os.path.basename(src_path)
        name_dst = os.path.basename(dst_path)
        difference = src - dst
        for ip in difference:
            print(ip)
        diff_cnt = len(difference)
        print('\n{0}中有 {1} 条数据不在{2}\n'.format(name_src, diff_cnt, name_dst))

    diff(path_first, path_second)
    diff(path_second, path_first)


if __name__ == '__main__':
    compare_ip()
