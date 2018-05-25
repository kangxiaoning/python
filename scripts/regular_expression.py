#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018-05-25 18:16 
# @Author  : kangxiaoning

import re

# 带有中文的pattern
# 1. pattern以unicode编码
# 2. 待匹配字符串以unicode编码
pattern = ur'中(\d+)文'
s = u'<font color="red">中12文</font>'
ret = re.findall(pattern, s)
print(ret)
