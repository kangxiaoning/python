requests使用记录
==============

# 1. encoding

当收到response，在执行response.text时，requests会尝试猜测encoding。
- 首先检查header指定的encoding
- 不存在则用chardet尝试猜测

遇到requests猜错的情况可以指定正确的encoding，执行response.text时就会以指定的encoding
进行decode。

```python
import requests

url = 'http://xxx.com'
response = requests.get(url, timeout=60)
response.encoding = 'gbk'
print(response.text)

```

