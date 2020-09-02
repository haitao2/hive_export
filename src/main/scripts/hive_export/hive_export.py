#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 作者： zhengtian
# 时间： 2020/5/7 上午11:08
# 文件： test_log.py
# IDE： PyCharm

import os

cmd = '''
python3 ../common/run_spark.py \
--spark-class \
com.yjp.export.RunApplication \
--deploy-mode cluster \
>/dev/null 2>&1 &
'''

os.system(cmd)
