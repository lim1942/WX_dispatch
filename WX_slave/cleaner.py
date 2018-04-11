#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-05-03 18:38:29
# @Author  : broono (broono@qq.com)
# @Link    : http://broono.sdf.org
# @Version : $Id$

import os
import sys

suffixes = ['pyc', 'pyo']


def clean(root_dir, suffixes=suffixes):
    if not os.path.isdir(root_dir):
        print 'Directory {} not exists, skipped.'.format(root_dir)
    suffixes = map(lambda x: '.' + x, suffixes)
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if any(map(lambda x: file.endswith(x), suffixes)):
                try:
                    os.remove(os.path.join(root, file))
                except Exception, e:
                    print 'Failed to delete {}: {}'.format(file, str(e))


def clean_all(root_dirs, suffixes=suffixes):
    for root_dir in root_dirs:
        clean(root_dir, suffixes)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        root_dirs = [os.path.dirname(os.path.abspath(__file__))]
    else:
        root_dirs = sys.argv[1:]
    clean_all(root_dirs)
