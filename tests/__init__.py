# coding: utf-8

# from __future__ import unicode_literals
# from __future__ import absolute_import

from os.path import join, realpath
from sys import path

# import fs

# Add the local code directory to the `fs` module path
# Can only rely on fs.__path__ being an iterable - on windows it's not a list
# newPath = list(fs.__path__)
# newPath.insert(0, realpath(join(__file__, "..", "..", "src")))
# print(newPath)
# fs.__path__ = newPath

path.append(realpath(join(__file__, "..", "..", "src")))
