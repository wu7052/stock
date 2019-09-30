import sys
import os
#
workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath +  '\\analyzer_package'
sys.path.insert(0, pack_path)
# print("@__init__ sys.path",sys.path)

from data_analyzer import analyzer