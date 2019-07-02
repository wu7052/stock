import sys
import os

workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\filter_package'
sys.path.insert(0, pack_path)

from filter_simple import filter_fix