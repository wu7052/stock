import sys
import os

workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\assess_package'
sys.path.insert(0, pack_path)

from back_assess import back_trader