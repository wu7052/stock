import sys
import os

workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\report_package'
sys.path.insert(0, pack_path)

from ws_report import ws_rp