import sys
import os

workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath +'\\filePackage'
sys.path.insert(0,pack_path)

from file_class import MyFile
