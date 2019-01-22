from datetime import datetime

class MyFile():

    def __init__(self, filepath):
        print('MyFile init...')
        self.filepath = filepath

    def printFilePath(self):
        print(self.filepath)

    def testReadFile(self):
        with open(self.filepath, 'r') as f:
            s = f.read()
            print('open for read...')
            print(s)

    def testWriteFile(self):
        with open(self.filepath, 'w', encoding='GBK') as f:
            f.write('今天是 ')
            f.write(datetime.now().strftime('%Y-%m-%d'))
        print("Write file complete")