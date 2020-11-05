from PyQt5 import QtWidgets,QtCore
from PyQt5.QtCore import pyqtSignal
import time
class LoadingThread(QtCore.QThread):
    _signal = pyqtSignal()
    _loading = False #关闭loading
    _current = None #当前窗口
    def __init__(self):
        super(LoadingThread,self).__init__()

    def run(self):
        #耗时导出操作
        self._current.exportsqls()
        self._signal.emit()



