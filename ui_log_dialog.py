# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'log_dialog.ui'
#
# Created by: PyQt4 UI code generator 4.11.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)

class Ui_log_dialog(object):
    def setupUi(self, log_dialog):
        log_dialog.setObjectName(_fromUtf8("log_dialog"))
        log_dialog.resize(471, 312)
        self.textBrowser = QtGui.QTextBrowser(log_dialog)
        self.textBrowser.setGeometry(QtCore.QRect(0, 0, 471, 311))
        self.textBrowser.setObjectName(_fromUtf8("textBrowser"))

        self.retranslateUi(log_dialog)
        QtCore.QMetaObject.connectSlotsByName(log_dialog)

    def retranslateUi(self, log_dialog):
        log_dialog.setWindowTitle(_translate("log_dialog", "Log", None))

