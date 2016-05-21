# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'about_dialog.ui'
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

class Ui_about_dialog(object):
    def setupUi(self, about_dialog):
        about_dialog.setObjectName(_fromUtf8("about_dialog"))
        about_dialog.resize(320, 65)
        self.label = QtGui.QLabel(about_dialog)
        self.label.setGeometry(QtCore.QRect(60, 20, 181, 16))
        self.label.setWordWrap(False)
        self.label.setObjectName(_fromUtf8("label"))

        self.retranslateUi(about_dialog)
        QtCore.QMetaObject.connectSlotsByName(about_dialog)

    def retranslateUi(self, about_dialog):
        about_dialog.setWindowTitle(_translate("about_dialog", "About", None))
        self.label.setText(_translate("about_dialog", "CopyRight by lhao 2016-5-21", None))

