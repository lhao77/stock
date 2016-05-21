# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'main_window.ui'
#
# Created by: PyQt4 UI code generator 4.11.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui
import ui_about_dialog,ui_log_dialog

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

class Ui_MainWindow(QtGui.QMainWindow):
    def __init__(self):
        super(Ui_MainWindow, self).__init__()
        self.about = ui_about_dialog.Ui_about_dialog()
        self.setupUi(self)

    def setupUi(self, MainWindow):
        MainWindow.setObjectName(_fromUtf8("MainWindow"))
        MainWindow.resize(763, 600)
        self.centralwidget = QtGui.QWidget(MainWindow)
        self.centralwidget.setObjectName(_fromUtf8("centralwidget"))
        self.initDataBtn = QtGui.QPushButton(self.centralwidget)
        self.initDataBtn.setGeometry(QtCore.QRect(60, 80, 75, 23))
        self.initDataBtn.setObjectName(_fromUtf8("initDataBtn"))
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtGui.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 763, 23))
        self.menubar.setObjectName(_fromUtf8("menubar"))
        self.menu = QtGui.QMenu(self.menubar)
        self.menu.setObjectName(_fromUtf8("menu"))
        self.menu_fenxi = QtGui.QMenu(self.menubar)
        self.menu_fenxi.setObjectName(_fromUtf8("menu_fenxi"))
        self.menu_group = QtGui.QMenu(self.menubar)
        self.menu_group.setObjectName(_fromUtf8("menu_group"))
        self.menu_about = QtGui.QMenu(self.menubar)
        self.menu_about.setObjectName(_fromUtf8("menu_about"))
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtGui.QStatusBar(MainWindow)
        self.statusbar.setObjectName(_fromUtf8("statusbar"))
        MainWindow.setStatusBar(self.statusbar)
        self.action_init_all = QtGui.QAction(MainWindow)
        self.action_init_all.setObjectName(_fromUtf8("action_init_all"))
        self.action_init_basic = QtGui.QAction(MainWindow)
        self.action_init_basic.setObjectName(_fromUtf8("action_init_basic"))
        self.action_init_gainian = QtGui.QAction(MainWindow)
        self.action_init_gainian.setObjectName(_fromUtf8("action_init_gainian"))
        self.action_V = QtGui.QAction(MainWindow)
        self.action_V.setObjectName(_fromUtf8("action_V"))
        self.action_find = QtGui.QAction(MainWindow)
        self.action_find.setObjectName(_fromUtf8("action_find"))
        self.action_rt_data = QtGui.QAction(MainWindow)
        self.action_rt_data.setObjectName(_fromUtf8("action_rt_data"))
        self.action_group = QtGui.QAction(MainWindow)
        self.action_group.setObjectName(_fromUtf8("action_group"))
        self.action_about = QtGui.QAction(MainWindow)
        self.action_about.setObjectName(_fromUtf8("action_about"))
        self.menu.addAction(self.action_init_all)
        self.menu.addAction(self.action_init_basic)
        self.menu.addAction(self.action_init_gainian)
        self.menu_fenxi.addAction(self.action_V)
        self.menu_fenxi.addAction(self.action_find)
        self.menu_group.addAction(self.action_rt_data)
        self.menu_group.addAction(self.action_group)
        self.menu_about.addAction(self.action_about)
        self.menubar.addAction(self.menu.menuAction())
        self.menubar.addAction(self.menu_fenxi.menuAction())
        self.menubar.addAction(self.menu_group.menuAction())
        self.menubar.addAction(self.menu_about.menuAction())

        self.retranslateUi(MainWindow)
        QtCore.QObject.connect(self.action_init_all, QtCore.SIGNAL(_fromUtf8("triggered()")), MainWindow.click_init_all)
        QtCore.QObject.connect(self.initDataBtn, QtCore.SIGNAL(_fromUtf8("clicked()")), MainWindow.click_init_all)
        QtCore.QObject.connect(self.action_init_basic, QtCore.SIGNAL(_fromUtf8("triggered()")), MainWindow.click_init_basic)
        QtCore.QObject.connect(self.action_init_gainian, QtCore.SIGNAL(_fromUtf8("triggered()")), MainWindow.click_init_gainian)
        QtCore.QObject.connect(self.action_about, QtCore.SIGNAL(_fromUtf8("triggered()")), MainWindow.click_about)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def click_about(self):
        dlg = QtGui.QDialog()
        self.about.setupUi(dlg)
        dlg.exec_()

    def click_init_all(self):
        dlg = QtGui.QDialog()
        self.about.setupUi(dlg)
        dlg.exec_()

    def click_init_basic(self):
        dlg = QtGui.QDialog()
        self.about.setupUi(dlg)
        dlg.exec_()

    def click_init_gainian(self):
        dlg = QtGui.QDialog()
        self.about.setupUi(dlg)
        dlg.exec_()

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow", None))
        self.initDataBtn.setText(_translate("MainWindow", "初始化数据", None))
        self.menu.setTitle(_translate("MainWindow", "初始化", None))
        self.menu_fenxi.setTitle(_translate("MainWindow", "分析", None))
        self.menu_group.setTitle(_translate("MainWindow", "股票分组", None))
        self.menu_about.setTitle(_translate("MainWindow", "关于", None))
        self.action_init_all.setText(_translate("MainWindow", "一键初始化", None))
        self.action_init_basic.setText(_translate("MainWindow", "初始化基本数据", None))
        self.action_init_gainian.setText(_translate("MainWindow", "初始化概念分类", None))
        self.action_V.setText(_translate("MainWindow", "统计大V成绩", None))
        self.action_find.setText(_translate("MainWindow", "查找强势股", None))
        self.action_rt_data.setText(_translate("MainWindow", "实时数据", None))
        self.action_group.setText(_translate("MainWindow", "分组管理", None))
        self.action_about.setText(_translate("MainWindow", "关于", None))

if __name__ == '__main__':

    import sys

    app = QtGui.QApplication(sys.argv)
    window = Ui_MainWindow()
    window.show()
    sys.exit(app.exec_())
