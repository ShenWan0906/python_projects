import sys
import os
import time
import platform
import subprocess
import psutil
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QTableWidget, QTableWidgetItem, QTextEdit, QGroupBox,
    QHeaderView, QComboBox, QCheckBox, QSystemTrayIcon, QMenu, QAction
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QThread
from PyQt5.QtGui import QIcon, QColor, QFont


class NetworkManager(QThread):
    status_updated = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        self.running = True
        self.current_network = None
        self.phone_network_enabled = True
        self.interfaces = self.detect_interfaces()

    def detect_interfaces(self):
        interfaces = psutil.net_if_addrs()
        result = {'ethernet': None, 'phone': None}
        for iface, addrs in interfaces.items():
            if not any(addr.family == 2 for addr in addrs):
                continue
            if platform.system() == 'Windows':
                if "以太网" in iface or "Ethernet" in iface:
                    result['ethernet'] = iface
                elif not result['phone']:
                    result['phone'] = iface
            else:
                if "eth" in iface or "enp" in iface:
                    result['ethernet'] = iface
                elif not result['phone']:
                    result['phone'] = iface
        return result

    def get_network_info(self, interface):
        addrs = psutil.net_if_addrs().get(interface, [])
        ipv4_info = next((addr for addr in addrs if addr.family == 2), None)
        if not ipv4_info:
            return None
        # 获取默认网关（Windows）
        gateway = "N/A"
        if platform.system() == 'Windows':
            try:
                cmd = f'netsh interface ipv4 show route interface="{interface}"'
                output = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
                for line in output.splitlines():
                    if "默认" in line or "0.0.0.0/0" in line:
                        parts = line.split()
                        if len(parts) > 3:
                            gateway = parts[3]
                            break
            except:
                pass
        status = "active" if interface == self.current_network else "standby"
        enabled = self.phone_network_enabled if interface == self.interfaces.get('phone') else True
        return {
            'interface': interface,
            'ip': ipv4_info.address,
            'netmask': ipv4_info.netmask,
            'gateway': gateway,
            'status': status,
            'enabled': enabled,
            'type': 'ethernet' if self.interfaces.get('ethernet') == interface else 'phone'
        }

    def run(self):
        while self.running:
            status_info = {}
            for iface in self.interfaces.values():
                if iface:
                    info = self.get_network_info(iface)
                    if info:
                        status_info[iface] = info
            self.status_updated.emit(status_info)
            time.sleep(3)

    def stop(self):
        self.running = False


class NetworkManagerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("网络智能分流工具")
        self.setGeometry(100, 100, 900, 700)
        self.network_manager = NetworkManager()
        self.network_manager.status_updated.connect(self.update_network_status)
        self.network_manager.start()
        self.init_ui()
        self.status_timer = QTimer(self)
        self.status_timer.timeout.connect(lambda: None)
        self.status_timer.start(2000)

    def init_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout()

        # 网络状态面板
        network_group = QGroupBox("网络状态")
        network_layout = QVBoxLayout()
        self.connection_label = QLabel("当前活动网络: 正在检测...")
        self.connection_label.setFont(QFont("Arial", 12, QFont.Bold))
        network_layout.addWidget(self.connection_label)
        self.network_table = QTableWidget()
        self.network_table.setColumnCount(6)
        self.network_table.setHorizontalHeaderLabels(["接口名称", "IP地址", "子网掩码", "网关", "状态", "类型"])
        self.network_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        network_layout.addWidget(self.network_table)
        network_group.setLayout(network_layout)
        main_layout.addWidget(network_group)

        # 控制面板
        control_group = QGroupBox("网络控制")
        control_layout = QHBoxLayout()
        self.network_selector = QComboBox()
        control_layout.addWidget(QLabel("选择网络:"))
        control_layout.addWidget(self.network_selector)
        switch_btn = QPushButton("切换到此网络")
        switch_btn.clicked.connect(self.switch_network)
        control_layout.addWidget(switch_btn)
        self.phone_toggle = QCheckBox("启用手机网络")
        self.phone_toggle.setChecked(True)
        self.phone_toggle.stateChanged.connect(self.toggle_phone_network)
        control_layout.addWidget(self.phone_toggle)
        control_group.setLayout(control_layout)
        main_layout.addWidget(control_group)

        # 日志面板
        log_group = QGroupBox("日志")
        log_layout = QVBoxLayout()
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        log_layout.addWidget(self.log_area)
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)

        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

    def update_network_status(self, status_info):
        # 下拉框保持当前选择
        current_selection = self.network_selector.currentData()
        self.network_selector.clear()
        for iface, info in status_info.items():
            self.network_selector.addItem(iface, iface)
        if current_selection:
            index = self.network_selector.findData(current_selection)
            if index >= 0:
                self.network_selector.setCurrentIndex(index)
        # 更新表格
        self.network_table.setRowCount(len(status_info))
        for row, (iface, info) in enumerate(status_info.items()):
            self.network_table.setItem(row, 0, QTableWidgetItem(info['interface']))
            self.network_table.setItem(row, 1, QTableWidgetItem(info['ip']))
            self.network_table.setItem(row, 2, QTableWidgetItem(info['netmask']))
            self.network_table.setItem(row, 3, QTableWidgetItem(info['gateway']))
            self.network_table.setItem(row, 4, QTableWidgetItem(info['status']))
            self.network_table.setItem(row, 5, QTableWidgetItem(info['type']))

    def switch_network(self):
        iface = self.network_selector.currentData()
        if iface:
            self.log_area.append(f"切换到网络: {iface}")
            self.network_manager.current_network = iface

    def toggle_phone_network(self, state):
        self.network_manager.phone_network_enabled = (state == Qt.Checked)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = NetworkManagerApp()
    window.show()
    sys.exit(app.exec_())
