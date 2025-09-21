import sys
import os
import subprocess
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QLabel,
    QLineEdit,
    QPushButton,
    QFileDialog,
    QVBoxLayout,
    QMessageBox,
    QHBoxLayout,
    QFrame,
    QSpinBox,
    QRadioButton,
    QButtonGroup
)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
from update import ProductBulkUpdater

POLL_INTERVAL_SECONDS = 30
MAX_POLL_RETRIES = None

class UpdateThread(QThread):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    stopped = pyqtSignal()

    def __init__(self, input_path: str, worker_count: int, mode: str):
        super().__init__()
        self.input_path = input_path
        self.worker_count = worker_count
        self.mode = mode
        self._is_running = True
        self.updater = None

    def stop(self):
        self._is_running = False
        if self.updater and hasattr(self.updater, "stop"):
            self.updater.stop()

    def run(self):
        try:
            self.updater = ProductBulkUpdater(
                source_file=self.input_path,
                mode=self.mode.lower(),
                max_workers=self.worker_count
            )
            if not self._is_running:
                self.stopped.emit()
                return
            self.updater.run_with_status_monitoring(
                max_retries=MAX_POLL_RETRIES,
                retry_interval=POLL_INTERVAL_SECONDS,
                skip_update_phase=False
            )
            if not self._is_running:
                self.stopped.emit()
                return
            self.finished.emit()
        except Exception as e:
            if self._is_running:
                self.error.emit(str(e))
            else:
                self.stopped.emit()

class DragDropLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setMinimumHeight(35)
        self.setStyleSheet(
            """
            QLineEdit {
                padding: 5px 10px;
                border: 2px solid #ddd;
                border-radius: 5px;
                font-size: 14px;
                background-color: white;
                color: #333333;
            }
            QLineEdit:focus {
                border: 2px solid #4CAF50;
            }
            QLineEdit::placeholder {
                color: #999999;
            }
            """
        )

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        files = [url.toLocalFile() for url in event.mimeData().urls()]
        if files:
            self.setText(files[0])

class App(QWidget):
    def __init__(self):
        super().__init__()
        self.result_file_path = None
        self.update_thread = None
        self.loading_timer = QTimer()
        self.loading_dots = 0
        self.init_ui()
        self.loading_timer.timeout.connect(self.update_loading_text)

    def init_ui(self):
        self.setWindowTitle("Bulk Product Update Tool")
        self.setGeometry(120, 120, 820, 460)
        self.setStyleSheet(
            """
            QWidget { background-color: #f5f5f5; font-family: 'Microsoft YaHei', Arial; }
            QLabel { font-size: 14px; color: #333; }
            QPushButton { font-size: 14px; padding: 8px 20px; border-radius: 5px; }
            QPushButton:disabled { background-color: #cccccc !important; color: #666666 !important; border: none !important; }
            QSpinBox {
                padding: 5px;
                border: 2px solid #ddd;
                border-radius: 5px;
                font-size: 14px;
                min-width: 70px;
                max-width: 80px;
            }
            QSpinBox:focus { border: 2px solid #4CAF50; }
            QRadioButton { font-size: 14px; }
            """
        )
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        title_label = QLabel("Bulk Product Update Tool")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("font-size:24px; font-weight:bold; color:#333; margin-bottom:10px;")
        main_layout.addWidget(title_label)

        input_frame = QFrame()
        input_frame.setStyleSheet("QFrame { background:white; border-radius:10px; padding:20px; }")
        input_layout = QVBoxLayout(input_frame)
        input_layout.setSpacing(18)

        file_row = QHBoxLayout()
        self.input_label = QLabel("File Path:")
        self.input_field = DragDropLineEdit(self)
        self.input_field.setPlaceholderText("Drag & drop or click Browse")
        self.input_field.textChanged.connect(self.on_file_changed)
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.setStyleSheet(
            "QPushButton { background:#2196F3; color:white; } QPushButton:hover { background:#1976D2; }"
        )
        self.browse_btn.clicked.connect(self.select_input_file)
        file_row.addWidget(self.input_label)
        file_row.addWidget(self.input_field, 1)
        file_row.addWidget(self.browse_btn)
        input_layout.addLayout(file_row)

        mode_row = QHBoxLayout()
        mode_label = QLabel("Mode:")
        self.radio_taobao = QRadioButton("Taobao")
        self.radio_warehouse = QRadioButton("Warehouse")
        self.radio_custom_field = QRadioButton("Custom Field")
        self.radio_taobao.setChecked(True)
        self.mode_group = QButtonGroup(self)
        self.mode_group.addButton(self.radio_taobao)
        self.mode_group.addButton(self.radio_warehouse)
        self.mode_group.addButton(self.radio_custom_field)
        mode_row.addWidget(mode_label)
        mode_row.addWidget(self.radio_taobao)
        mode_row.addWidget(self.radio_warehouse)
        mode_row.addWidget(self.radio_custom_field)
        mode_row.addStretch()
        input_layout.addLayout(mode_row)

        worker_row = QHBoxLayout()
        worker_label = QLabel("Workers:")
        self.worker_spinbox = QSpinBox()
        self.worker_spinbox.setRange(1, 50)
        self.worker_spinbox.setValue(5)
        self.worker_spinbox.setSingleStep(1)
        worker_row.addWidget(worker_label)
        worker_row.addWidget(self.worker_spinbox)
        worker_row.addStretch()
        input_layout.addLayout(worker_row)

        main_layout.addWidget(input_frame)

        button_frame = QFrame()
        button_layout = QHBoxLayout(button_frame)
        button_layout.setSpacing(15)

        self.execute_button = QPushButton("Start")
        self.execute_button.setStyleSheet(
            "QPushButton { background:#4CAF50; color:white; min-height:40px; font-size:16px; } QPushButton:hover { background:#45a049; }"
        )
        self.execute_button.setCursor(Qt.PointingHandCursor)
        self.execute_button.clicked.connect(self.start_processing)
        button_layout.addWidget(self.execute_button, 1)

        self.stop_button = QPushButton("Stop")
        self.stop_button.setStyleSheet(
            "QPushButton { background:#f44336; color:white; min-height:40px; font-size:16px; } QPushButton:hover { background:#d32f2f; }"
        )
        self.stop_button.setCursor(Qt.PointingHandCursor)
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_processing)
        button_layout.addWidget(self.stop_button, 1)

        self.open_result_button = QPushButton("Open Result")
        self.open_result_button.setStyleSheet(
            "QPushButton { background:#FF9800; color:white; min-height:40px; font-size:16px; } QPushButton:hover { background:#F57C00; }"
        )
        self.open_result_button.setCursor(Qt.PointingHandCursor)
        self.open_result_button.setEnabled(False)
        self.open_result_button.clicked.connect(self.open_result_file)
        button_layout.addWidget(self.open_result_button, 1)

        main_layout.addWidget(button_frame)
        self.setLayout(main_layout)

    def on_file_changed(self):
        self.open_result_button.setEnabled(bool(self.input_field.text()))
        if self.input_field.text():
            file_path = self.input_field.text()
            if os.path.isfile(file_path):
                file_dir = os.path.dirname(file_path)
                file_name = os.path.splitext(os.path.basename(file_path))[0]
                self.result_file_path = os.path.join(file_dir, f"{file_name}_result.xlsx")

    def update_loading_text(self):
        self.loading_dots = (self.loading_dots + 1) % 4
        dots = "." * self.loading_dots
        self.execute_button.setText(f"Processing{dots}")

    def select_input_file(self):
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select the input Excel file",
            "",
            "Excel Files (*.xlsx *.xls);;All Files (*)",
            options=options
        )
        if file_path:
            self.input_field.setText(file_path)

    def start_processing(self):
        input_path = self.input_field.text()
        worker_count = self.worker_spinbox.value()
        mode = "taobao" if self.radio_taobao.isChecked() else "warehouse" if self.radio_warehouse.isChecked() else "custom_field"
        if not input_path:
            self._msg("Error", "Please select the input file path", QMessageBox.Critical)
            return
        if not input_path.lower().endswith((".xlsx", ".xls")):
            self._msg("Error", "Please select the Excel file (.xlsx or .xls)", QMessageBox.Critical)
            return
        self.execute_button.setEnabled(False)
        self.execute_button.setText("Processing")
        self.stop_button.setEnabled(True)
        self.open_result_button.setEnabled(False)
        self.loading_dots = 0
        self.loading_timer.start(500)
        self.update_thread = UpdateThread(input_path, worker_count, mode)
        self.update_thread.finished.connect(self.on_update_finished)
        self.update_thread.error.connect(self.on_update_error)
        self.update_thread.stopped.connect(self.on_update_stopped)
        self.update_thread.start()

    def stop_processing(self):
        if self.update_thread and self.update_thread.isRunning():
            reply = QMessageBox.question(
                self,
                "Confirm Stop",
                "Are you sure you want to stop the current processing?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.update_thread.stop()
                self.update_thread.wait()
                self.on_update_stopped()

    def on_update_finished(self):
        self.loading_timer.stop()
        self.execute_button.setEnabled(True)
        self.execute_button.setText("Start")
        self.stop_button.setEnabled(False)
        if self.input_field.text():
            file_name = os.path.splitext(os.path.basename(self.input_field.text()))[0]
            self.open_result_button.setText(f"Open {file_name}_result.xlsx")
        self.open_result_button.setEnabled(True)
        self._msg("Success", "Processing completed successfully!", QMessageBox.Information)

    def on_update_error(self, error_msg):
        self.loading_timer.stop()
        self.execute_button.setEnabled(True)
        self.execute_button.setText("Start")
        self.stop_button.setEnabled(False)
        self.open_result_button.setEnabled(bool(self.result_file_path and os.path.exists(self.result_file_path)))
        self._msg("Error", f"Error: {error_msg}", QMessageBox.Critical)

    def on_update_stopped(self):
        self.loading_timer.stop()
        self.execute_button.setEnabled(True)
        self.execute_button.setText("Start")
        self.stop_button.setEnabled(False)
        self.open_result_button.setEnabled(bool(self.result_file_path and os.path.exists(self.result_file_path)))
        self._msg("Stopped", "The process has been stopped by user", QMessageBox.Information)

    def open_result_file(self):
        if self.result_file_path and os.path.exists(self.result_file_path):
            try:
                if sys.platform == "win32":
                    os.startfile(self.result_file_path)
                elif sys.platform == "darwin":
                    subprocess.run(["open", self.result_file_path])
                else:
                    subprocess.run(["xdg-open", self.result_file_path])
            except Exception as e:
                self._msg("Error", f"Failed to open result file: {e}", QMessageBox.Critical)
        else:
            self._msg("Warning", "Result file not found!", QMessageBox.Warning)

    def _msg(self, title, text, icon):
        m = QMessageBox(self)
        m.setIcon(icon)
        m.setWindowTitle(title)
        m.setText(text)
        m.setStandardButtons(QMessageBox.Ok)
        m.setMinimumWidth(320)
        m.setMinimumHeight(160)
        m.exec_()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    ex = App()
    ex.show()
    sys.exit(app.exec_())
