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
    QButtonGroup,
    QFrame,
    QSpinBox,
)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
import sys
import os
import subprocess
from update import UpdateTaobaoID


class UpdateThread(QThread):
    finished = pyqtSignal()
    error = pyqtSignal(str)

    def __init__(self, input_path, worker_count):
        super().__init__()
        self.input_path = input_path
        self.worker_count = worker_count

    def run(self):
        try:
            update_taobao_id = UpdateTaobaoID(source_file=self.input_path, max_workers=self.worker_count)
            update_taobao_id.update_scipts()
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))


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
        self.init_ui()
        self.loading_timer = QTimer()
        self.loading_timer.timeout.connect(self.update_loading_text)
        self.loading_dots = 0
        self.update_thread = None

    def init_ui(self):
        self.setWindowTitle("Taobao ID Update Tool")
        self.setGeometry(100, 100, 800, 400)
        self.setStyleSheet(
            """
            QWidget {
                background-color: #f5f5f5;
                font-family: 'Microsoft YaHei', Arial;
            }
            QLabel {
                font-size: 14px;
                color: #333;
                min-width: 80px;
            }
            QPushButton {
                font-size: 14px;
                padding: 8px 20px;
                border-radius: 5px;
            }
            QPushButton:disabled {
                background-color: #cccccc !important;
                color: #666666 !important;
                border: none !important;
            }
            QSpinBox {
                padding: 5px;
                border: 2px solid #ddd;
                border-radius: 5px;
                font-size: 14px;
                min-width: 60px;
                max-width: 60px;
            }
            QSpinBox:focus {
                border: 2px solid #4CAF50;
            }
        """
        )

        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        title_label = QLabel("Taobao ID Update Tool")
        title_label.setStyleSheet(
            """
            font-size: 24px;
            color: #333;
            font-weight: bold;
            margin-bottom: 20px;
        """
        )
        title_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title_label)

        input_container = QFrame()
        input_container.setStyleSheet(
            """
            QFrame {
                background-color: white;
                border-radius: 10px;
                padding: 20px;
            }
        """
        )
        input_layout = QVBoxLayout(input_container)
        input_layout.setSpacing(15)

        file_input_layout = QHBoxLayout()
        self.input_label = QLabel("File Path：")
        self.input_field = DragDropLineEdit(self)
        self.input_field.setPlaceholderText("Drag and drop file here or click browse button")
        self.input_field.textChanged.connect(self.check_input_file)
        self.input_button = QPushButton("Browse", self)
        self.input_button.setStyleSheet(
            """
            QPushButton {
                background-color: #2196F3;
                color: white;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
        """
        )
        self.input_button.clicked.connect(self.select_input_file)

        file_input_layout.addWidget(self.input_label)
        file_input_layout.addWidget(self.input_field, 1)
        file_input_layout.addWidget(self.input_button)
        input_layout.addLayout(file_input_layout)

        worker_layout = QHBoxLayout()
        self.worker_label = QLabel("Worker：")
        self.worker_spinbox = QSpinBox()
        self.worker_spinbox.setRange(1, 50)
        self.worker_spinbox.setValue(5)
        self.worker_spinbox.setSingleStep(1)
        worker_layout.addWidget(self.worker_label)
        worker_layout.addWidget(self.worker_spinbox)
        worker_layout.addStretch()
        input_layout.addLayout(worker_layout)

        main_layout.addWidget(input_container)

        button_container = QFrame()
        button_layout = QHBoxLayout(button_container)
        button_layout.setSpacing(15)

        self.execute_button = QPushButton("Start", self)
        self.execute_button.setStyleSheet(
            """
            QPushButton {
                background-color: #4CAF50;
                color: white;
                min-height: 40px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """
        )
        self.execute_button.setCursor(Qt.PointingHandCursor)
        self.execute_button.clicked.connect(self.generate_report_handler)
        button_layout.addWidget(self.execute_button, 1)

        self.open_result_button = QPushButton("Open Result", self)
        self.open_result_button.setStyleSheet(
            """
            QPushButton {
                background-color: #FF9800;
                color: white;
                min-height: 40px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #F57C00;
            }
        """
        )
        self.open_result_button.setCursor(Qt.PointingHandCursor)
        self.open_result_button.clicked.connect(self.open_result_file)
        self.open_result_button.setEnabled(False)
        button_layout.addWidget(self.open_result_button, 1)

        main_layout.addWidget(button_container)
        self.setLayout(main_layout)

    def check_input_file(self):
        self.open_result_button.setEnabled(bool(self.input_field.text()))

    def update_loading_text(self):
        self.loading_dots = (self.loading_dots + 1) % 4
        dots = "." * self.loading_dots
        self.execute_button.setText(f"Processing{dots}")

    def select_input_file(self):
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getOpenFileName(self, "Select the input Excel file", "", "Excel Files (*.xlsx *.xls);;All Files (*)", options=options)
        if file_path:
            self.input_field.setText(file_path)
            file_dir = os.path.dirname(file_path)
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            self.result_file_path = os.path.join(file_dir, f"{file_name}_result.xlsx")

    def open_result_file(self):
        if self.result_file_path and os.path.exists(self.result_file_path):
            try:
                if sys.platform == "win32":
                    os.startfile(self.result_file_path)
                elif sys.platform == "darwin":  # macOS
                    subprocess.run(["open", self.result_file_path])
                else:  # linux
                    subprocess.run(["xdg-open", self.result_file_path])
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to open result file: {e}")
        else:
            QMessageBox.warning(self, "Warning", "Result file not found!")

    def generate_report_handler(self):
        input_path = self.input_field.text()
        worker_count = self.worker_spinbox.value()

        if not input_path:
            QMessageBox.critical(self, "Error", "Please select the input file path")
            return

        if not input_path.lower().endswith((".xlsx", ".xls")):
            QMessageBox.critical(self, "Error", "Please select the Excel file (.xlsx or .xls)")
            return

        # Set loading state immediately
        self.execute_button.setEnabled(False)
        self.execute_button.setText("Processing...")
        self.open_result_button.setEnabled(False)
        self.loading_timer.start(500)

        # Create and start the update thread
        self.update_thread = UpdateThread(input_path, worker_count)
        self.update_thread.finished.connect(self.on_update_finished)
        self.update_thread.error.connect(self.on_update_error)
        self.update_thread.start()

    def on_update_finished(self):
        self.loading_timer.stop()
        self.execute_button.setEnabled(True)
        self.execute_button.setText("Start")

        file_name = os.path.splitext(os.path.basename(self.input_field.text()))[0]
        self.open_result_button.setText(f"Open {file_name}_result.xlsx")
        self.open_result_button.setEnabled(True)

        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle("Success")
        msg.setText("Processing completed successfully!")
        msg.setStandardButtons(QMessageBox.Ok)
        msg.exec_()

    def on_update_error(self, error_msg):
        self.loading_timer.stop()
        self.execute_button.setEnabled(True)
        self.execute_button.setText("Start")

        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle("Error")
        msg.setText(f"Error: {error_msg}")
        msg.setStandardButtons(QMessageBox.Ok)
        msg.exec_()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    ex = App()
    ex.show()
    sys.exit(app.exec_())
