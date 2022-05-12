from PyQt5.QtWidgets import QMainWindow, QApplication, QLabel, QLineEdit, QRadioButton, QCheckBox, QPushButton, QFileDialog, QMessageBox
from PyQt5.QtGui import QFont
from PyQt5.QtCore import Qt
import sys
import PyPDF2
import subprocess

class MainWidget(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Data Extraction')
        self.setMinimumSize(1000, 910)
        self.resize(1200, 1200)
        self.setAcceptDrops(True)
        self.setStyleSheet("QMainWindow {background: '#F8F8FF';}")

        self.header_font = QFont('Open Sans', 15, QFont.Bold)
        self.regular_font = QFont('Open Sans', 12)
        self.path_font = QFont('Open Sans', 10)

        self.input_label = QLabel('Input', self)
        self.input_label.setFont(self.header_font)
        self.input_label.move(10, 10)
        self.input_label.adjustSize()

        self.file_label = QLabel('Input File: ', self)
        self.file_label.setFont(self.regular_font)
        self.file_label.move(10, 70)
        self.file_label.adjustSize()

        self.choose_file_button = QPushButton('Choose File', self)
        self.choose_file_button.setEnabled(True)
        self.choose_file_button.setFont(self.regular_font)
        self.choose_file_button.adjustSize()
        self.choose_file_button.move(170, 60)
        self.choose_file_button.clicked.connect(self.choose_file)

        self.pdf_path = QLabel('', self)
        self.pdf_path.setFont(self.path_font)
        self.pdf_path.move(365, 50)
        self.pdf_path.setWordWrap(True)
        self.pdf_path.setFixedWidth(625)
        self.pdf_path.setFixedHeight(120)
        self.pdf_path.setAlignment(Qt.AlignTop)

        self.pages_label = QLabel('Pages', self)
        self.pages_label.setFont(self.header_font)
        self.pages_label.adjustSize()
        self.pages_label.move(10, 170)

        self.start_label = QLabel('Start:', self)
        self.start_label.setFont(self.regular_font)
        self.start_label.move(10, 230)
        self.start_label.adjustSize()

        self.start_input = QLineEdit(parent=self)
        self.start_input.setFont(self.regular_font)
        self.start_input.move(100, 230)
        self.start_input.adjustSize()

        self.stop_label = QLabel('Stop:', self)
        self.stop_label.setFont(self.regular_font)
        self.stop_label.move(10, 280)
        self.stop_label.adjustSize()

        self.stop_input = QLineEdit(parent=self)
        self.stop_input.setFont(self.regular_font)
        self.stop_input.move(100, 280)
        self.stop_input.adjustSize()

        self.analysis_options_label = QLabel('Analysis Options', self)
        self.analysis_options_label.setFont(self.header_font)
        self.analysis_options_label.move(10, 350)
        self.analysis_options_label.adjustSize()

        self.text_option = QRadioButton('Text', self)
        self.text_option.setChecked(True)
        self.text_option.move(10, 410)
        self.text_option.setFont(self.regular_font)
        self.text_option.adjustSize()
        self.text_option.toggled.connect(self.update_output_options)

        self.table_option = QRadioButton('Table', self)
        self.table_option.move(150, 410)
        self.table_option.setFont(self.regular_font)
        self.table_option.adjustSize()

        self.convert_to_PNG_option = QCheckBox('Convert to PNG', self)
        self.convert_to_PNG_option.setFont(self.regular_font)
        self.convert_to_PNG_option.move(10, 460)
        self.convert_to_PNG_option.adjustSize()
        self.convert_to_PNG_option.setHidden(True)

        self.output_options_label = QLabel('Output Options', self)
        self.output_options_label.setFont(self.header_font)
        self.output_options_label.move(10, 480)
        self.output_options_label.adjustSize()

        self.label_option = QCheckBox('Labels', self)
        self.label_option.setChecked(True)
        self.label_option.setFont(self.regular_font)
        self.label_option.move(10, 540)
        self.label_option.adjustSize()

        self.relationship_option = QCheckBox('Relationship', self)
        self.relationship_option.setChecked(True)
        self.relationship_option.setFont(self.regular_font)
        self.relationship_option.move(170, 540)
        self.relationship_option.adjustSize()

        self.output_label = QLabel('Outputs', self)
        self.output_label.setFont(self.header_font)
        self.output_label.move(10, 610)
        self.output_label.adjustSize()

        self.output_folder_label = QLabel('Output Folder: ', self)
        self.output_folder_label.setFont(self.regular_font)
        self.output_folder_label.move(10, 670)
        self.output_folder_label.adjustSize()

        self.choose_folder_button = QPushButton('Choose Folder', self)
        self.choose_folder_button.setEnabled(True)
        self.choose_folder_button.setFont(self.regular_font)
        self.choose_folder_button.adjustSize()
        self.choose_folder_button.move(240, 660)
        self.choose_folder_button.clicked.connect(self.choose_folder)

        self.folder_label = QLabel('', self)
        self.folder_label.setFont(self.path_font)
        self.folder_label.setWordWrap(True)
        self.folder_label.move(475, 650)
        self.folder_label.setFixedWidth(515)
        self.folder_label.setFixedHeight(120)
        self.folder_label.adjustSize()
        self.folder_label.setAlignment(Qt.AlignTop)

        self.job_label = QLabel('Job Name: ', self)
        self.job_label.setFont(self.regular_font)
        self.job_label.move(10, 770)
        self.job_label.adjustSize()

        self.job_name = QLineEdit(parent=self)
        self.job_name.setFont(self.regular_font)
        self.job_name.move(220, 770)
        self.job_name.setFixedWidth(730)
        self.job_name.adjustSize()

        self.submit = QPushButton('Submit', self)
        self.submit.setFont(self.regular_font)
        self.submit.adjustSize()
        self.submit.move(10, 840)
        self.submit.clicked.connect(self.check_form)

        self.file_path = None
        self.folder_path = None

    def update_output_options(self):
        if self.text_option.isChecked():
            self.output_options_label.setHidden(False)
            self.label_option.setHidden(False)
            self.relationship_option.setHidden(False)
            self.convert_to_PNG_option.setHidden(True)
        else:
            self.output_options_label.setHidden(True)
            self.label_option.setHidden(True)
            self.relationship_option.setHidden(True)
            self.convert_to_PNG_option.setHidden(False)
    
    def choose_folder(self):
        dialog = QFileDialog()
        self.folder_path = dialog.getExistingDirectory(None, "Select Folder")
        self.folder_label.setText(self.folder_path)

    def choose_file(self):
        dialog = QFileDialog()
        self.file_path, _ = dialog.getOpenFileName(None, "Select File")
        self.pdf_path.setText(self.file_path)

    def create_message_box(self, message):
        self.valid_form = False
        message_box = QMessageBox(parent=self, text=message)
        message_box.exec()

    def check_form(self):
        self.start_value = None
        self.stop_value = None
        self.valid_form = True
        if self.start_input.text() != '':
            try: 
                self.start_value = int(self.start_input.text())
            except:
                self.create_message_box('Please insert a valid integer for the start value')
        else:
            self.start_value = None
        if self.stop_input.text() != '':
            try: 
                self.stop_value = int(self.stop_input.text())
            except:
                self.create_message_box('Please insert a valid integer for the stop value')
        else:
            self.stop_value = None
        if self.start_value is not None and self.stop_value is not None and self.start_value > self.stop_value:
            self.create_message_box('Start page should be less than stop page')
        if self.file_path is not None:
            pdfReader = None
            self.num_pages = -1
            try:
                pdfReader = PyPDF2.PdfFileReader(open(self.file_path, 'rb'))
                self.num_pages = pdfReader.numPages
            except:
                self.create_message_box('Invalid PDF file')
            if pdfReader is not None and ((self.start_value is not None and (self.start_value > self.num_pages or self.start_value < 1)) or
                (self.stop_value is not None and (self.stop_value > self.num_pages or self.stop_value < 1))):
                self.create_message_box('Start page value and/or stop page value not within the PDF page count')
        else:
            self.create_message_box('Please drag and drop a valid PDF file')
        if self.folder_path is None:
            self.create_message_box('Please choose folder to store outputs')
        if self.job_name.text() == '':
            self.create_message_box('Please input a job name')
        if self.valid_form:
            command = [r'C:\ProgramData\Anaconda3\envs\TextractPrototype\python.exe', r'C:\Users\Jeffrey.Lu\PycharmProjects\TextractPrototype\main.py']
            if self.text_option.isChecked():
                command.append('text')
            if self.table_option.isChecked():
                command.append('table')
            command.append(self.file_path)
            command.append(self.folder_path)
            command.append(self.job_name.text())
            if self.start_value is not None and self.stop_value is None:
                command.extend(['--start', str(self.start_value), '--stop', str(self.num_pages)])
            elif self.stop_value is not None and self.start_value is None:
                command.extend(['--start', '1', '--stop', str(self.stop_value)])
            elif self.start_value is not None and self.stop_value is not None:
                command.extend(['--start', str(self.start_value), '--stop', str(self.stop_value)])
            if self.text_option.isChecked():
                if self.label_option.isChecked():
                    command.append('--labels')
                if self.relationship_option.isChecked():
                    command.append('--relationships')
            if self.table_option.isChecked():
                if self.convert_to_PNG_option.isChecked():
                    command.append('--png')
            print(subprocess.run(command))

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = MainWidget()
    ui.show()
    sys.exit(app.exec_())