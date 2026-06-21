APP_STYLE = """
QMainWindow {
    background: #eef3f8;
}
QWidget {
    color: #1f2937;
    font-size: 11px;
    background: #eef3f8;
}
QFrame#Header,
QFrame#Panel,
QFrame#Guide,
QFrame#StatCard {
    background: #ffffff;
    border: 1px solid #d7e0ea;
    border-radius: 12px;
}
QLabel#Title {
    color: #0f172a;
    font-size: 20px;
    font-weight: 700;
}
QLabel#Subtitle,
QLabel#GuideText,
QLabel#Hint,
QLabel#Subtle,
QLabel#StatTitle {
    color: #64748b;
    font-size: 11px;
}
QLabel#Metric {
    color: #0f172a;
    font-size: 14px;
    font-weight: 700;
}
QLabel#SectionTitle,
QLabel#GuideTitle {
    color: #0f172a;
    font-size: 13px;
    font-weight: 700;
}
QLabel#Badge {
    color: #334155;
    background: #f8fafc;
    border: 1px solid #d7e0ea;
    border-radius: 10px;
    padding: 6px 10px;
}
QComboBox,
QLineEdit {
    min-height: 30px;
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    padding: 3px 8px;
    background: #ffffff;
    color: #111827;
    selection-background-color: #dbeafe;
}
QComboBox QAbstractItemView {
    background: #ffffff;
    color: #111827;
    border: 1px solid #cbd5e1;
    selection-background-color: #dbeafe;
    selection-color: #111827;
}
QLineEdit:focus,
QComboBox:focus {
    border: 1px solid #3b82f6;
}
QPushButton {
    min-height: 32px;
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    padding: 4px 12px;
    background: #ffffff;
    color: #1f2937;
    font-weight: 600;
}
QPushButton:hover {
    background: #f8fafc;
}
QPushButton#Primary {
    background: #2563eb;
    border: 1px solid #2563eb;
    color: white;
}
QPushButton#Primary:hover {
    background: #1d4ed8;
}
QPushButton#Danger {
    background: #e11d48;
    border: 1px solid #e11d48;
    color: white;
}
QPushButton#Danger:hover {
    background: #be123c;
}
QPushButton#Secondary {
    background: #ffffff;
}
QCheckBox {
    color: #1f2937;
    spacing: 8px;
    background: transparent;
}
QCheckBox::indicator {
    width: 16px;
    height: 16px;
}
QCheckBox::indicator:unchecked {
    border: 1px solid #94a3b8;
    background: #ffffff;
    border-radius: 4px;
}
QCheckBox::indicator:checked {
    border: 1px solid #2563eb;
    background: #2563eb;
    border-radius: 4px;
}
QTableWidget,
QListWidget,
QTextEdit {
    background: #ffffff;
    border: 1px solid #d7e0ea;
    border-radius: 10px;
    color: #1f2937;
    gridline-color: #e5ebf2;
    selection-background-color: #e8f1ff;
    selection-color: #111827;
}
QTableWidget#OrderBookTable {
    font-size: 9px;
}
QListWidget::item {
    padding: 7px 8px;
    border-bottom: 1px solid #eef2f7;
}
QListWidget::item:selected {
    background: #e8f1ff;
    color: #0f172a;
}
QHeaderView::section {
    background: #f8fafc;
    color: #64748b;
    border: none;
    border-bottom: 1px solid #d7e0ea;
    padding: 3px 5px;
    font-weight: 600;
}
QInputDialog,
QMessageBox {
    background: #ffffff;
}
QInputDialog QLabel,
QMessageBox QLabel {
    color: #1f2937;
    font-size: 13px;
}
QInputDialog QLineEdit {
    min-height: 36px;
    background: #ffffff;
    color: #111827;
    border: 1px solid #94a3b8;
    border-radius: 8px;
    padding: 4px 10px;
}
QInputDialog QPushButton,
QMessageBox QPushButton {
    min-width: 84px;
    background: #ffffff;
    color: #1f2937;
    border: 1px solid #cbd5e1;
}
QInputDialog QPushButton:hover,
QMessageBox QPushButton:hover {
    background: #f8fafc;
}
QScrollBar:vertical {
    background: #f8fafc;
    width: 10px;
    margin: 2px;
}
QScrollBar::handle:vertical {
    background: #cbd5e1;
    border-radius: 5px;
    min-height: 30px;
}
QScrollBar::add-line:vertical,
QScrollBar::sub-line:vertical,
QScrollBar::add-page:vertical,
QScrollBar::sub-page:vertical {
    background: transparent;
    height: 0px;
}
"""
