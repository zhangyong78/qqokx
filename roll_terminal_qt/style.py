APP_STYLE = """
QMainWindow {
    background: #eef3f8;
}
QWidget {
    color: #1f2937;
    font-size: 11px;
    background: #eef3f8;
}
QLabel {
    background: transparent;
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
    font-size: 18px;
    font-weight: 600;
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
    font-size: 13px;
    font-weight: 600;
}
QLabel#SectionTitle,
QLabel#GuideTitle {
    color: #0f172a;
    font-size: 12px;
    font-weight: 600;
}
QLabel#Badge {
    color: #334155;
    background: #f8fafc;
    border: 1px solid #d7e0ea;
    border-radius: 8px;
    padding: 4px 8px;
}
QMenuBar {
    background: #ffffff;
    border-bottom: 1px solid #d7e0ea;
    spacing: 4px;
}
QMenuBar::item {
    background: transparent;
    color: #0f172a;
    padding: 6px 10px;
    border-radius: 7px;
    margin: 3px 2px;
}
QMenuBar::item:selected {
    background: #eff6ff;
    color: #1d4ed8;
}
QMenuBar::item:pressed {
    background: #dbeafe;
    color: #1d4ed8;
}
QMenu {
    background: #ffffff;
    color: #0f172a;
    border: 1px solid #d7e0ea;
    border-radius: 10px;
    padding: 6px;
}
QMenu::item {
    padding: 7px 28px 7px 12px;
    border-radius: 7px;
    background: transparent;
}
QMenu::item:selected {
    background: #eff6ff;
    color: #1d4ed8;
}
QMenu::separator {
    height: 1px;
    background: #e5ebf2;
    margin: 4px 6px;
}
QComboBox,
QLineEdit,
QTextEdit,
QPlainTextEdit {
    min-height: 28px;
    border: 1px solid #cbd5e1;
    border-radius: 7px;
    padding: 2px 8px;
    background: #ffffff;
    color: #111827;
    selection-background-color: #dbeafe;
}
QComboBox:hover,
QLineEdit:hover,
QTextEdit:hover,
QPlainTextEdit:hover {
    border: 1px solid #93c5fd;
    background: #fdfefe;
}
QComboBox QAbstractItemView {
    background: #ffffff;
    color: #111827;
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    selection-background-color: #dbeafe;
    selection-color: #111827;
}
QComboBox::drop-down {
    subcontrol-origin: padding;
    subcontrol-position: top right;
    width: 24px;
    border-left: 1px solid #e2e8f0;
    background: #f8fafc;
    border-top-right-radius: 7px;
    border-bottom-right-radius: 7px;
}
QComboBox::drop-down:hover {
    background: #eff6ff;
}
QLineEdit:focus,
QComboBox:focus,
QTextEdit:focus,
QPlainTextEdit:focus {
    border: 1px solid #3b82f6;
    background: #ffffff;
}
QPushButton {
    min-height: 30px;
    border: 1px solid #cbd5e1;
    border-radius: 7px;
    padding: 3px 10px;
    background: #ffffff;
    color: #1f2937;
    font-weight: 500;
}
QPushButton:hover {
    background: #f8fafc;
    border: 1px solid #93c5fd;
}
QPushButton:pressed {
    background: #e2e8f0;
    border: 1px solid #94a3b8;
    padding-top: 4px;
    padding-right: 9px;
    padding-bottom: 2px;
    padding-left: 11px;
}
QPushButton:checked {
    background: #dbeafe;
    border: 1px solid #60a5fa;
    color: #1d4ed8;
}
QPushButton:disabled {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    color: #94a3b8;
}
QPushButton#Primary {
    background: #2563eb;
    border: 1px solid #2563eb;
    color: white;
}
QPushButton#Primary:hover {
    background: #1d4ed8;
    border: 1px solid #1d4ed8;
}
QPushButton#Primary:pressed {
    background: #1e40af;
    border: 1px solid #1e3a8a;
}
QPushButton#Danger {
    background: #e11d48;
    border: 1px solid #e11d48;
    color: white;
}
QPushButton#Danger:hover {
    background: #be123c;
    border: 1px solid #be123c;
}
QPushButton#Danger:pressed {
    background: #9f1239;
    border: 1px solid #881337;
}
QPushButton#Secondary {
    background: #ffffff;
}
QPushButton#Secondary:hover {
    background: #f8fafc;
    border: 1px solid #93c5fd;
}
QPushButton#Secondary:pressed {
    background: #e2e8f0;
    border: 1px solid #94a3b8;
}
QToolButton {
    min-height: 28px;
    border: 1px solid #cbd5e1;
    border-radius: 7px;
    padding: 3px 10px;
    background: #ffffff;
    color: #1f2937;
}
QToolButton:hover {
    background: #f8fafc;
    border: 1px solid #93c5fd;
}
QToolButton:pressed,
QToolButton:checked {
    background: #dbeafe;
    border: 1px solid #60a5fa;
    color: #1d4ed8;
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
QTableWidget::item:hover,
QListWidget::item:hover {
    background: #f8fbff;
}
QListWidget::item {
    padding: 6px 8px;
    border-bottom: 1px solid #eef2f7;
}
QListWidget::item:selected {
    background: #e8f1ff;
    color: #0f172a;
}
QTabWidget::pane {
    border: 1px solid #d7e0ea;
    border-radius: 10px;
    background: #ffffff;
    top: -1px;
}
QTabBar::tab {
    background: #f8fafc;
    color: #475569;
    border: 1px solid #d7e0ea;
    border-bottom: none;
    border-top-left-radius: 8px;
    border-top-right-radius: 8px;
    padding: 5px 12px;
    margin-right: 4px;
    min-height: 24px;
}
QTabBar::tab:hover {
    background: #eff6ff;
    color: #1d4ed8;
}
QTabBar::tab:selected {
    background: #ffffff;
    color: #0f172a;
    border-color: #93c5fd;
    font-weight: 600;
}
QTabBar::tab:pressed {
    background: #dbeafe;
}
QHeaderView::section {
    background: #f8fafc;
    color: #64748b;
    border: none;
    border-bottom: 1px solid #d7e0ea;
    padding: 2px 5px;
    font-weight: 500;
}
QHeaderView::section:hover {
    background: #f1f5f9;
    color: #475569;
}
QHeaderView::section:pressed {
    background: #e2e8f0;
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
    border: 1px solid #93c5fd;
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
QScrollBar::handle:vertical:hover {
    background: #94a3b8;
}
QScrollBar::handle:vertical:pressed {
    background: #64748b;
}
QScrollBar:horizontal {
    background: #f8fafc;
    height: 10px;
    margin: 2px;
}
QScrollBar::handle:horizontal {
    background: #cbd5e1;
    border-radius: 5px;
    min-width: 30px;
}
QScrollBar::handle:horizontal:hover {
    background: #94a3b8;
}
QScrollBar::handle:horizontal:pressed {
    background: #64748b;
}
QScrollBar::add-line:horizontal,
QScrollBar::sub-line:horizontal,
QScrollBar::add-page:horizontal,
QScrollBar::sub-page:horizontal,
QScrollBar::add-line:vertical,
QScrollBar::sub-line:vertical,
QScrollBar::add-page:vertical,
QScrollBar::sub-page:vertical {
    background: transparent;
    height: 0px;
}
QSplitter::handle {
    background: #d7e0ea;
}
QSplitter::handle:horizontal {
    width: 8px;
    margin: 0 2px;
    border-left: 1px solid #cbd5e1;
    border-right: 1px solid #f8fafc;
}
QSplitter::handle:vertical {
    height: 8px;
    margin: 2px 0;
    border-top: 1px solid #cbd5e1;
    border-bottom: 1px solid #f8fafc;
}
QSplitter::handle:hover {
    background: #bfdbfe;
}
QSplitter::handle:pressed {
    background: #93c5fd;
}
"""
