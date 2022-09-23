import sys
import signal
import pathlib

import PyQt5.QtWidgets as qtw

from epyqtsettings.settings import Settings
from dispense.dispenser_tcp_controller import Dispenser


class ClientWindow(qtw.QWidget):
    def __init__(self):
        super().__init__(windowTitle="Dispenser Controller")

        # Set up the settings
        self.settings_path = str(pathlib.Path(__file__).parent / "settings.dat")
        self.settings = Settings()
        try:
            self.settings.load(self.settings_path)
        except Exception:
            pass

        # Set up the UI
        layout = qtw.QVBoxLayout()
        self.setLayout(layout)
        self.dispenser = Dispenser(self.settings)
        layout.addWidget(self.dispenser.make_ui_widget())

        """def a():
            print("    * just went high")

        def b():
            print("    * just went low")

        def c():
            print("    * just changed")

        self.dispenser.registers["digitals"].running_high.connect(a)
        self.dispenser.registers["digitals"].running_low.connect(b)
        self.dispenser.registers["digitals"].running_changed.connect(c)"""

    def quit(self):
        self.dispenser.shut_down()
        try:
            self.settings.save(self.settings_path)
        except Exception:
            pass


def make_app():
    # Global exception hook to pick up on exceptions thrown in worker threads.
    sys._excepthook = sys.excepthook

    def exception_hook(exctype, value, traceback):
        sys._excepthook(exctype, value, traceback)
        sys.exit(1)

    sys.excepthook = exception_hook

    return qtw.QApplication([])


def main(app, window):
    app.aboutToQuit.connect(window.quit)
    try:
        window.showMaximized()
    finally:
        exit(app.exec_())


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    app = make_app()
    win = ClientWindow()
    main(app, win)
