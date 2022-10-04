import sys
import signal
import pathlib

import PyQt5.QtWidgets as qtw

from epyqtsettings.settings import Settings
from dispense.dispenser_tcp_controller import Dispenser


class ClientWindow(qtw.QWidget):
    def __init__(self):
        super().__init__(windowTitle="Dispenser Controller")
        self.settings = Settings(str(pathlib.Path(__file__).parent / "settings.dat"))
        self.dispenser = Dispenser(self.settings)
        self.dispenser.polling_period = 1

        # Set up the UI
        layout = qtw.QVBoxLayout()
        self.setLayout(layout)

        self.test_button = qtw.QPushButton("Test")
        self.test_button.clicked.connect(self.test)
        layout.addWidget(self.test_button)

        layout.addWidget(self.dispenser.make_connection_widget())
        layout.addWidget(self.dispenser.make_polling_widget())
        layout.addWidget(self.dispenser.make_trigger_widget())
        layout.addWidget(self.dispenser.make_read_only_widget())
        layout.addWidget(self.dispenser.make_date_time_widget())
        layout.addWidget(self.dispenser.make_params_widget())
        layout.addWidget(self.dispenser.make_digitals_widget())
        layout.addStretch()

    def quit(self):
        self.dispenser.shut_down()
        try:
            self.settings.save()
        except Exception:
            pass

    def test(self):
        self.dispenser.trigger = not self.dispenser.trigger


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
