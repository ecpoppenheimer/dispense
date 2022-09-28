import queue
import types

import PyQt5.QtWidgets as qtw
import PyQt5.QtGui as qtg
import PyQt5.QtNetwork as qtn
import PyQt5.QtCore as qtc
import numpy as np

from epyqtwidgets.ip4validator import IP4Validator
import epyqtsettings.settings_widgets as sw


class Dispenser:
    """
    Public Methods
    --------------
    make_ui_widget
    connect
    disconnect
    shut_down
    read
    write
    do_deferred_writes
    set_system_time
    data_changed
    time_changed
    get_digital
    read_digital
    set_digital
    check_write_digital

    Public Attributes
    -----------------
    settings
    registers
    defer_read
    defer_write
    digitals_names
    polling_period
    polling_active
    """
    """
    It is tricky to design an interface to this dispenser that doesn't block on sending a command and sensibly handles
    errors.  For every command sent, we should receive either a copy of the command, or an error message, and all
    responses *should* occur in order.  I don't know if this will happen in practice, but at the moment I am going to
    rely on responses being returned in order and use a queue to make sure that each response sent is either
    acknowledged as good or as an error.  In theory, errors should only ever even get sent if this class sends a
    malformed message, so with good design they shouldn't even happen, but I am going to try to account for them anyway.
    """
    _initialized = False
    DEFAULT_PORT = 9000

    # Commands to send to the dispenser.
    READ = 3
    WRITE = 16

    # Commands received from the dispenser.
    # Sucessful read returns a 3
    # Sucessful write returns a 16
    READ_ERROR = 131
    WRITE_ERROR = 144
    ERROR_CODES = {
        -1: "Data Format Error",
        -2: "Data Limit Error"
    }

    # Register stuff
    """
    Name                Readable        Writable        Read Order
    Digitals            Y               Y               
    Model Type          Y               N               0
    Program NUM         Y               Y               5
    Dispense Mode       Y               Y
    Dispense Time       Y               Y
    Pressure            Y               Y
    Vacuum              Y               Y
    System Count        Y               N               1
    Shot Count          Y               N               2
    MultiShot Count     Y               Y
    MultiShot Time      Y               Y
    System Date         Y               Y               6
    System Time         Y               Y               7
    Software Version    Y               N               3
    Pressure Units      Y               Y
    Vacuum Units        Y               Y
    Time Format         Y               N               4
    """
    READ_REG_COUNT = 17
    READ_REGISTERS = [
        "digitals",
        "model_type",
        "program_num",
        "dispense_mode",
        "dispense_time",
        "pressure",
        "vacuum",
        "system_count",
        "shot_count",
        "multishot_count",
        "multishot_time",
        "system_date",
        "system_time",
        "software_version",
        "pressure_units",
        "vacuum_units",
        "time_format",
    ]
    WRITE_REG_COUNT = 12
    WRITE_REGISTERS = [
        "digitals",
        "program_num",
        "dispense_mode",
        "dispense_time",
        "pressure_units",
        "pressure",
        "vacuum_units",
        "vacuum",
        "multishot_count",
        "multishot_time",
        "system_date",
        "system_time",
    ]
    WRITE_INDICES = np.arange(WRITE_REG_COUNT)

    BLANK_WRITE_DATA = np.array(
        [0, 1, 1, 1, 1, 3, 0, 0, 0, 0, 1, 19011213, 0, 0, 0, 0, 0, 0, 0, 0],
        dtype=np.int32
    )

    def __init__(self, settings, ip=None, port=None):
        self.settings = settings
        self._data = b""
        if ip is None:
            ip = "localhost"
        if port is None:
            port = self.DEFAULT_PORT
        self.settings.establish_defaults(
            dispenser_port=port, dispenser_ip=ip, polling_active=True, polling_period=1.0
        )
        self.connection_event = ConnectionChanged()

        self._server_socket = None
        self._ui = types.SimpleNamespace()
        self._connection_ready = False
        self._debug = False
        self._polling_timer = qtc.QTimer()
        self.polling_period = self.settings.polling_period
        self._polling_timer.timeout.connect(self._read_all)
        self._polling_active = self.settings.polling_active

        # Define the registers.  These are objects that handle all formatting and type checking, and act as
        # public attributes of the class that can be read and written (if appropriate), and will automatically trigger
        # TCP communications, unless the defer flags are set.
        self.registers = {}
        self.registers.update({
            "digitals": DigitalRegister(self, "digitals", 0, 0),
            "model_type": ModelTypeRegister(self, "model_type", 1, 2, int, 1),
            "system_count": Register(self, "system_count", 0, 2147483647, int, 7),
            "shot_count": Register(self, "shot_count", 0, 2147483647, int, 8),
            "software_version": Register(self, "software_version", 0, 2147483647, int, 13, scale=1000),
            "time_format": TimeFormatRegister(self, "time_format", 0, 2, int, 16),
            "program_num": Register(self, "program_num", 1, 16, int, 2, write_index=1),
            "system_date": DateRegister(self, "system_date", 19011213, 20380119, int, 11, write_index=10),
            "system_time": TimeRegister(self, "system_time", 0, 235959, int, 12, write_index=11),
            "dispense_mode": SelectableRegister(self, "dispense_mode", {
                1: "single_shot", 2: "steady_mode", 3: "teach_mode", 4: "multishot"
            }, 3, 2),
            "dispense_time": Register(self, "dispense_time", .0001, 9999, float, 4, 3, scale=10000, unit_type="s"),
            "pressure_units": SelectableRegister(self, "pressure_units", {
                0: "psi", 1: "bar", 2: "kpa"
            }, 14, 4, selection_callback=self._update_pressure_units),
            "pressure": Register(self, "pressure", .68, 689.4, float, 5, 5, scale=100, unit_type=14),
            "vacuum_units": SelectableRegister(self, "vacuum_units", {
                0: "inches_water", 1: "inches_Hg", 2: "kpa"
            }, 15, 6, selection_callback=self._update_vacuum_units),
            "vacuum": Register(self, "vacuum", 0, 18.0, float, 6, 7, scale=100, unit_type=15),
            "multishot_count": Register(self, "multishot_count", 0, 9999, int, 9, 8),
            "multishot_time": Register(self, "multishot_time", .1, 999.9, float, 10, 9, scale=100, unit_type="s"),
        })
        self._write_registers_by_index = [self.registers[name] for name in self.WRITE_REGISTERS]
        self._read_registers_by_index = [self.registers[name] for name in self.READ_REGISTERS]
        self._register_keys = set(self.registers.keys())
        self._deferred_writes = np.zeros((self.WRITE_REG_COUNT,), dtype=bool)

        # Message queue.  Messages should be added here as a tuple of (data, callback), and the callback will
        # be fired when the message is acknowledged.
        self._message_queue = queue.Queue()
        self._initialized = True

        self._make_ui_widget()

    @property
    def ui_widget(self):
        return self._ui.base_widget

    def _make_ui_widget(self):
        self._ui.base_widget = qtw.QWidget()
        layout = qtw.QGridLayout()
        self._ui.base_widget.setLayout(layout)
        ui_row = 0

        # IP controls
        self._ui.ip_entry = sw.SettingsEntryBox(self.settings, "dispenser_ip", str, validator=IP4Validator())
        self._ui.ip_entry.edit_box.textChanged.connect(self._ip_changed)
        layout.addWidget(self._ui.ip_entry, ui_row, 0, 1, 12)
        ui_row += 1

        self._ui.port_entry = sw.SettingsEntryBox(
            self.settings, "dispenser_port", int, validator=qtg.QIntValidator(0, 65535)
        )
        layout.addWidget(self._ui.port_entry, ui_row, 0, 1, 12)
        ui_row += 1

        self._ui.connect_button = qtw.QPushButton("Connect")
        self._ui.connect_button.clicked.connect(self._click_connect)
        layout.addWidget(self._ui.connect_button, ui_row, 0, 1, 4)

        # Check box to show the registers
        self._ui.show_registers = qtw.QCheckBox("Show Registers")
        self._ui.show_registers.setChecked(0)
        self._ui.show_registers.setTristate(False)
        self._ui.show_registers.stateChanged.connect(self._set_show_registers)
        layout.addWidget(self._ui.show_registers, ui_row, 6, 1, 3)

        # Test button
        self._ui.test_button = qtw.QPushButton("Test")
        if self._debug:
            self._ui.test_button.clicked.connect(self._run_test)
            layout.addWidget(self._ui.test_button, ui_row, 9, 1, 3)
        ui_row += 1

        self._ui.reg_widget = qtw.QWidget()
        self._ui.reg_widget.hide()
        reg_layout = qtw.QGridLayout()
        reg_ui_row = 0
        self._ui.reg_widget.setLayout(reg_layout)
        layout.addWidget(self._ui.reg_widget, ui_row, 0, 1, 12)
        ui_row += 1

        # Controls for deferred read/write
        self._ui.defer_read_checkbox = qtw.QCheckBox("Defer Reads")
        self._ui.defer_read_checkbox.setChecked(0)
        self._ui.defer_read_checkbox.setTristate(False)
        reg_layout.addWidget(self._ui.defer_read_checkbox, reg_ui_row, 0, 1, 3)

        self._ui.read_button = qtw.QPushButton("Read All")
        self._ui.read_button.clicked.connect(self._read_all)
        self._ui.read_button.setEnabled(False)
        reg_layout.addWidget(self._ui.read_button, reg_ui_row, 3, 1, 3)

        self._ui.defer_write_checkbox = qtw.QCheckBox("Defer Writes")
        self._ui.defer_write_checkbox.setChecked(0)
        self._ui.defer_write_checkbox.setTristate(False)
        self._ui.defer_write_checkbox.stateChanged.connect(self._clicked_defer_write)
        reg_layout.addWidget(self._ui.defer_write_checkbox, reg_ui_row, 6, 1, 3)

        self._ui.do_defer_write_button = qtw.QPushButton("Write Now")
        self._ui.do_defer_write_button.setEnabled(False)
        self._ui.do_defer_write_button.clicked.connect(self.do_deferred_writes)
        reg_layout.addWidget(self._ui.do_defer_write_button, reg_ui_row, 9, 1, 3)
        reg_ui_row += 1

        # Polling controls
        def set_polling_period():
            self.polling_period = self.settings.polling_period

        def set_polling_active():
            self.polling_active = self.settings.polling_active

        self._ui.polling_active_entry = sw.SettingsCheckBox(
            self.settings, "polling_active", "Activate Read Polling", callback=set_polling_active
        )
        reg_layout.addWidget(self._ui.polling_active_entry, reg_ui_row, 0, 1, 6)

        self._ui.polling_period_entry = sw.SettingsEntryBox(
            self.settings, "polling_period", float, qtg.QDoubleValidator(.001, 1000, 3), set_polling_period
        )
        reg_layout.addWidget(self._ui.polling_period_entry, reg_ui_row, 6, 1, 6)
        reg_ui_row += 1

        # Date time widget is separate from the register widgets, since this is a single widget to control two
        # registers.  The registers will hold the real value, and this widget will send/receive updates to the values
        # held by those registers.
        reg_layout.addWidget(qtw.QLabel("Dispenser Time"), reg_ui_row, 0, 1, 3)
        self._ui.date_time_picker = qtw.QDateTimeEdit()
        self._ui.date_time_picker.setCalendarPopup(True)
        self._ui.date_time_picker.setMinimumDate(qtc.QDate(1901, 12, 13))
        self._ui.date_time_picker.setMaximumDate(qtc.QDate(2038, 1, 19))
        self._ui.date_time_picker.dateChanged.connect(self.date_changed)
        self._ui.date_time_picker.timeChanged.connect(self.time_changed)
        reg_layout.addWidget(self._ui.date_time_picker, reg_ui_row, 3, 1, 6)
        self._ui.now_time_button = qtw.QPushButton("Get Time")
        self._ui.now_time_button.clicked.connect(self.set_system_time)
        reg_layout.addWidget(self._ui.now_time_button, reg_ui_row, 9, 1, 3)
        reg_ui_row += 1

        # The register widgets
        for r in self.registers.values():
            w = r.make_widget()
            if w is not None:
                reg_layout.addWidget(w, reg_ui_row, 0, 1, 12)
                reg_ui_row += 1
        self.registers["digitals"].set_enabled(False)

        return self._ui.base_widget

    def connect(self, address=None, port=None):
        if address is None:
            address = self.settings.dispenser_ip
        if port is None:
            port = self.settings.dispenser_port
        self._server_socket = qtn.QTcpSocket(self._ui.base_widget)
        self._server_socket.connectToHost(address, port)
        self._server_socket.connected.connect(self._got_connection)

    def _click_connect(self):
        if self._server_socket is None:
            self.connect()
            self._ui.connect_button.setText("Working...")
        else:
            self.disconnect()

    def _got_connection(self):
        if self._server_socket is not None:
            self._ui.connect_button.setText("Disconnect")
            self._server_socket.disconnected.connect(self.disconnect)
            self._server_socket.errorOccurred.connect(self._socket_error)
            self._server_socket.readyRead.connect(self._read_chunks)
            self._ui.read_button.setEnabled(True)
            self.registers["digitals"].set_enabled(True)
            self.read(callback=self._initial_read_completed)
            self.connection_event.emit(True)

    def _initial_read_completed(self, _):
        self._connection_ready = True
        self.set_system_time()
        self._try_toggle_polling()

    def _ip_changed(self):
        state = self._ui.ip_entry.edit_box.validator().validate(self._ui.ip_entry.edit_box.text(), 0)[0]
        if state == qtg.QValidator.Acceptable:
            color = "white"
        else:
            color = "pink"
        self._ui.ip_entry.setStyleSheet("QLineEdit { background-color: %s }" % color)

    def disconnect(self):
        if self._server_socket is not None:
            self._server_socket.close()
        self._server_socket = None
        self._ui.read_button.setEnabled(False)
        self._ui.defer_write_checkbox.setChecked(0)
        self._ui.defer_read_checkbox.setChecked(0)
        self._ui.do_defer_write_button.setEnabled(False)
        self._ui.connect_button.setText("Connect")
        self.registers["digitals"].set_enabled(False)
        self._connection_ready = False
        self._try_toggle_polling()
        self.connection_event.emit(False)

    def shut_down(self):
        self.disconnect()

    def _socket_error(self, error):
        print(f"received socket error: {error}")
        self.disconnect()

    def _read_chunks(self):
        if self._server_socket is None:
            return

        self._data += self._server_socket.read(self._server_socket.bytesAvailable())
        delimiter_pos = self._data.find(b';')
        while delimiter_pos > 0:
            chunk = self._data[:delimiter_pos]
            chunk_data = np.asarray(chunk.split(b','))
            chunk_data =tuple(int(b.decode("utf-8")) for b in chunk_data)
            self._data = self._data[delimiter_pos + 1:]
            delimiter_pos = self._data.find(b';')

            # Match this chunk to the message queue, to make sure that the message was correctly acknowledged, or
            # forward the error if an error was received.
            original_message, callback, error_callback, digital = self._message_queue.get()

            if np.all(chunk_data[:3] == original_message[:3]):
                # command was successful
                if chunk_data[0] == self.READ:
                    self._update_ui(chunk_data[3:], original_message)
                    if callback is not None:
                        if digital is not None:
                            callback(digital, self.get_digital(digital))
                        else:
                            labeled_data = self._unpack(chunk_data[1:], self.READ_REGISTERS)
                            callback(labeled_data)
                else:  # Should only ever reach here if header[0] == self.WRITE
                    self._notify_registers(original_message, True)
                    if callback is not None:
                        if digital is not None:
                            callback(digital, self.registers["digitals"].check_write_value(digital))
                        else:
                            labeled_data = self._unpack(chunk_data[1:], self.WRITE_REGISTERS)
                            callback(labeled_data)
            else:
                # command was not successful, so determine what kind of error happened
                try:
                    error_command, error_code = chunk[0], chunk[1]
                except Exception as e:
                    raise RuntimeError(f"Dispenser: could not interpret error code.  {chunk}") from e

                original_command = original_message[0]
                if original_command == self.WRITE and error_command == self.WRITE_ERROR:
                    self._notify_registers(original_message, False, self.ERROR_CODES[error_code])
                    if error_callback is not None:
                        error_callback(self.ERROR_CODES[error_code], original_message)
                    print(f"Dispenser: write error successfully caught.")
                elif original_command == self.READ and error_command == self.READ_ERROR:
                    if error_callback is not None:
                        error_callback(self.ERROR_CODES[error_code], original_message)
                    print(f"Dispenser: read error successfully caught.")
                else:
                    raise RuntimeError(
                        f"Dispenser: caught wrong kind of error for command!  Error code is "
                        f"{error_code}, and original data is {original_message}"
                    )

    def _read_all(self):
        # Stub to consume the arg that the read_button.clicked event seems to be feeding to this function
        self.read()

    def read(self, start_register=0, count=None, callback=None, error_callback=None, digital=None):
        if self._server_socket is None:
            return
        self.defer_read = False
        if count is None:
            count = self.READ_REG_COUNT - start_register
        elif count > self.READ_REG_COUNT - start_register:
            raise ValueError(f"Dispenser.read: count of {count} too high for start register {start_register}.")

        data = f"{self.READ},{start_register},{count};".encode("utf-8")
        self._message_queue.put(((self.READ, start_register, count), callback, error_callback, digital))
        self._server_socket.write(data)

    def write(self, start_register=0, count=None, callback=None, error_callback=None, digital=None):
        if self._server_socket is None:
            return

        if count is None:
            count = self.READ_REG_COUNT - start_register
        elif count > self.READ_REG_COUNT - start_register:
            raise ValueError(f"Dispenser.read: count of {count} too high for start register {start_register}.")

        data = f"{self.WRITE},{start_register},{count}".encode("utf-8")
        for i in range(start_register, start_register + count):
            data += b"," + self._write_registers_by_index[i].to_bytes()
        data += b";"
        self._message_queue.put(((self.WRITE, start_register, count), callback, error_callback, digital))
        self._server_socket.write(data)

    def do_deferred_writes(self, *_, callback=None, error_callback=None, digital=None):
        """
        This function is connected to a push button slot, which causes it to receive a forced parameter it doesn't need,
        hence the *_
        """
        if self._server_socket is None:
            return

        self.defer_write = False
        if not np.any(self._deferred_writes):
            return
        indices_to_write = self.WRITE_INDICES[self._deferred_writes]
        start_register, stop_index = np.amin(indices_to_write), np.amax(indices_to_write)
        count = stop_index - start_register + 1

        self.write(start_register, count, callback, error_callback, digital)
        self._deferred_writes = np.zeros((self.WRITE_REG_COUNT,), dtype=bool)

    def _try_write_register(self, write_index, callback=None, error_callback=None, digital=None):
        if self._initialized and self._connection_ready:
            if self._ui.defer_write_checkbox.checkState():
                self._deferred_writes[write_index] = True
            else:
                self.write(write_index, 1, callback, error_callback, digital)

    def _notify_registers(self, original_message, success, cause=None):
        start_register, count = original_message[1], original_message[2]
        for i in range(start_register, start_register + count):
            if success:
                self._write_registers_by_index[i]._notify_success()
            else:
                self._write_registers_by_index[i]._notify_failure(cause)

    @staticmethod
    def _unpack(raw_data, registers):
        start_register, register_count = raw_data[:2]
        register_values = raw_data[2:]

        return {
            registers[i]: v for i, v in zip(range(start_register, start_register + register_count), register_values)
        }

    def _update_ui(self, data, original_message):
        start_register, count = original_message[1], original_message[2]
        for i, d in zip(range(start_register, start_register + count), data):
            self._read_registers_by_index[i]._raw_set_value(d, do_write=False)

        self._update_pressure_units()
        self._update_vacuum_units()

    def _update_pressure_units(self):
        r = self.registers["pressure"]
        r._label_units()

        # Adjust the limits on the acceptable pressure range
        pressure_units = self.registers["pressure_units"].value
        if self.registers["model_type"].value == "UltimusPlus-NX I":
            # Is a type I high-pressure dispenser
            if pressure_units == "psi":
                low, high = 10, 100
            elif pressure_units == "bar":
                low, high = .68, 6.89
            else:  # pressure_units == "kpa"
                low, high = 68.9, 689.4
        else:
            # Is a type II low-pressure dispenser
            if pressure_units == "psi":
                low, high = .3, 15
            elif pressure_units == "bar":
                low, high = .02, 1.03
            else:  # pressure_units == "kpa"
                low, high = 2.1, 103.4
        r._edit_box.validator().setRange(low, high, 8)
        r._text_changed()

    def _update_vacuum_units(self):
        r = self.registers["vacuum"]
        r._label_units()

        # Adjust the limits on the acceptable vacuum pressure range
        pressure_units = self.registers["vacuum_units"].value
        # Don't care about the dispenser type for this setting
        if pressure_units == "inches water":
            low, high = 0, 18
        elif pressure_units == "inches Hg":
            low, high = 0, 1.32
        else:  # pressure_units == "kpa"
            low, high = 0, 4.4
        r._edit_box.validator().setRange(low, high, 8)
        r._text_changed()

    @property
    def defer_read(self):
        return self._ui.defer_read_checkbox.checkState()

    @defer_read.setter
    def defer_read(self, val):
        self._ui.defer_read_checkbox.setChecked(val)

    @property
    def defer_write(self):
        return self._ui.defer_write_checkbox.checkState()

    @defer_write.setter
    def defer_write(self, val):
        self._ui.defer_write_checkbox.setChecked(val)
        if val:
            self._ui.do_defer_write_button.setEnabled(True)
        else:
            self._ui.do_defer_write_button.setEnabled(False)

    def _clicked_defer_write(self):
        if self._ui.defer_write_checkbox.checkState():
            self._ui.do_defer_write_button.setEnabled(True)
        else:
            self._ui.do_defer_write_button.setEnabled(False)

    def _set_show_registers(self):
        if self._ui.show_registers.checkState():
            self._ui.reg_widget.show()
        else:
            self._ui.reg_widget.hide()

    def set_system_time(self):
        if self._server_socket is None:
            return

        self.defer_write = True
        self.date_changed(qtc.QDate.currentDate())
        self.time_changed(qtc.QTime.currentTime())
        self.do_deferred_writes()

    def date_changed(self, new_date, do_register_update=True):
        """
        Change the date of the dispenser.  new_date must implement month, day, and year as methods, not just
        attributes.  A qtc.QTDate is the expected input.
        """
        if self._server_socket is None:
            return

        if do_register_update:
            self.registers["system_date"].value = new_date.month(), new_date.day(), new_date.year()
        else:
            self._ui.date_time_picker.blockSignals(True)
            self._ui.date_time_picker.setDate(new_date)
            self._ui.date_time_picker.blockSignals(False)

    def time_changed(self, new_time, do_register_update=True):
        """
        Change the time of the dispenser.  new_time must implement hour, minute, and second as methods, not just
        attributes.  A qtc.QTime is the expected input.
        """
        if self._server_socket is None:
            return

        if do_register_update:
            self.registers["system_time"].value = new_time.hour(), new_time.minute(), new_time.second()
        else:
            self._ui.date_time_picker.blockSignals(True)
            self._ui.date_time_picker.setTime(new_time)
            self._ui.date_time_picker.blockSignals(False)

    def __getattr__(self, key):
        return self.registers[key].value

    def __setattr__(self, key, value):
        if self._initialized and key in self._register_keys:
            self.registers[key].value = value
        else:
            super().__setattr__(key, value)

    def _run_test(self):
        if self._connection_ready:
            """register = "trigger"
            def f(name, value):
                print(f"test callback called, with name {name} and value {value}")

            self.set_digital(register, "pulse", f)"""
            # self.dispense_mode = "multishot"
            pass

    def get_digital(self, name):
        """
        Retrieves a digital flag from the read register, but does not issue a read command to the physical dispenser.
        """
        return self.registers["digitals"].read(name)

    def read_digital(self, name, callback, error_callback=None):
        """
        Issues a read command for a digital to the physical dispenser, and once it is received, fire a callback.  The
        callback will receive two parameters, the name of the digital, and its value.
        """
        self.read(0, 1, callback, error_callback, name)

    def set_digital(self, name, value, callback=None, error_callback=None):
        """
        Sets a digital flag in the write register.  This WILL issue a write command to the physical dispenser (unlike
        get_digital) though this function will respect the state of defer_write - by setting defer_write, multiple
        write operations can be queued together and send with only a single network operation.

        Many flags make more sense to set with a pulse than setting them to a constant value.  If value is a bool, the
        flag will be set to that value.  If value is the string "pulse", then the flag will be set high, and once it is
        acknowledged by the physical dispenser, will automatically set the flag back to low.

        The callback will receive two parameters, the name of the digital, and its value.
        """
        if value == "pulse":

            def pulse_callback(name, _, callback=callback):
                callback(name, True)
                self.set_digital(name, False)

            self.registers["digitals"].write(name, value, pulse_callback, error_callback)
        else:
            print(f"set digital called, with value {value}")
            self.registers["digitals"].write(name, value, callback, error_callback)

    def check_write_digital(self, name):
        """
        Check the state of a write digital.  From the local machine, since this value does not exist on the physical
        dispenser.
        """
        return bool(self.registers["digitals"]._write_value & self.registers["digitals"]._write_bits[name])

    @property
    def digitals_names(self):
        """
        Get the names of the digital register's flags, which are used to set them programmatically.
        """
        return {
            "read-only": tuple(self.registers["digitals"]._read_bits.keys()),
            "write-only": tuple(self.registers["digitals"]._write_bits.keys())
        }

    @property
    def polling_active(self):
        return self._polling_active

    @polling_active.setter
    def polling_active(self, val):
        self._polling_active = bool(val)
        self._try_toggle_polling()

    @property
    def polling_period(self):
        return self._polling_timer.interval()

    @polling_period.setter
    def polling_period(self, val):
        self._polling_timer.setInterval(val * 1000)

    def _try_toggle_polling(self):
        if self._polling_active and self._connection_ready:
            self._polling_timer.start()
        else:
            self._polling_timer.stop()


# ======================================================================================================================


class Register:
    """
    The value stored in _value is always an int, and always formatted to exactly match what the dispenser
    expects.  i.e. it is scaled appropriately.

    The setter/getter for the attribute value uses parse/format to convert the raw _value into a more
    user friendly format.

    The _raw_set_value's job is to update the UI element and send a write command to the dispenser, if applicable.
    """
    def __init__(self, master_widget, name, min, max, dtype, read_index, write_index=None, scale=1, unit_type=None):
        self._name = name
        self._read_index = read_index
        self._write_index = write_index
        self._scale = scale
        self._value = 0
        self._unit_type = unit_type
        self._master_widget = master_widget
        if dtype in {int, float}:
            self._dtype = dtype
        else:
            raise ValueError(f"Register {name}: dtype must be int or float.")
        self._validator = self._get_validator(min, max)
        self._label_base = self._name.replace('_', ' ').title()
        self._label = qtw.QLabel(self._label_base)
        self._edit_box = None

        self.write_successful = RegisterWriteSuccess()
        self.write_failed = RegisterWriteFailure()

    def format(self, value):
        return self._dtype(value / self._scale)

    def parse(self, value):
        if self._dtype is int:
            return int(value) * self._scale
        else:
            return round(float(value) * self._scale)

    def make_widget(self):
        widget = qtw.QWidget()
        layout = qtw.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        widget.setLayout(layout)

        layout.addWidget(self._label)
        # If the unit type is a string, might as well update it once now.  Will never need to be updated again
        if type(self._unit_type) is str:
            self._label_units()
        if self._write_index is None:
            self._edit_box = qtw.QLabel("")
        else:
            self._edit_box = qtw.QLineEdit("")
            if self._validator is not None:
                self._edit_box.setValidator(self._validator)
            self._edit_box.editingFinished.connect(self._editing_finished)
            self._edit_box.textChanged.connect(self._text_changed)
        layout.addWidget(self._edit_box)
        layout.addStretch()

        return widget

    def _get_validator(self, min, max):
        if self._dtype is int:
            return qtg.QIntValidator(min, max)
        else:  # dtype is float
            return qtg.QDoubleValidator(min, max, 8)

    @property
    def writable(self):
        return self._write_index is None

    def _raw_set_value(self, val, do_write=True):
        self._value = val
        if self._edit_box is not None:
            self._edit_box.setText(str(self.format(val)))
        if do_write and self._write_index is not None:
            self._master_widget._try_write_register(self._write_index)

    def _label_units(self):
        if self._unit_type is None:
            return
        if type(self._unit_type) is str:
            units = self._unit_type
        else:
            units = self._master_widget.registers[Dispenser.READ_REGISTERS[self._unit_type]].value
        self._label.setText(self._label_base + f" ({units})")

    def _editing_finished(self):
        self.value = self._edit_box.text()

    def _text_changed(self):
        if self._validator is None:
            return
        if self._validator.validate(self._edit_box.text(), 0)[0] == qtg.QValidator.Acceptable:
            self._edit_box.setStyleSheet("QLineEdit { background-color: white}")
        else:
            self._edit_box.setStyleSheet("QLineEdit { background-color: pink}")

    @property
    def value(self):
        return self.format(self._value)

    @value.setter
    def value(self, val):
        if self._edit_box is not None and self._validator is not None:
            if self._validator.validate(str(val), 0)[0] != qtg.QValidator.Acceptable:
                raise ValueError(f"Register {self._name} value {val} is out of bounds.")

        val = self.parse(val)
        if self._write_index is None:
            raise AttributeError(f"Register {self._name} cannot be written to.")
        else:
            self._raw_set_value(val)

    def to_bytes(self):
        return f"{self._value}".encode("utf-8")

    def _notify_success(self):
        self.write_successful.emit(self._name)

    def _notify_failure(self, cause):
        self.write_failed.emit((self._name, cause))


class DigitalRegister:
    INDICATOR_SIZE = 20

    def __init__(self, master_widget, name, read_index, write_index):
        self._name = name
        self._read_index = read_index
        self._write_index = write_index
        self._master_widget = master_widget
        self._read_value = 0x0
        self._write_value = 0x0

        self._read_bits = {
            "running": 0x1,
            "e_stop_active": 0x2,
            "sleeping": 0x4,
            "log_full": 0x8
        }
        self._read_indicators = {}
        self._write_bits = {
            "trigger": 0x0001,
            "e_stop": 0x0002,
            "sleep": 0x0004,
            "delete_log_1": 0x0008,
            "set_datetime": 0x0010,
            "update_program_params": 0x0020,
            "update_program_num": 0x0040,
            "update_units": 0x0080,
            "delete_log_2": 0x0800,
        }
        self._write_buttons = {}

        self.write_successful = RegisterWriteSuccess()
        self.write_failed = RegisterWriteFailure()

        # Signals for when the read bits change state
        self.running_low = FlagLow()
        self.running_high = FlagHigh()
        self.running_changed = FlagChanged()

        self.e_stop_active_low = FlagLow()
        self.e_stop_active_high = FlagHigh()
        self.e_stop_active_changed = FlagChanged()

        self.sleeping_low = FlagLow()
        self.sleeping_high = FlagHigh()
        self.sleeping_changed = FlagChanged()

        self.log_full_low = FlagLow()
        self.log_full_high = FlagHigh()
        self.log_full_changed = FlagChanged()

    def make_widget(self):
        widget = qtw.QWidget()
        layout = qtw.QGridLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        widget.setLayout(layout)

        read_row = 1
        layout.addWidget(qtw.QLabel("Read Bits"), 0, 3, 1, 2)
        for name, mask in self._read_bits.items():
            layout.addWidget(qtw.QLabel(name.replace('_', ' ').title()), read_row, 3, 1, 1)
            button = qtw.QPushButton("")
            button.setEnabled(False)
            button.setStyleSheet("QPushButton {background-color: gray}")
            button.setMinimumWidth(self.INDICATOR_SIZE)
            button.setMaximumWidth(self.INDICATOR_SIZE)
            button.setMaximumHeight(self.INDICATOR_SIZE)
            layout.addWidget(button, read_row, 4, 1, 1)
            self._read_indicators[name] = button
            read_row += 1

        write_row = 1
        layout.addWidget(qtw.QLabel("Write Bits"), 0, 0, 1, 2)
        for name, mask in self._write_bits.items():
            layout.addWidget(qtw.QLabel(name.replace('_', ' ').title()), write_row, 0, 1, 1)
            button = qtw.QPushButton("")
            button.setStyleSheet("QPushButton {background-color: gray}")
            button.setMinimumWidth(self.INDICATOR_SIZE)
            button.setMaximumWidth(self.INDICATOR_SIZE)
            button.setMaximumHeight(self.INDICATOR_SIZE)
            layout.addWidget(button, write_row, 1, 1, 1)
            self._write_buttons[name] = button
            write_row += 1

        # Hook callbacks up to each write button
        for name, mask in self._write_bits.items():
            def func(_, mask=mask, name=name):
                new_state = not bool(self._write_value & mask)
                if new_state:
                    self._write_value |= mask
                    self._write_buttons[name].setStyleSheet("QPushButton {background-color: green}")
                else:
                    self._write_value &= ~mask
                    self._write_buttons[name].setStyleSheet("QPushButton {background-color: gray}")
                self._master_widget._try_write_register(self._write_index)
            self._write_buttons[name].clicked.connect(func)

        max_rows = max(write_row, read_row)
        separator = qtw.QFrame()
        separator.setFrameShape(qtw.QFrame.VLine)
        separator.setFrameShadow(qtw.QFrame.Sunken)
        layout.addWidget(separator, 0, 2, max_rows, 1)

        return widget

    def _raw_set_value(self, val, do_write=False):
        """
        Called in _update_ui, this should set the read values.  We will never need to use the do_write argument,
        since the input and output interface are different for this register, and we will never have to worry about
        the infinite loop issue of updates to the UI causing updates to the value causing updates to the UI again.

        Unlike other registers, we do not want to send a write command from this function, because this class
        does not internally use this function - it is here just for compatibility.
        """
        changed_flags = self._read_value ^ val

        self._read_value = val
        for name, mask in self._read_bits.items():
            if self._read_value & mask:
                self._read_indicators[name].setStyleSheet("QPushButton {background-color: green}")
            else:
                self._read_indicators[name].setStyleSheet("QPushButton {background-color: gray}")

        # Fire off all the events when flags change
        mask = self._read_bits["running"]

        if changed_flags & mask:
            is_on = self._read_value & mask
            self.running_changed.emit(is_on)
            if is_on:
                self.running_high.emit()
            else:
                self.running_low.emit()

        mask = self._read_bits["e_stop_active"]
        if changed_flags & mask:
            is_on = self._read_value & mask
            self.e_stop_active_changed.emit(is_on)
            if is_on:
                self.e_stop_active_high.emit()
            else:
                self.e_stop_active_low.emit()

        mask = self._read_bits["sleeping"]
        if changed_flags & mask:
            is_on = self._read_value & mask
            self.sleeping_changed.emit(is_on)
            if is_on:
                self.sleeping_high.emit()
            else:
                self.sleeping_low.emit()

        mask = self._read_bits["log_full"]
        if changed_flags & mask:
            is_on = self._read_value & mask
            self.log_full_changed.emit(is_on)
            if is_on:
                self.log_full_high.emit()
            else:
                self.log_full_low.emit()

    def to_bytes(self):
        return f"{self._write_value}".encode("utf-8")

    def _notify_success(self):
        self.write_successful.emit(self._name)

    def _notify_failure(self, cause):
        self.write_failed.emit((self._name, cause))

    def set_enabled(self, state):
        for button in self._write_buttons.values():
            button.setEnabled(state)

    def read(self, name):
        return bool(self._read_value & self._read_bits[name])

    def check_write_value(self, name):
        return bool(self._write_value & self._write_bits[name])

    def write(self, name, value, callback=None, error_callback=None):
        if value:
            self._write_value |= self._write_bits[name]
            self._write_buttons[name].setStyleSheet("QPushButton {background-color: green}")
        else:
            self._write_value &= ~self._write_bits[name]
            self._write_buttons[name].setStyleSheet("QPushButton {background-color: gray}")
        self._master_widget._try_write_register(self._write_index, callback, error_callback, name)


class ModelTypeRegister(Register):
    @staticmethod
    def format(value):
        if value == 1:
            return "UltimusPlus-NX I"
        else:
            return "UltimusPlus-NX II"


class TimeFormatRegister(Register):
    @staticmethod
    def format(value):
        if value == 0:
            return "AM"
        elif value == 1:
            return "PM"
        else:
            return "24HR"


class DateRegister(Register):
    def make_widget(self):
        return None

    def parse(self, value):
        month, day, year = value
        return int(f"{year}{month:02d}{day:02d}")

    def format(self, value):
        s = f"{value:08d}"
        return int(s[:4]), int(s[4:6]), int(s[6:])  # year, month, day, as ints

    def _raw_set_value(self, val, do_write=True):
        self._value = val
        self._master_widget.date_changed(qtc.QDate(*self.format(val)), do_register_update=False)
        if do_write:
            self._master_widget._try_write_register(self._write_index)

    def to_bytes(self):
        return f"{self._value:08d}".encode("utf-8")


class TimeRegister(Register):
    def make_widget(self):
        return None

    def parse(self, value):
        hour, minute, second = value
        return int(f"{hour:02d}{minute:02d}{second:02d}")

    def format(self, value):
        s = f"{value:06d}"
        return int(s[:2]), int(s[2:4]), int(s[4:])

    def _raw_set_value(self, val, do_write=True):
        self._value = val
        self._master_widget.time_changed(qtc.QTime(*self.format(val)), do_register_update=False)
        if do_write:
            self._master_widget._try_write_register(self._write_index)

    def to_bytes(self):
        return f"{self._value:06d}".encode("utf-8")


class SelectableRegister(Register):
    def __init__(self, master_widget, name, choices, read_index, write_index, scale=1, selection_callback=None):
        self._choices = choices
        self._selector = None
        self._parser = {v: k for k, v in choices.items()}
        self._selection_callback = selection_callback
        keys = np.array(tuple(choices.keys()), dtype=np.int32)
        min_key, max_key = np.amin(keys), np.amax(keys)
        super().__init__(master_widget, name, min_key, max_key, int, read_index, write_index=write_index, scale=scale)

    def format(self, value):
        return self._choices[value]

    def parse(self, value):
        try:
            return self._parser[value]
        except KeyError as e:
            raise ValueError(f"SelectableRegister: {value} is not valid for this register") from e

    def make_widget(self):
        widget = qtw.QWidget()
        layout = qtw.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        widget.setLayout(layout)

        layout.addWidget(qtw.QLabel(self._name.replace('_', ' ').title()))
        self._selector = qtw.QComboBox()
        self._selector.addItems(self._choices.values())
        self._selector.currentTextChanged.connect(self._selection_changed)
        if self._selection_callback is not None:
            self._selector.currentTextChanged.connect(self._selection_callback)
        layout.addWidget(self._selector)
        layout.addWidget(self._selector)
        layout.addStretch()

        return widget

    def _selection_changed(self, text):
        self.value = text

    def _raw_set_value(self, val, do_write=True):
        self._value = val
        if self._selector is not None:
            self._selector.setCurrentText(self.format(val))
        if do_write and self._write_index is not None:
            self._master_widget._try_write_register(self._write_index)


class RegisterWriteSuccess(qtc.QObject):
    sig = qtc.pyqtSignal(str)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class RegisterWriteFailure(qtc.QObject):
    sig = qtc.pyqtSignal(tuple)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class FlagLow(qtc.QObject):
    sig = qtc.pyqtSignal()

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class FlagHigh(qtc.QObject):
    sig = qtc.pyqtSignal()

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class FlagChanged(qtc.QObject):
    sig = qtc.pyqtSignal(bool)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class ConnectionChanged(qtc.QObject):
    sig = qtc.pyqtSignal(bool)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)
