import queue
import time

import PyQt5.QtWidgets as qtw
import PyQt5.QtGui as qtg
import PyQt5.QtNetwork as qtn
import PyQt5.QtCore as qtc
import numpy as np

from epyqtwidgets.ip4validator import IP4Validator
import epyqtsettings.settings_widgets as sw
from epyqtsettings.settings import Settings
from epyqtwidgets.indicator import Indicator

STUPID_TIME_DELAY = .02


class Dispenser:
    """
    API for a Nordson UltimusPlus fluid dispenser.  Fully asynchronous.

    This class takes care of the networking tasks required to communicate with the dispenser.  It can be used headless
    but also provides method to construct PyQt5 Widgets that can be used to build a UI.

    The registers used by the dispenser are available as public attributes read_registers and write_registers,
    and they can be read from and written to with the public methods read(), write(), and pulse().  These operations are
    basic, and work on the raw, unformatted values used by the dispenser.  A more user-friendly interface is provided
    through read/write public properties.  These properties are formatted into more user-friendly values.

    Accessing these properties does not trigger a networking operation.  Their values are cached from the last time a
    read or poll command was sent to the dispenser.  You can ensure that the value is fresh by calling read() and
    passing a callback - if the callback is called then accessing readable registers will return fresh values.

    Setting a writable property does trigger a networking operation to update the value on the physical dispenser.
    Many of the write operations require one of the corresponding digital registers to be set alongside the numerical
    register value.  This process is handled automatically by the property, and means three individual networking
    operations will be executed for most property write calls.  Property writes do not provide assurance that the update
    was received and acknowledged by the physical dispenser.  If this is required, use the equivalent set method
    and provide a callback.  When the callback is triggered, the value has been guaranteed to have been accepted by
    the physical dispenser.  These methods may also take an error_callback which is called if the write operation fails
    for some reason.

    Read only boolean properties
        dispensing
        e_stop
        sleep
        log_full

    Write only boolean properties
        trigger
        e_stop
        sleep

    Read only properties
        model_type: str, {"UltimusPlus-NX "I", "UltimusPlus-NX II"}
        system_count: int
        shot_count: int
        software_version: float

    Read-write properties
        program_num: int {1-16}
        dispense_mode: int or str, {1-4} or {"Single Shot", "Steady Mode", "MultiShot"}
            3: "Teach Mode" is a valid read value, but invalid write value.
        dispense_time: float
        pressure_units: int or str, {0-2} or {"psi", "bar", "kPa"}
        pressure: float
        vacuum_units: int or str, {0-2} or {"in water", "in Hg", "kPa"}
        vacuum: float
        multishot_count: int
        multishot_time: float
        system_date: int, in format YYYYMMDD
        system_time: int, in format HHMMSS

    The write-only boolean registers are not settable properties, but callable methods that pulse the associated
    boolean register.  These methods accept callbacks that fire when the pulse has been acknowledged as received.
    These do not have a corresponding set_ method - they are the set method without set_ in the name.
    Write only boolean properties
        delete_log
        update_datetime
        update_params
        update_program_num
        update_units

    The ideal way to trigger depends on dispense mode.  For steady mode, setting and clearing trigger is the way
    to go.  In single shot or multi shot mode, It is better to use a pulse, for which a convenience function is provided
        trigger_shot

    Each readable register also has a PyQt signal that is fired whenever the register's value changes, which can
    be connected to any number of PyQt slots.  This signal's name is {register_name}_changed

    This class can be set to automatically poll the physical dispenser and update the read register values.  This
    behavior can be enabled via the poll property, and the polling period can be set by the polling_period property.
    These both have _changed signals that fire with the new value whenever one of these properties is changed.

    There is a connection_event that is fired whenever the connection state to the physical dispenser changes, and
    receives one of four values: Dispenser.CONNECTED = 1, Dispenser.CONNECTING = 2, Dispenser.DISCONNECTED = 3,
    or Dispenser.CONNECTION_ERROR = 4

    """

    _initialized = False
    DEFAULT_PORT = 9000

    # Commands to send to the dispenser.
    READ = 3
    WRITE = 16

    # Actions that can be fed to the queue
    POLL = 1
    PULSE = 2

    COMMAND_LUT = {
        READ: "read",
        WRITE: "write",
        POLL: "poll",
        PULSE: "pulse"
    }

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
    READ_ONLY_REGISTERS = ["model_type", "system_count", "shot_count", "software_version"]

    READ_INDICES = np.arange(READ_REG_COUNT)
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
    DIGITAL_WRITE_REGISTERS = {
        "trigger": 0x0001,
        "e_stop": 0x0002,
        "sleep": 0x0004,
        "delete_log": 0x0808,
        "update_datetime": 0x0010,
        "update_params": 0x0020,
        "update_program_num": 0x0040,
        "update_units": 0x0080,
    }
    MAP_WRITE_TO_READ = [0, 2, 3, 4, 14, 5, 15, 6, 9, 10, 11, 12]

    # Connection state
    CONNECTED = 1
    CONNECTING = 2
    DISCONNECTED = 3
    CONNECTION_ERROR = 4

    def __init__(self, settings=None, ip=None, port=None, autoconnect=True):
        self._data_stream = b""
        if ip is None:
            ip = "localhost"
        if port is None:
            port = self.DEFAULT_PORT

        # settings is optional.  Will make a temporary one if None was specified.
        if settings is None:
            self.settings = Settings()
            self.settings.establish_defaults(
                dispenser_port=port, dispenser_ip=ip, polling_active=True, polling_period=1.0, save_settings=False
            )
        else:
            self.settings = settings
            self.settings.establish_defaults(
                dispenser_port=port, dispenser_ip=ip, polling_active=True, polling_period=1.0, save_settings=True
            )

        self.read_registers = -1 * np.ones((self.READ_REG_COUNT,), dtype=np.int64)
        self.write_registers = np.zeros((self.WRITE_REG_COUNT,), dtype=np.int64)

        # Read register changed events
        self.digitals_changed = IntChanged()
        self.dispensing_changed = FlagChanged()
        self.e_stop_active_changed = FlagChanged()
        self.sleeping_changed = FlagChanged()
        self.log_full_changed = FlagChanged()
        self.model_type_changed = StrChanged()
        self.program_num_changed = IntChanged()
        self.dispense_mode_changed = StrChanged()
        self.dispense_time_changed = FloatChanged()
        self.pressure_changed = FloatChanged()
        self.vacuum_changed = FloatChanged()
        self.system_count_changed = IntChanged()
        self.shot_count_changed = IntChanged()
        self.multishot_count_changed = IntChanged()
        self.multishot_time_changed = FloatChanged()
        self.system_date_changed = IntChanged()
        self.system_time_changed = IntChanged()
        self.software_version_changed = IntChanged()
        self.pressure_units_changed = StrChanged()
        self.vacuum_units_changed = StrChanged()
        self.time_format_changed = StrChanged()

        self.connection_event = ConnectionChanged()
        self._connection_state = self.DISCONNECTED

        self._server_socket = None
        self._debug = False
        self._polling_timer = qtc.QTimer()
        self._polling_period = self.settings.polling_period
        self._polling_timer.timeout.connect(self._do_poll)
        self._polling_active = False
        self._poll_queued = False
        self.poll_changed = FlagChanged()
        self.poll_period_changed = FloatChanged()
        self.poll = self.settings.polling_active

        """
        self.registers = {}
        self.registers.update({
            "digitals": DigitalRegister(self, "digitals", 0, 0),
            "model_type": ModelTypeRegister(self, "model_type", 1, 2, int, 1),
            "system_count": Register(self, "system_count", 0, 2147483647, int, 7),
            "shot_count": Register(self, "shot_count", 0, 2147483647, int, 8),
            "software_version": Register(self, "software_version", 0, 2147483647, int, 13, scale=1000),
            "time_format": TimeFormatRegister(self, "time_format", 0, 2, int, 16),
            "program_num": Register(
                self, "program_num", 1, 16, int, 2, write_index=1,
                digital_update_register=DigitalRegister._write_bits["update_program_num"]
            ),
            "system_date": DateRegister(self, "system_date", 19011213, 20380119, int, 11, write_index=10),
            "system_time": TimeRegister(self, "system_time", 0, 235959, int, 12, write_index=11),
            "dispense_mode": SelectableRegister(
                self, "dispense_mode", {
                    1: "single_shot", 2: "steady_mode", 3: "teach_mode", 4: "multishot"
                }, 3, 2,
                digital_update_register=DigitalRegister._write_bits["update_program_params"]
            ),
            "dispense_time": Register(
                self, "dispense_time", .0001, 9999, float, 4, 3, scale=10000, unit_type="s",
                digital_update_register=DigitalRegister._write_bits["update_program_params"]
            ),
            "pressure_units": SelectableRegister(
                self, "pressure_units", {
                    0: "psi", 1: "bar", 2: "kpa"
                }, 14, 4, selection_callback=self._update_pressure_units,
                digital_update_register=DigitalRegister._write_bits["update_units"]
            ),
            "pressure": Register(
                self, "pressure", .68, 689.4, float, 5, 5, scale=100, unit_type=14,
                digital_update_register=DigitalRegister._write_bits["update_program_params"]
            ),
            "vacuum_units": SelectableRegister(
                self, "vacuum_units", {
                    0: "inches_water", 1: "inches_Hg", 2: "kpa"
                }, 15, 6, selection_callback=self._update_vacuum_units,
                digital_update_register=DigitalRegister._write_bits["update_units"]
            ),
            "vacuum": Register(
                self, "vacuum", 0, 18.0, float, 6, 7, scale=100, unit_type=15,
                digital_update_register=DigitalRegister._write_bits["update_program_params"]
            ),
            "multishot_count": Register(
                self, "multishot_count", 0, 9999, int, 9, 8,
                digital_update_register=DigitalRegister._write_bits["update_program_params"]
            ),
            "multishot_time": Register(
                self, "multishot_time", .1, 999.9, float, 10, 9, scale=100, unit_type="s",
                digital_update_register=DigitalRegister._write_bits["update_program_params"]
            ),
        })
        self._write_registers_by_index = [self.registers[name] for name in self.WRITE_REGISTERS]
        self._read_registers_by_index = [self.registers[name] for name in self.READ_REGISTERS]
        self._register_keys = set(self.registers.keys())
        self._deferred_writes = np.zeros((self.WRITE_REG_COUNT,), dtype=bool)"""

        # Message queue.  Messages should be added here as a tuple of (data, callback), and the callback will
        # be fired when the message is acknowledged.
        self._action_queue = queue.Queue()
        self._pending_action = None
        if autoconnect:
            self.connect()

    def connect(self, address=None, port=None):
        if address is None:
            address = self.settings.dispenser_ip
        if port is None:
            port = self.settings.dispenser_port
        self._server_socket = qtn.QTcpSocket()
        self._server_socket.connectToHost(address, port)
        self._server_socket.connected.connect(self._got_connection)

        self._set_connection_state(self.CONNECTING)

    def disconnect(self, _error=False):
        if self._server_socket is not None:
            self._server_socket.close()
        self._server_socket = None
        if _error:
            self._set_connection_state(self.CONNECTION_ERROR)
        else:
            self._set_connection_state(self.DISCONNECTED)

    def shut_down(self):
        self.disconnect()

    def _got_connection(self):
        if self._server_socket is not None:
            self._server_socket.disconnected.connect(self.disconnect)
            self._server_socket.errorOccurred.connect(self._socket_error)
            self._server_socket.readyRead.connect(self._read_chunks)
            self._set_connection_state(self.CONNECTED)

            # Things to do when a new connection is initialized
            self.get_datetime()
            self.read()

    def _socket_error(self, _error):
        self.disconnect(_error=True)
        print(f"received socket error: {_error}")
        # TODO crashes after reaching this point, without a traceback, only an exit code

    def make_connection_widget(self, alignment="vertical"):
        return DispenserConnectionWidget(self, alignment)

    def make_polling_widget(self):
        return DispenserPollingWidget(self)

    def make_read_only_widget(self, registers=None):
        if registers is None:
            registers = self.READ_ONLY_REGISTERS
        elif registers in self.READ_ONLY_REGISTERS:
            return ReadOnlyWidget(self, registers)
        elif set(registers).issubset(self.READ_ONLY_REGISTERS):
            pass
        else:
            raise ValueError(
                "Dispenser make_read_only_widget: registers must be a single string register, a subset of Dispenser."
                "READ_ONLY_REGISTERS, or None."
            )

        w = qtw.QWidget()
        layout = qtw.QVBoxLayout()
        w.setLayout(layout)
        for r in registers:
            layout.addWidget(ReadOnlyWidget(self, r))
        return w

    def make_date_time_widget(self):
        return DateTimeWidget(self)

    def make_params_widget(self):
        return ParamsWidget(self)

    def make_digitals_widget(self, show_log=False, horizontal=True):
        return DigitalsWidget(self, show_log, horizontal)

    def make_trigger_widget(self):
        return TriggerWidget(self)

    @property
    def connection_state(self):
        return self._connection_state

    def _set_connection_state(self, state):
        self._connection_state = state
        self.connection_event.emit(state)

    def _read_chunks(self):
        """
        This function parses the data stream to get data from the dispenser.  This handles both reading data
        via read commands, and acknowledging write commands.  The read result is its own acknowledgement.

        This function also consumes elements from the action queue.  Only one command should ever be sent to the
        dispenser at once, although many may be queued in the action queue.  This is so we can guarantee acknowledging
        each requested action, and route everything to the correct place.  The while loop inside this function SHOULD
        only ever trigger once, as there should only ever be a single message per call.

        This function will send additional messages to the dispenser if there is more than one element in the action
        queue.
        """
        if self._server_socket is None:
            return

        # I am keeping the code to read in multiple chunks at a time for robustness, even though the action queue
        # should prevent more than one message from ever being queued up.
        self._data_stream += self._server_socket.read(self._server_socket.bytesAvailable())
        delimiter_pos = self._data_stream.find(b';')
        while delimiter_pos > 0:
            chunk = self._data_stream[:delimiter_pos]
            chunk_data = np.asarray(chunk.split(b','))
            chunk_data =tuple(int(b.decode("utf-8")) for b in chunk_data)
            self._data_stream = self._data_stream[delimiter_pos + 1:]
            delimiter_pos = self._data_stream.find(b';')

            # This data chunk should correspond to the first item in the action queue
            if self._pending_action is None:
                print(f"Dispenser: Warning, ignored an unexpected chunk of data (no action was queued) {chunk_data}")
            else:
                action = self._pending_action[0]
                if action == self.READ:
                    self._acknowledge_read(chunk_data)
                elif action == self.POLL:
                    self._poll_queued = False
                    self._acknowledge_read(chunk_data)
                elif action == self.WRITE or action == self.PULSE:
                    self._acknowledge_write(chunk_data, action == self.PULSE)
                    if action == self.PULSE:
                        time.sleep(STUPID_TIME_DELAY)
                else:
                    raise RuntimeError(
                        f"Dispenser: Got an invalid action {action} from the action queue."
                    )
            self._pending_action = None

        # Finished acknowledging the pending action.  If there are any actions in the action queue, now is the time
        # to execute a new one.
        if not self._action_queue.empty():
            self._execute_action()

    def _acknowledge_read(self, chunk_data):
        command, callback, error_callback, extra_args = self._pending_action
        if chunk_data[0] == self.READ:
            start_register, count = extra_args
            if chunk_data[1] == start_register and chunk_data[2] == count:
                new_register_values = chunk_data[3:]
                for i, new_register_value in zip(range(start_register, start_register + count), new_register_values):
                    # If and only if the register has changed, update its value and then fire the corresponding
                    # register_changed event.  Use the register property getter to format the value.
                    if i == 0:
                        # Special case for the digitals register, since we need to be able to fire each individual
                        # flag changed event.
                        self._emit_digital_changes(new_register_value, self.read_registers[0])
                    if self.read_registers[i] != new_register_value:
                        # Do this for all registers, including the digitals register.
                        self.read_registers[i] = new_register_value
                        register_name = self.READ_REGISTERS[i]
                        getattr(self, register_name + "_changed").emit(getattr(self, register_name))

                # Read was sucessful!
                if callback is not None:
                    try:
                        callback()
                    except Exception as e:
                        print(f"Dispenser: Error firing callback on read: {chunk_data}")
                        print(e)
            else:
                print(f"Dispenser: Acknowledging read command but the registers do not match - out of order?")
                return
        else:
            print(
                f"Dispenser: Error: expected to receive a read acknowledgement, but instead received Chunk data: "
                f"{chunk_data}, args: {extra_args}"
            )
            if error_callback is not None:
                try:
                    error_callback()
                except Exception as e:
                    print(f"Dispenser: Error firing error callback on read: {chunk_data}")
                    print(e)

    def _emit_digital_changes(self, new_value, old_value):
        new_dig = new_value
        dig_diff = old_value ^ new_dig
        if dig_diff & 0x1:
            self.dispensing_changed.emit(new_dig & 0x1)
        if dig_diff & 0x2:
            self.e_stop_active_changed.emit(new_dig & 0x2)
        if dig_diff & 0x4:
            self.sleeping_changed.emit(new_dig & 0x4)
        if dig_diff & 0x8:
            self.log_full_changed.emit(new_dig & 0x8)

    def _acknowledge_write(self, chunk_data, is_pulse):
        command, callback, error_callback, extra_args = self._pending_action
        if chunk_data[0] == self.WRITE:
            if is_pulse:
                start_register, count = 0, 1
            else:
                start_register, count = extra_args
            if chunk_data[1] == start_register and chunk_data[2] == count:
                # Need to emit the changed events.  Need to determine whether the write has changed the value relative
                # to the last read.  While this could be done by issuing a new read request, or automatic polling, I
                # would really like to actually use the information in the write request, even though that complicates
                # things a bit.  Need to map between read register indices and write register indices, since they
                # do not line up.
                new_register_values = chunk_data[3:]
                for write_index, new_register_value in zip(
                    range(start_register, start_register + count), new_register_values
                ):
                    read_index = self.MAP_WRITE_TO_READ[write_index]

                    # Special case for the digitals
                    if read_index == 0:
                        self._emit_digital_changes(new_register_value, self.read_registers[read_index])

                    if new_register_value != self.read_registers[read_index]:
                        self.read_registers[read_index] = new_register_value
                        register_name = self.READ_REGISTERS[read_index]
                        getattr(self, register_name + "_changed").emit(getattr(self, register_name))

                # Write was successful!
                if callback is not None:
                    try:
                        callback()
                    except Exception as e:
                        print(f"Dispenser: Error firing callback on write: {chunk_data}")
                        print(e)
                return

        print(f"Dispenser: Error acknowledging write.  Chunk data: {chunk_data}, args: {extra_args}")
        if error_callback is not None:
            try:
                error_callback()
            except Exception as e:
                print(f"Dispenser: Error firing error_callback on write: {chunk_data}")
                print(e)

    def _prepare_action(self, action, callback, error_callback, extra_args):
        """
        This function adds an action to the action queue.  It should be called by everything that wants to write to
        the dispenser EXCEPT for _read_chunks, which may execute actions directly if multiple are queued at once.

        If the action queue is empty, this function will send it immediately after enqueueing it.
        """
        if self._action_queue.empty() and self._pending_action is None:
            send_now = True
        else:
            send_now = False

        self._action_queue.put((action, callback, error_callback, extra_args))
        if send_now:
            self._execute_action()

    def _execute_action(self):
        """
        This function encodes the logic for sending messages to the dispenser, but should never be called directly,
        except by _read_chunks and _prepare_action, which are both responsible for checking whether the action may
        be executed.
        Use _prepare_action to put an action in the queue and eventually execute it.
        """
        self._pending_action = self._action_queue.get()
        if self._pending_action[0] == self.POLL and not self._action_queue.empty():
            # polling actions are always pushed to the back of the queue.  At least... unless there are multiple
            # floating around.  If there are two polls in a row, one will get executed here, but that is good - it will
            # ensure all polls eventually get consumed, in case more than one is ever queued by accident.
            self._action_queue.put(self._pending_action)
            self._pending_action = self._action_queue.get()

        # Execute the action
        action, callback, error_callback, extra_args = self._pending_action
        if action == self.READ or action == self.POLL:
            start_register, count = extra_args
            data = f"{self.READ},{start_register},{count};".encode("utf-8")
            self._server_socket.write(data)
        elif action == self.WRITE:
            start_register, count = extra_args
            data = f"{self.WRITE},{start_register},{count}".encode("utf-8")
            for i in range(start_register, start_register + count):
                data += f",{self.write_registers[i]}".encode("utf-8")
            data += b";"
            self._server_socket.write(data)
        elif action == self.PULSE:
            mask, level = extra_args
            digitals = int(self.write_registers[0])
            if level:
                # pulsing on
                digitals |= mask
            else:
                # pulsing off
                digitals &= ~mask
            data = f"{self.WRITE},0,1,{digitals};".encode("utf-8")
            self._server_socket.write(data)

    def read(self, start_register=0, count=None, callback=None, error_callback=None):
        """
        Public but low-level function for reading from the dispenser into the read_registers.
        """
        if self._server_socket is None:
            return
        if count is None:
            count = self.READ_REG_COUNT - start_register
        elif count > self.READ_REG_COUNT - start_register:
            raise ValueError(f"Dispenser.read: count of {count} too high for start register {start_register}.")
        if start_register < 0 or start_register > self.READ_REG_COUNT:
            raise ValueError(
                f"Dispenser.read: start_register {start_register} out of range (0, {self.READ_REG_COUNT})."
            )

        self._prepare_action(self.READ, callback, error_callback, (start_register, count))

    def write(self, start_register=0, count=None, callback=None, error_callback=None):
        """
        Public but low-level for sending the values in write registers to the dispenser.

        This function will not pulse the digital flags that are required to set certain registers.  That must be done
        manually when using this function.  Or just use the writable attributes to feed values to the dispenser.
        """
        if self._server_socket is None:
            return
        if count is None:
            count = self.READ_REG_COUNT - start_register
        elif count > self.READ_REG_COUNT - start_register:
            raise ValueError(f"Dispenser.write: count of {count} too high for start register {start_register}.")
        if start_register < 0 or start_register > self.READ_REG_COUNT:
            raise ValueError(
                f"Dispenser.write: start_register {start_register} out of range (0, {self.READ_REG_COUNT})."
            )

        self._prepare_action(self.WRITE, callback, error_callback, (start_register, count))

    def pulse(self, register, callback=None, error_callback=None):
        self._prepare_action(self.PULSE, None, error_callback, (self.DIGITAL_WRITE_REGISTERS[register], True))
        self._prepare_action(self.PULSE, callback, error_callback, (self.DIGITAL_WRITE_REGISTERS[register], False))

    def get_datetime(self):
        date = qtc.QDate.currentDate()
        time = qtc.QTime.currentTime()
        self.system_date = f"{date.year():04}{date.month():02}{date.day():02}"
        self.system_time = f"{time.hour():02}{time.minute():02}{time.second():02}"

    @property
    def poll(self):
        return self._polling_active

    @poll.setter
    def poll(self, val):
        val = bool(val)
        if val == self._polling_active:
            return

        self.settings._polling_active = val
        self._polling_active = val
        self.poll_changed.emit(val)
        if self._polling_active:
            self._polling_timer.start()
        else:
            self._polling_timer.stop()

    def _do_poll(self):
        if self._connection_state == self.CONNECTED:
            if not self._poll_queued:
                self._prepare_action(self.POLL, None, None, (0, self.READ_REG_COUNT))
                self._poll_queued = True

    @property
    def polling_period(self):
        return self._polling_timer.interval() / 1000

    @polling_period.setter
    def polling_period(self, val):
        v = int(val * 1000)
        if v != self._polling_timer.interval():
            self._polling_timer.setInterval(v)
            self.settings.polling_period = val
            self.poll_period_changed.emit(val)

    @property
    def digitals(self):
        return self.read_registers[0]

    @digitals.setter
    def digitals(self, val):
        self.write_registers[0] = val
        self._prepare_action(self.WRITE, None, None, (0, 1))

    @property
    def dispensing(self):
        return bool(self.read_registers[0] & 0x1)

    def trigger_shot(self, callback=None, error_callback=None):
        self.pulse("trigger", callback, error_callback)

    @property
    def trigger(self):
        return bool(self.read_registers[0] & 0x1)

    @trigger.setter
    def trigger(self, val):
        mask = self.DIGITAL_WRITE_REGISTERS["trigger"]
        if val:
            # turn on
            self.write_registers[0] |= mask
        else:
            # turn off
            self.write_registers[0] &= ~mask
        self._prepare_action(self.WRITE, None, None, (0, 1))

    @property
    def e_stop(self):
        return bool(self.read_registers[0] & 0x2)

    @e_stop.setter
    def e_stop(self, val):
        mask = self.DIGITAL_WRITE_REGISTERS["e_stop"]
        if val:
            # turn on
            self.write_registers[0] |= mask
        else:
            # turn off
            self.write_registers[0] &= ~mask
        self._prepare_action(self.WRITE, None, None, (0, 1))

    @property
    def sleep(self):
        return bool(self.read_registers[0] & 0x4)

    @sleep.setter
    def sleep(self, val):
        mask = self.DIGITAL_WRITE_REGISTERS["sleep"]
        if val:
            # turn on
            self.write_registers[0] |= mask
        else:
            # turn off
            self.write_registers[0] &= ~mask
        self._prepare_action(self.WRITE, None, None, (0, 1))

    @property
    def log_full(self):
        return bool(self.read_registers[0] & 0x8)

    def delete_log(self, callback=None, error_callback=None):
        self.pulse("delete_log", callback, error_callback)

    def update_datetime(self, callback, error_callback):
        self.pulse("update_datetime", callback, error_callback)

    def update_params(self, callback, error_callback):
        self.pulse("update_params", callback, error_callback)

    def update_program_num(self, callback, error_callback):
        self.pulse("update_program_num", callback, error_callback)

    def update_units(self, callback, error_callback):
        self.pulse("update_units", callback, error_callback)

    @property
    def model_type(self):
        v = self.read_registers[1]
        if v == 1:
            return "UltimusPlus-NX I"
        elif v == 2:
            return "UltimusPlus-NX II"
        else:
            return "Unknown Model Type"

    @property
    def program_num(self):
        return self.read_registers[2]

    @staticmethod
    def _format_program_num(val):
        if 1 <= val <= 16:
            return val
        else:
            raise ValueError(f"Dispenser: program num {val} out of range (1, 16)")

    @program_num.setter
    def program_num(self, val):
        if self.write_registers[1] != self._format_program_num(val):
            self.write_registers[1] = self._format_program_num(val)
            self._prepare_action(self.WRITE, None, None, (1, 1))
            self.pulse("update_program_num")

    def set_program_num(self, val, callback, error_callback=None):
        self.write_registers[1] = self._format_program_num(val)
        self._prepare_action(self.WRITE, None, error_callback, (1, 1))
        self.pulse("update_program_num", callback, error_callback)

    @property
    def dispense_mode(self):
        v = self.read_registers[3]
        if v == 1:
            return "Single Shot"
        elif v == 2:
            return "Steady Mode"
        elif v == 3:
            return "Teach Mode"
        elif v == 4:
            return "MultiShot"
        else:
            return "Unknown Dispense Mode"

    @staticmethod
    def _format_dispense_mode(val):
        if type(val) is int:
            if val in {1, 2, 4}:
                return val
            else:
                raise ValueError(f"Dispenser: Invalid value {val} for dispense mode.")
        else:
            if val == "Single Shot":
                return 1
            elif val == "Steady Mode":
                return 2
            elif val == "MultiShot":
                return 4
            else:
                raise ValueError(f"Dispenser: Invalid value {val} for dispense mode.")

    @dispense_mode.setter
    def dispense_mode(self, val):
        self.write_registers[2] = self._format_dispense_mode(val)
        self._prepare_action(self.WRITE, None, None, (2, 1))
        self.pulse("update_params")

    def set_dispense_mode(self, val, callback, error_callback=None):
        self.write_registers[2] = self._format_dispense_mode(val)
        self._prepare_action(self.WRITE, None, error_callback, (2, 1))
        self.pulse("update_params", callback, error_callback)

    @property
    def dispense_time(self):
        return self.read_registers[4] / 10000

    @staticmethod
    def _format_dispense_time(val):
        return val * 10000

    @dispense_time.setter
    def dispense_time(self, val):
        self.write_registers[3] = self._format_dispense_time(val)
        self._prepare_action(self.WRITE, None, None, (3, 1))
        self.pulse("update_params")

    def set_dispense_time(self, val, callback, error_callback=None):
        self.write_registers[3] = self._format_dispense_time(val)
        self._prepare_action(self.WRITE, None, error_callback, (3, 1))
        self.pulse("update_params", callback, error_callback)

    @property
    def pressure(self):
        return self.read_registers[5] / 100

    def _format_pressure(self, val):
        low, high = self.get_pressure_lims()
        if low <= val <= high:
            return val * 100
        else:
            raise ValueError(f"Dispenser: Pressure {val} out of range for unit: ({low}, {high}) {self.pressure_units}.")

    def get_pressure_lims(self, units=None):
        if units is None:
            units = self.pressure_units
        if self.model_type == "UltimusPlus-NX II":
            if units == "psi":
                return .3, 15.0
            elif units == "bar":
                return .02, 1.03
            elif units == "kPa":
                return 2.1, 103.4
        else:  # self.model_type == "UltimusPlus-NX I":
            if units == "psi":
                return 10.0, 100.0
            elif units == "bar":
                return .68, 6.89
            elif units == "kPa":
                return 68.9, 689.4

    @pressure.setter
    def pressure(self, val):
        self.write_registers[5] = self._format_pressure(val)
        self._prepare_action(self.WRITE, None, None, (5, 1))
        self.pulse("update_params")

    def set_pressure(self, val, callback, error_callback=None):
        self.write_registers[5] = self._format_pressure(val)
        self._prepare_action(self.WRITE, None, error_callback, (5, 1))
        self.pulse("update_params", callback, error_callback)

    @property
    def vacuum(self):
        return self.read_registers[6] / 100

    def _format_vacuum(self, val):
        low, high = self.get_vacuum_lims()
        if low <= val <= high:
            return val * 100
        else:
            raise ValueError(f"Dispenser: Vacuum {val} out of range for unit: ({low}, {high}) {self.vacuum_units}.")

    def get_vacuum_lims(self, units=None):
        if units is None:
            units = self.vacuum_units
        if units == "in water":
            return 0.0, 18.0
        elif units == "in Hg":
            return 0.0, 1.03
        elif units == "kPa":
            return 0.0, 4.4

    @vacuum.setter
    def vacuum(self, val):
        self.write_registers[7] = self._format_vacuum(val)
        self._prepare_action(self.WRITE, None, None, (7, 1))
        self.pulse("update_params")

    def set_vacuum(self, val, callback, error_callback=None):
        self.write_registers[7] = self._format_vacuum(val)
        self._prepare_action(self.WRITE, None, error_callback, (7, 1))
        self.pulse("update_params", callback, error_callback)

    @property
    def system_count(self):
        return self.read_registers[7]

    @property
    def shot_count(self):
        return self.read_registers[8]

    @property
    def multishot_count(self):
        return self.read_registers[9]

    @staticmethod
    def _format_multishot_count(val):
        return val

    @multishot_count.setter
    def multishot_count(self, val):
        self.write_registers[8] = self._format_multishot_count(val)
        self._prepare_action(self.WRITE, None, None, (8, 1))
        self.pulse("update_params")

    def set_multishot_count(self, val, callback, error_callback=None):
        self.write_registers[8] = self._format_multishot_count(val)
        self._prepare_action(self.WRITE, None, error_callback, (8, 1))
        self.pulse("update_params", callback, error_callback)

    @property
    def multishot_time(self):
        return self.read_registers[10] / 100

    @staticmethod
    def _format_multishot_time(val):
        return val * 100

    @multishot_time.setter
    def multishot_time(self, val):
        self.write_registers[9] = self._format_multishot_time(val)
        self._prepare_action(self.WRITE, None, None, (9, 1))
        self.pulse("update_params")

    def set_multishot_time(self, val, callback, error_callback=None):
        self.write_registers[9] = self._format_multishot_time(val)
        self._prepare_action(self.WRITE, None, error_callback, (9, 1))
        self.pulse("update_params", callback, error_callback)

    @property
    def system_date(self):
        return self.read_registers[11]

    @staticmethod
    def _format_system_date(val):
        return val

    @system_date.setter
    def system_date(self, val):
        self.write_registers[10] = self._format_system_date(val)
        self._prepare_action(self.WRITE, None, None, (10, 1))
        self.pulse("update_datetime")

    def set_system_date(self, val, callback, error_callback=None):
        self.write_registers[10] = self._format_system_date(val)
        self._prepare_action(self.WRITE, None, error_callback, (10, 1))
        self.pulse("update_datetime", callback, error_callback)

    @property
    def system_time(self):
        return self.read_registers[12]

    @staticmethod
    def _format_system_time(val):
        return val

    @system_time.setter
    def system_time(self, val):
        self.write_registers[11] = self._format_system_time(val)
        self._prepare_action(self.WRITE, None, None, (11, 1))
        self.pulse("update_datetime")

    def set_system_time(self, val, callback, error_callback=None):
        self.write_registers[11] = self._format_system_time(val)
        self._prepare_action(self.WRITE, None, error_callback, (11, 1))
        self.pulse("update_datetime", callback, error_callback)

    @property
    def software_version(self):
        return self.read_registers[13] / 1000

    @property
    def pressure_units(self):
        v = self.read_registers[14]
        if v == 0:
            return "psi"
        elif v == 1:
            return "bar"
        elif v == 2:
            return "kPa"
        else:
            return "Unknown Unit"

    @staticmethod
    def _format_pressure_units(val):
        if type(val) is int:
            if 0 <= val <= 2:
                return val
            else:
                raise ValueError(f"Dispenser: Invalid value {val} for pressure units.")
        else:
            if val == "psi":
                return 0
            elif val == "bar":
                return 1
            elif val == "kPa":
                return 2
            else:
                raise ValueError(f"Dispenser: Invalid value {val} for pressure units.")

    @pressure_units.setter
    def pressure_units(self, val):
        self.write_registers[4] = self._format_pressure_units(val)
        self._prepare_action(self.WRITE, None, None, (4, 1))
        self.pulse("update_units")

    def set_pressure_units(self, val, callback, error_callback=None):
        self.write_registers[4] = self._format_pressure_units(val)
        self._prepare_action(self.WRITE, None, error_callback, (4, 1))
        self.pulse("update_units", callback, error_callback)
        self.read(5, 1)

    @property
    def vacuum_units(self):
        v = self.read_registers[15]
        if v == 0:
            return "in water"
        elif v == 1:
            return "in Hg"
        elif v == 2:
            return "kPa"
        else:
            return "Unknown Unit"

    @staticmethod
    def _format_vacuum_units(val):
        if type(val) is int:
            if 0 <= val <= 2:
                return val
            else:
                raise ValueError(f"Dispenser: Invalid value {val} for vacuum units.")
        else:
            if val == "in water":
                return 0
            elif val == "in Hg":
                return 1
            elif val == "kPa":
                return 2
            else:
                raise ValueError(f"Dispenser: Invalid value {val} for vacuum units.")

    @vacuum_units.setter
    def vacuum_units(self, val):
        self.write_registers[6] = self._format_vacuum_units(val)
        self._prepare_action(self.WRITE, None, None, (6, 1))
        self.pulse("update_units")

    def set_vacuum_units(self, val, callback, error_callback=None):
        self.write_registers[6] = self._format_vacuum_units(val)
        self._prepare_action(self.WRITE, None, error_callback, (6, 1))
        self.pulse("update_units", callback, error_callback)

    @property
    def time_format(self):
        v = self.read_registers[16]
        if v == 0:
            return "AM"
        elif v == 1:
            return "PM"
        elif v == 2:
            return "24 Hour"
        else:
            return "Unknown Time Format"


# ======================================================================================================================


class DispenserConnectionWidget(qtw.QWidget):
    def __init__(self, dispenser, alignment="vertical", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.connection_event.connect(self._connection_changed)

        # IP controls
        self._ip_entry = sw.SettingsEntryBox(
            self.dispenser.settings, "dispenser_ip", str, validator=IP4Validator(), label="Dispenser IP"
        )
        self._ip_entry.edit_box.textChanged.connect(self._ip_changed)

        self._port_entry = sw.SettingsEntryBox(
            self.dispenser.settings, "dispenser_port", int, validator=qtg.QIntValidator(0, 65535),
            label="Dispenser Port"
        )

        self._connect_button = qtw.QPushButton("Connect")
        self._connect_button.clicked.connect(self._click_connect)

        self._connection_indicator = Indicator("gray")

        if alignment == "vertical":
            layout = qtw.QGridLayout()
            layout.setContentsMargins(0, 0, 0, 0)
            self.setLayout(layout)
            ui_row = 0

            layout.addWidget(self._ip_entry, ui_row, 0, 1, 12)
            ui_row += 1
            layout.addWidget(self._port_entry, ui_row, 0, 1, 12)
            ui_row += 1
            layout.addWidget(self._connect_button, ui_row, 0, 1, 4)
            layout.addWidget(self._connection_indicator, ui_row, 5, 1, 1)
        else:  # alignment == "horizontal"
            layout = qtw.QHBoxLayout()
            layout.setContentsMargins(0, 0, 0, 0)
            self.setLayout(layout)

            layout.addWidget(self._ip_entry)
            layout.addWidget(self._port_entry)
            layout.addWidget(self._connect_button)
            layout.addWidget(self._connection_indicator)
            layout.addStretch()

    def _connection_changed(self, state):
        if state == Dispenser.CONNECTED:
            self._connect_button.setText("Disconnect.")
            self._connection_indicator.set_color("green")
        elif state == Dispenser.CONNECTING:
            self._connect_button.setText("Connecting...")
            self._connection_indicator.set_color("yellow")
        elif state == Dispenser.DISCONNECTED:
            self._connect_button.setText("Connect")
            self._connection_indicator.set_color("gray")
        elif state == Dispenser.CONNECTION_ERROR:
            self._connect_button.setText("Connect")
            self._connection_indicator.set_color("red")

    def _click_connect(self):
        if self.dispenser.connection_state == Dispenser.DISCONNECTED:
            self.dispenser.connect()
        else:
            self.dispenser.disconnect()

    def _ip_changed(self):
        state = self._ip_entry.edit_box.validator().validate(self._ip_entry.edit_box.text(), 0)[0]
        if state == qtg.QValidator.Acceptable:
            color = "white"
        else:
            color = "pink"
        self._ip_entry.setStyleSheet("QLineEdit { background-color: %s }" % color)


class DispenserPollingWidget(qtw.QWidget):
    def __init__(self, dispenser, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.poll_changed.connect(self._poll_remotely_changed)
        self.dispenser.poll_period_changed.connect(self._poll_period_remotely_changed)
        self.dispenser.connection_event.connect(self._respond_to_connection_event)

        layout = qtw.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        self.poll_active_entry = sw.SettingsCheckBox(
            self.dispenser.settings, "polling_active", "Automatic Polling", callback=self._poll_clicked
        )
        layout.addWidget(self.poll_active_entry)

        self.poll_period_entry = sw.SettingsEntryBox(
            self.dispenser.settings, "polling_period", float, qtg.QDoubleValidator(1e-2, 1e3, 3),
            callback=self._period_changed
        )
        layout.addWidget(self.poll_period_entry)

        read_button = qtw.QPushButton("Read Now")
        read_button.clicked.connect(self._read)
        layout.addWidget(read_button)

        self._respond_to_connection_event(dispenser.connection_state)

    def _poll_clicked(self, val):
        self.dispenser.poll = val

    def _poll_remotely_changed(self, val):
        self.poll_active_entry.set_value(val)

    def _respond_to_connection_event(self, state):
        if state == Dispenser.CONNECTED:
            self.show()
        else:
            self.hide()

    def _period_changed(self):
        self.dispenser.polling_period = self.dispenser.settings.polling_period

    def _poll_period_remotely_changed(self, val):
        self.poll_period_entry.set_value(val)

    def _read(self):
        self.dispenser.read()


class ReadOnlyWidget(qtw.QWidget):
    def __init__(self, dispenser, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.connection_event.connect(self._respond_to_connection_event)

        layout = qtw.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        layout.addWidget(qtw.QLabel(name.replace("_", " ").title()))
        self._label = qtw.QLabel("")
        layout.addWidget(self._label)
        changed_signal = getattr(self.dispenser, name + "_changed")
        changed_signal.connect(lambda x: self._label.setText(str(x)))

    def _respond_to_connection_event(self, state):
        if state == Dispenser.CONNECTED:
            self.show()
        else:
            self.hide()


class DateTimeWidget(qtw.QDateTimeEdit):
    def __init__(self, dispenser, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.connection_event.connect(self._respond_to_connection_event)

        self.setCalendarPopup(True)
        self.setMinimumDate(qtc.QDate(1901, 12, 13))
        self.setMaximumDate(qtc.QDate(2038, 1, 19))
        self.dateChanged.connect(self._date_changed)
        self.timeChanged.connect(self._time_changed)

        self.dispenser.system_date_changed.connect(self._update_date)
        self.dispenser.system_time_changed.connect(self._update_time)

    def _date_changed(self):
        date = self.date()
        self.dispenser.system_date = 10000 * date.year() + 100 * date.month() + date.day()

    def _time_changed(self):
        time = self.time()
        self.dispenser.system_time = 10000 * time.hour() + 100 * time.minute() + time.second()

    def _update_date(self, val):
        d = val
        day = d % 100
        d = d // 100
        month = d % 100
        d = d // 100
        year = d
        self.blockSignals(True)
        self.setDate(qtc.QDate(year, month, day))
        self.blockSignals(False)

    def _update_time(self, val):
        t = val
        second = t % 100
        t = t // 100
        minute = t % 100
        t = t // 100
        hour = t
        self.blockSignals(True)
        self.setTime(qtc.QTime(hour, minute, second))
        self.blockSignals(False)

    def _respond_to_connection_event(self, state):
        if state == Dispenser.CONNECTED:
            self.show()
        else:
            self.hide()


class ParamsWidget(qtw.QWidget):
    def __init__(self, dispenser, *args, **kwargs):
        # TODO there is some wierd, rare crosstalk between the pressure / vacuum units and the vacuum / pressure.
        # I can't figure it out though, and it is minor enough that I am going to ignore for now and move on.
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.connection_event.connect(self._respond_to_connection_event)
        self._old_pressure_units = None
        self._old_vacuum_units = None

        layout = qtw.QGridLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)
        ui_row = 0

        self._program_num_label = qtw.QLabel("Program number")
        layout.addWidget(self._program_num_label, ui_row, 0, 1, 1)
        self._program_num = qtw.QLineEdit("1")
        self._program_num.setValidator(qtg.QIntValidator(1, 16))
        self._program_num.editingFinished.connect(self._program_num_changed)
        self.dispenser.program_num_changed.connect(self._update_program_num)
        layout.addWidget(self._program_num, ui_row, 1, 1, 3)
        ui_row += 1

        self._dispense_mode_label = qtw.QLabel("Dispense mode")
        layout.addWidget(self._dispense_mode_label, ui_row, 0, 1, 1)
        self._dispense_mode = qtw.QComboBox()
        self._dispense_mode.addItems(("Single Shot", "Steady Mode", "MultiShot"))
        self._dispense_mode.currentTextChanged.connect(self._dispense_mode_changed)
        self.dispenser.dispense_mode_changed.connect(self._update_dispense_mode)
        layout.addWidget(self._dispense_mode, ui_row, 1, 1, 3)
        ui_row += 1

        self._dispense_time_label = qtw.QLabel("Dispense time")
        layout.addWidget(self._dispense_time_label, ui_row, 0, 1, 1)
        self._dispense_time = qtw.QLineEdit("")
        self._dispense_time.setValidator(qtg.QDoubleValidator(.0001, 9999.0, 4))
        self._dispense_time.editingFinished.connect(self._dispense_time_changed)
        self.dispenser.dispense_time_changed.connect(self._update_dispense_time)
        layout.addWidget(self._dispense_time, ui_row, 1, 1, 3)
        ui_row += 1

        self._pressure_label = qtw.QLabel("Pressure")
        layout.addWidget(self._pressure_label, ui_row, 0, 1, 1)
        self._pressure = qtw.QLineEdit("")
        self._pressure.setValidator(qtg.QDoubleValidator(0, 10000, 2))
        self._pressure.editingFinished.connect(self._pressure_changed)
        self.dispenser.pressure_changed.connect(self._update_pressure)
        layout.addWidget(self._pressure, ui_row, 1, 1, 2)

        self._pressure_units = qtw.QComboBox()
        self._pressure_units.addItems(("psi", "bar", "kPa"))
        self._pressure_units.currentTextChanged.connect(self._pressure_units_changed)
        self.dispenser.pressure_units_changed.connect(self._update_pressure_units)
        layout.addWidget(self._pressure_units, ui_row, 3, 1, 1)
        ui_row += 1

        self._vacuum_label = qtw.QLabel("Vacuum")
        layout.addWidget(self._vacuum_label, ui_row, 0, 1, 1)
        self._vacuum = qtw.QLineEdit("")
        self._vacuum.setValidator(qtg.QDoubleValidator(0, 10000, 2))
        self._vacuum.editingFinished.connect(self._vacuum_changed)
        self.dispenser.vacuum_changed.connect(self._update_vacuum)
        layout.addWidget(self._vacuum, ui_row, 1, 1, 2)

        self._vacuum_units = qtw.QComboBox()
        self._vacuum_units.addItems(("in water", "in Hg", "kPa"))
        self._vacuum_units.currentTextChanged.connect(self._vacuum_units_changed)
        self.dispenser.vacuum_units_changed.connect(self._update_vacuum_units)
        layout.addWidget(self._vacuum_units, ui_row, 3, 1, 1)
        ui_row += 1
        
        self._multishot_count_label = qtw.QLabel("MultiShot Count")
        layout.addWidget(self._multishot_count_label, ui_row, 0, 1, 1)
        self._multishot_count = qtw.QLineEdit("")
        self._multishot_count.setValidator(qtg.QIntValidator(0, 9999))
        self._multishot_count.editingFinished.connect(self._multishot_count_changed)
        self.dispenser.multishot_count_changed.connect(self._update_multishot_count)
        layout.addWidget(self._multishot_count, ui_row, 1, 1, 3)
        ui_row += 1

        self._multishot_time_label = qtw.QLabel("MultiShot Time")
        layout.addWidget(self._multishot_time_label, ui_row, 0, 1, 1)
        self._multishot_time = qtw.QLineEdit("")
        self._multishot_time.setValidator(qtg.QDoubleValidator(0.1, 999.9, 1))
        self._multishot_time.editingFinished.connect(self._multishot_time_changed)
        self.dispenser.multishot_time_changed.connect(self._update_multishot_time)
        layout.addWidget(self._multishot_time, ui_row, 1, 1, 3)
        ui_row += 1

    def _program_num_changed(self):
        self.dispenser.program_num = int(self._program_num.text())

    def _update_program_num(self, val):
        self._program_num.blockSignals(True)
        self._program_num.setText(str(val))
        self._program_num.blockSignals(False)

    def _dispense_mode_changed(self, new_mode):
        self.dispenser.dispense_mode = new_mode
        self._dispense_mode_changed_side_effects(new_mode)

    def _dispense_mode_changed_side_effects(self, new_mode):
        if new_mode == "Steady Mode":
            self._dispense_time.hide()
            self._dispense_time_label.hide()
            self._multishot_count_label.hide()
            self._multishot_count.hide()
            self._multishot_time_label.hide()
            self._multishot_time.hide()
        elif new_mode == "Single Shot":
            self._dispense_time.show()
            self._dispense_time_label.show()
            self._multishot_count_label.hide()
            self._multishot_count.hide()
            self._multishot_time_label.hide()
            self._multishot_time.hide()
        else:  # new_mode == "Single Shot"
            self._dispense_time.show()
            self._dispense_time_label.show()
            self._multishot_count_label.show()
            self._multishot_count.show()
            self._multishot_time_label.show()
            self._multishot_time.show()

    def _respond_to_connection_event(self, state):
        if state == Dispenser.CONNECTED:
            self.show()
        else:
            self.hide()

    def _update_dispense_mode(self, val):
        self._dispense_mode.blockSignals(True)
        self._dispense_mode.setCurrentText(val)
        self._dispense_mode.blockSignals(False)
        self._dispense_mode_changed_side_effects(val)

    def _dispense_time_changed(self):
        self.dispenser.dispense_time = float(self._dispense_time.text())

    def _update_dispense_time(self, val):
        self._dispense_time.blockSignals(True)
        self._dispense_time.setText(str(val))
        self._dispense_time.blockSignals(False)

    def _pressure_units_changed(self, new_units):
        self.dispenser.pressure_units = new_units
        self._pressure_units_changed_side_effects(new_units)

    def _pressure_units_changed_side_effects(self, new_units):
        if self._old_pressure_units is None:
            self._old_pressure_units = new_units
        low, high = self.dispenser.get_pressure_lims(new_units)
        self._pressure.validator().setRange(low, high, 2)
        # Let the dispenser perform the unit conversion, which can be accomplished by just calling read.  This will
        # also ensure that the limits get updated correctly.
        self.dispenser.read()

    def _update_pressure_units(self, val):
        self._pressure_units.blockSignals(True)
        self._pressure_units.setCurrentText(val)
        self._pressure_units.blockSignals(False)
        self._pressure_units_changed_side_effects(val)

    def _pressure_changed(self):
        self.dispenser.pressure = float(self._pressure.text())

    def _update_pressure(self, val):
        self._pressure.blockSignals(True)
        self._pressure.setText(str(val))
        self._pressure.blockSignals(False)

    def _vacuum_units_changed(self, new_units):
        self.dispenser.vacuum_units = new_units
        self._vacuum_units_changed_side_effects(new_units)

    def _vacuum_units_changed_side_effects(self, new_units):
        if self._old_vacuum_units is None:
            self._old_vacuum_units = new_units
        low, high = self.dispenser.get_vacuum_lims(new_units)
        self._vacuum.validator().setRange(low, high, 2)
        # Let the dispenser perform the unit conversion, which can be accomplished by just calling read.  This will
        # also ensure that the limits get updated correctly.
        self.dispenser.read()

    def _update_vacuum_units(self, val):
        self._vacuum_units.blockSignals(True)
        self._vacuum_units.setCurrentText(val)
        self._vacuum_units.blockSignals(False)
        self._vacuum_units_changed_side_effects(val)

    def _vacuum_changed(self):
        self.dispenser.vacuum = float(self._vacuum.text())

    def _update_vacuum(self, val):
        self._vacuum.blockSignals(True)
        self._vacuum.setText(str(val))
        self._vacuum.blockSignals(False)

    def _multishot_count_changed(self):
        self.dispenser.multishot_count = int(self._multishot_count.text())

    def _update_multishot_count(self, val):
        self._multishot_count.blockSignals(True)
        self._multishot_count.setText(str(val))
        self._multishot_count.blockSignals(False)

    def _multishot_time_changed(self):
        self.dispenser.multishot_time = int(self._multishot_time.text())

    def _update_multishot_time(self, val):
        self._multishot_time.blockSignals(True)
        self._multishot_time.setText(str(val))
        self._multishot_time.blockSignals(False)


class DigitalsWidget(qtw.QWidget):
    def __init__(self, dispenser, show_log=False, horizontal=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.connection_event.connect(self._respond_to_connection_event)

        if horizontal:
            layout = qtw.QHBoxLayout()
        else:
            layout = qtw.QVBoxLayout()
        self.setLayout(layout)

        self._sleep_button = qtw.QCheckBox("Sleep")
        self._sleep_button.setTristate(False)
        self._sleep_button.clicked.connect(self._toggle_sleep)
        self.dispenser.sleeping_changed.connect(self._update_sleeping)
        layout.addWidget(self._sleep_button)

        self._e_stop_button = qtw.QCheckBox("E Stop")
        self._e_stop_button.setTristate(False)
        self._e_stop_button.clicked.connect(self._toggle_e_stop)
        self.dispenser.e_stop_active_changed.connect(self._update_e_stop)
        layout.addWidget(self._e_stop_button)

        self._clear_log = qtw.QPushButton("Delete Log")
        self._clear_log.clicked.connect(lambda x: self.dispenser.delete_log())
        layout.addWidget(self._clear_log)

        if horizontal:
            layout.addStretch()

        self.show_log = show_log

    def _toggle_sleep(self):
        self.dispenser.sleep = not self.dispenser.sleep

    def _update_sleeping(self, val):
        self._sleep_button.blockSignals(True)
        if val:
            s = 2
        else:
            s = 0
        self._sleep_button.setCheckState(s)
        self._sleep_button.blockSignals(False)

    def _toggle_e_stop(self):
        self.dispenser.e_stop = not self.dispenser.e_stop

    def _update_e_stop(self, val):
        self._e_stop_button.blockSignals(True)
        if val:
            s = 2
        else:
            s = 0
        self._e_stop_button.setCheckState(s)
        self._e_stop_button.blockSignals(False)

    def _respond_to_connection_event(self, state):
        if state == Dispenser.CONNECTED:
            self.show()
        else:
            self.hide()

    @property
    def show_log(self):
        return self._show_log

    @show_log.setter
    def show_log(self, val):
        self._show_log = val
        if val:
            self._clear_log.show()
        else:
            self._clear_log.hide()


class TriggerWidget(qtw.QWidget):
    def __init__(self, dispenser, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispenser = dispenser
        self.dispenser.connection_event.connect(self._respond_to_connection_event)
        self.dispenser.dispense_mode_changed.connect(self._respond_to_mode_change)

        layout = qtw.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        self._trigger_shot_button = qtw.QPushButton("Trigger Shot")
        self._trigger_shot_button.clicked.connect(lambda x: self.dispenser.trigger_shot())
        layout.addWidget(self._trigger_shot_button)

        self._trigger_steady_button = qtw.QPushButton("Trigger Start")
        self._trigger_steady_button.clicked.connect(self._trigger_steady)
        layout.addWidget(self._trigger_steady_button)

        self._status = Indicator("gray")
        self.dispenser.dispensing_changed.connect(self._set_status)
        layout.addWidget(self._status)

        layout.addStretch()

    def _set_status(self, state):
        if state:
            self._status.set_color("blue")
            self._trigger_steady_button.setText("Trigger Stop")
        else:
            self._status.set_color("gray")
            self._trigger_steady_button.setText("Trigger Start")

    def _trigger_steady(self):
        self.dispenser.trigger = not self.dispenser.trigger

    def _respond_to_connection_event(self, state):
        if state == Dispenser.CONNECTED:
            self.show()
        else:
            self.hide()

    def _respond_to_mode_change(self, new_mode):
        if new_mode == "Steady Mode":
            self._trigger_shot_button.hide()
            self._trigger_steady_button.show()
        else:
            self._trigger_shot_button.show()
            self._trigger_steady_button.hide()


# ======================================================================================================================


class FlagChanged(qtc.QObject):
    sig = qtc.pyqtSignal(bool)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class ConnectionChanged(qtc.QObject):
    sig = qtc.pyqtSignal(int)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class FloatChanged(qtc.QObject):
    sig = qtc.pyqtSignal(float)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class IntChanged(qtc.QObject):
    sig = qtc.pyqtSignal(int)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)


class StrChanged(qtc.QObject):
    sig = qtc.pyqtSignal(str)

    def connect(self, value):
        self.sig.connect(value)

    def emit(self, *args):
        self.sig.emit(*args)
