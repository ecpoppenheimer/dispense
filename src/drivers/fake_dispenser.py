import traceback

from twisted.internet import protocol, reactor, endpoints
import numpy as np


class FakePump(protocol.Protocol):
    # The register indices that will be received by write_command aren't actually the indices of the register but
    # the indices of the write_registers... I have to construct a map that excludes the read-only registers
    """
    Name                Readable        Writable    Index
    Digitals            Y               Y           0
    Model Type          Y               N           1
    Program NUM         Y               Y           2
    Dispense Mode       Y               Y           3
    Dispense Time       Y               Y           4
    Pressure            Y               Y           5
    Vacuum              Y               Y           6
    System Count        Y               N           7
    Shot Count          Y               N           8
    MultiShot Count     Y               Y           9
    MultiShot Time      Y               Y           10
    System Date         Y               Y           11
    System Time         Y               Y           12
    Software Version    Y               N           13
    Pressure Units      Y               Y           14
    Vacuum Units        Y               Y           15
    Time Format         Y               N           16
    """
    REG_WRITE_MAP = np.array((0, 2, 3, 4, 14, 5, 15, 6, 9, 10, 11, 12), dtype=np.int32)

    def dataReceived(self, raw_data):
        try:
            chunks = self.process_raw_data(raw_data)
            for data in chunks:
                # process command
                command = data[0]
                if command == 16:
                    self.write_command(data)
                elif command == 3:
                    self.read_command(data)
                else:
                    print(f"got unrecognized command {command}")

        except Exception:
            print(traceback.format_exc())
            self.transport.write(raw_data)

    def write_command(self, data):
        try:
            print(f"write command received {data}")
            start_register = data[1]
            register_count = data[2]
            for i, d in zip(range(start_register, start_register + register_count), data[3:]):
                if i == 0:
                    # Writing to the digitals register, which requires special consideration since I am packing both
                    # the readable digitals and the writable ones into the same int.
                    readable_registers = self.factory.registers[self.REG_WRITE_MAP[i]] & 0xf000
                    self.factory.registers[self.REG_WRITE_MAP[i]] = d | readable_registers
                else:
                    self.factory.registers[self.REG_WRITE_MAP[i]] = d

            # acknowledge the write operation back to the client
            repackaged = self.package_data(data)
            self.transport.write(repackaged)
        except ValueError:
            # There was a data format error
            self.transport.write(self.package_data([144, -1]))
        except IndexError:
            # There was a data limit error
            self.transport.write(self.package_data([144, -2]))

        # If the digital write register was set, update the digital read register
        if start_register == 0:
            self.factory.registers[0] = (self.factory.registers[0] & 0x0fff) + ((data[3] & 0xf) << 12)

    def read_command(self, data):
        try:
            start_register = data[1]
            register_count = data[2]
            read_data = self.factory.registers[start_register: start_register + register_count].copy()

            if start_register == 0:
                # Special consideration if reading the digitals register, since I am packing both the read and
                # write bits into a single int.
                read_data[0] = (read_data[0] & 0xf000) >> 12

            # acknowledge the write operation back to the client
            repackaged = self.package_data(np.concatenate((data, read_data)))
            self.transport.write(repackaged)
        except ValueError:
            # There was a data format error
            self.transport.write(self.package_data([131, -1]))
        except IndexError:
            # There was a data limit error
            self.transport.write(self.package_data([131, -2]))

    @staticmethod
    def process_raw_data(raw_data):
        raw_data = str(raw_data)[2:-2]
        chunks = []
        for chunk in raw_data.split(";"):
            chunks.append(np.array(tuple(int(s) for s in chunk.split(",")), dtype=np.int32))
        return chunks

    @staticmethod
    def package_data(data):
        s = ",".join(data.astype(str)) + ";"
        return s.encode("utf-8")

    def connectionMade(self):
        # print("just got a connection")
        pass

    def connectionLost(self, reason):
        # print(f"just lost a connection: {reason}")
        pass


class PumpFactory(protocol.Factory):
    protocol = FakePump

    def __init__(self):
        """
        registers are...
        Digitals
        Model Type
        Program NUM
        Dispense Mode
        Dispense Time
        Pressure
        Vacuum
        System Count
        Shot Count
        MultiShot Count
        MultiShot Time
        System Date
        System Time
        Software Version
        Pressure Units
        Vacuum Units
        Time Format
        """
        self.registers = np.array(
            [0x0000, 1, 1, 1, 1, 68, 0, 0, 0, 0, 10, 19011213, 0, 0, 0, 0, 0, 0, 0, 0],
            dtype=np.int32
        )  # First octal in first element is the 4 read-only bits of the digitals.


endpoints.serverFromString(reactor, "tcp:9000").listen(PumpFactory())
reactor.run()
