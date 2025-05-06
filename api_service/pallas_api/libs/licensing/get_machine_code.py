import hashlib
import platform
from subprocess import Popen, PIPE


class Helpers:

    @staticmethod
    def get_SHA256(string):
        """
        Compute the SHA256 signature of a string.
        """
        return hashlib.sha256(string.encode("utf-8")).hexdigest()

    @staticmethod
    def start_process(command):

        process = Popen(command, stdout=PIPE)
        (output, err) = process.communicate()
        exit_code = process.wait()
        return output.decode("utf-8")

    @staticmethod
    def get_dbus_machine_id():
        try:
            with open("/etc/machine-id") as f:
                return f.read().strip()
        except:
            pass
        try:
            with open("/var/lib/dbus/machine-id") as f:
                return f.read().strip()
        except:
            pass
        return ""

    @staticmethod
    def get_inodes():
        import os
        files = ["/bin", "/etc", "/lib", "/root", "/sbin", "/usr", "/var"]
        inodes = []
        for file in files:
            try:
                inodes.append(os.stat(file).st_ino)
            except:
                pass
        return "".join([str(x) for x in inodes])

    def compute_machine_code(self):
        return self.get_dbus_machine_id() + self.get_inodes()

    def GetMachineCode(self):

        """
        Get a unique identifier for this device.
        """

        if "windows" in platform.platform().lower():
            return self.get_SHA256(
                self.start_process(["cmd.exe", "/C", "wmic", "csproduct", "get", "uuid"]))
        elif "mac" in platform.platform().lower() or "darwin" in platform.platform().lower():
            res = self.start_process(["system_profiler", "SPHardwareDataType"])
            return self.get_SHA256(res[res.index("UUID"):].strip())
        elif "linux" in platform.platform().lower():
            return self.get_SHA256(self.compute_machine_code())
        else:
            return self.get_SHA256(self.compute_machine_code())

    @staticmethod
    def IsOnRightMachine(license_key, is_floating_license=False, allow_overdraft=False):

        """
        Check if the device is registered with the license key.
        """

        current_mid = Helpers().GetMachineCode()

        if license_key.hwid is None:
            return False

        for act_machine in license_key.hwid:
            if current_mid == act_machine:
                return True

        return False


if __name__ == '__main__':
    a = Helpers().GetMachineCode()
    print(a)
