import subprocess
import datetime

class WorkloadGenerator:
    def __init__(self, endpoint: str="5000", rate: int=1000, duration: str="120m") -> None:
        self.threads = 15
        self.connections = 45
        self.duration = duration
        self.script = "./wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        self.endpoint = f"http://127.0.0.1:{endpoint}"
        self.rate = rate
        self.command = f"cd ../hotelReservation && \
            ../wrk2/wrk -t {self.threads} -c {self.connections} -d {self.duration} \
                -L -s {self.script} {self.endpoint} -R {self.rate}"
    
    def generate_stationary(self):
        self.workload_process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(datetime.datetime.now(), f"[Workload] Generating with {self.rate} reqs/s for {self.duration}..")
        self.workload_process.wait()
        output, error = self.workload_process.communicate()
        if error.decode():
            print(datetime.datetime.now(), "[Workload] Error:", error.decode())
        else:
            print(datetime.datetime.now(), "[Workload] Done")


    def terminate(self):
        self.workload_process.terminate()
        output, error = self.workload_process.communicate()
        if error.decode():
            print(datetime.datetime.now(), "[Workload] Error:", error.decode())
        else:
            print(datetime.datetime.now(), "[Workload] Done:", output.decode())


    def generate_nonstationary(self, durations_with_rate: list):
        for duration, rate in durations_with_rate:
            self.command = f"cd ../hotelReservation && \
            ../wrk2/wrk -t {self.threads} -c {self.connections} -d {duration} \
                -L -s {self.script} {self.endpoint} -R {rate}"
            self.workload_process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(datetime.datetime.now(), f"[Workload] Generating with {rate} reqs/s for {duration}..")

            self.workload_process.wait()
            output, error = self.workload_process.communicate()
            if error.decode():
                print(datetime.datetime.now(), "[Workload] Error:", error.decode())
            else:
                print(datetime.datetime.now(), "[Workload] Done:", output.decode())
            
