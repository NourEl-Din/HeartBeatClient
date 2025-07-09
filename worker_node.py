import requests
import time
from datetime import datetime
import sys

import random

class WorkerNode:
    def __init__(self, node_id, server_url='http://localhost:9000/', heartbeat_interval=0.5, simulate_unstable=False):
        self.node_id = node_id
        self.server_url = server_url
        self.heartbeat_interval = heartbeat_interval
        self.running = True
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.simulate_unstable = simulate_unstable

    def set_unstable(self, value: bool):
        self.simulate_unstable = value

    def _parse_interval(self, interval_value):
        # Accept float, int, or string like '1:0'
        if isinstance(interval_value, (float, int)):
            return float(interval_value)
        if isinstance(interval_value, str):
            if ':' in interval_value:
                parts = interval_value.split(':')
                try:
                    minutes = float(parts[0])
                    seconds = float(parts[1])
                    return minutes * 60 + seconds
                except Exception:
                    pass
            try:
                return float(interval_value)
            except Exception:
                pass
        return self.heartbeat_interval

    def send_heartbeat(self):
        if self.simulate_unstable:
            # 30% chance to simulate a network failure
            if random.random() < 0.3:
                print(f"[{self.node_id}] Simulated network failure!")
                return False, self.heartbeat_interval
            # 20% chance to add a random delay (1-5 seconds)
            if random.random() < 0.2:
                delay = random.uniform(1, 5)
                print(f"[{self.node_id}] Simulated network delay: {delay:.2f}s")
                time.sleep(delay)
        payload = {
            'NodeId': self.node_id,
        }
        try:
            response = requests.post(
                f"{self.server_url}/api/heartbeat",
                json=payload,
                headers=self.headers,
                timeout=3
            )
            print(f"[{self.node_id}] Server response: {response.status_code} - {response.text.strip()}")
            if response.status_code == 200:
                try:
                    data = response.json()
                    interval_raw = data.get('heartbeatInterval', self.heartbeat_interval)
                    interval = self._parse_interval(interval_raw)
                    return True, interval
                except Exception as e:
                    print(f"[{self.node_id}] Failed to parse response JSON: {e}")
                    return True, self.heartbeat_interval
            else:
                return False, self.heartbeat_interval
        except requests.exceptions.RequestException as e:
            print(f"[{self.node_id}] Heartbeat failed: {type(e).__name__}: {str(e)}")
            return False, self.heartbeat_interval

    def run(self):
        print(f"[{self.node_id}] Starting worker node (Initial heartbeat interval: {self.heartbeat_interval}s)")
        
        while self.running:
            success, interval = self.send_heartbeat()
            if not success:
                print(f"[{self.node_id}] Failed to send heartbeat")
            self.heartbeat_interval = interval
            time.sleep(self.heartbeat_interval)

if __name__ == '__main__':
    import os
    node_id = str(os.getppid())  # Use parent process (C# app) PID as NodeId
    interval = 0.5
    simulate_unstable = False
    for arg in sys.argv[1:]:
        if arg == '--unstable':
            simulate_unstable = True
        else:
            try:
                interval = float(arg)
            except ValueError:
                print(f"Warning: Unrecognized argument '{arg}' ignored.")

    worker = WorkerNode(node_id=node_id, heartbeat_interval=interval, simulate_unstable=simulate_unstable)
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.running = False
        print(f"[{node_id}] Worker node stopped")