import requests
import time
from datetime import datetime
import sys

class WorkerNode:
    def __init__(self, node_id, server_url='http://localhost:9000/', heartbeat_interval=0.5):
        self.node_id = node_id
        self.server_url = server_url
        self.heartbeat_interval = heartbeat_interval
        self.running = True
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def send_heartbeat(self):
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
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"[{self.node_id}] Heartbeat failed: {type(e).__name__}: {str(e)}")
            return False

    def run(self):
        print(f"[{self.node_id}] Starting worker node (Heartbeat interval: {self.heartbeat_interval}s)")
        
        while self.running:
            success = self.send_heartbeat()
            if not success:
                print(f"[{self.node_id}] Failed to send heartbeat")
            
            time.sleep(self.heartbeat_interval)

if __name__ == '__main__':
    import uuid
    node_id = str(uuid.uuid4())[:8]
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5

    worker = WorkerNode(node_id=node_id, heartbeat_interval=interval)
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.running = False
        print(f"[{node_id}] Worker node stopped")