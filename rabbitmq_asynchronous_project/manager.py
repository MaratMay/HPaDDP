import pika
import json
import time
import threading
import sys
import signal

class Manager:
    def __init__(self, my_id, n_workers, n_managers):
        self.my_id = my_id
        self.tmp_n_workers = n_workers
        self.n_workers = n_workers
        self.n_managers = n_managers

        self.command = None
        self.load_type = None

        self.state = "KING" if my_id == n_managers - 1 else 'REPLICA' #KING or REPLICA
        self.king_id = n_managers - 1

        self.timeout_duration = 4.0
        self.election_timeout_duration = 5
        self.heartbeat_frequency = 1
        self.heartbeat_check_frequency = 1

        def graceful_shutdown(signum, frame):
            print(f"Менеджер {self.my_id}: получен SIGTERM, закрываю соединение...")
            self.rmq_connection.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, graceful_shutdown)

        self.thread_connection = None
        self.thread_channel = None
        self.manager_broadcast_queue = None
        threading.Thread(target=self.init_thread, daemon=True).start()

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()

        self.rmq_channel.exchange_declare(exchange='yell_at_manager', exchange_type='fanout')
        result = self.rmq_channel.queue_declare(queue='', exclusive=True)
        self.manager_queue = result.method.queue
        self.rmq_channel.queue_bind(exchange='yell_at_manager', queue=self.manager_queue)

        self.rmq_channel.queue_declare(queue='from_manager_to_client', durable=True)

        self.rmq_channel.queue_declare(queue='from_worker_to_manager', durable=True)

        self.rmq_channel.exchange_declare(exchange='yell_at_workers', exchange_type='fanout')

        if self.state == 'KING':
            self.n_workers = n_workers * 2
            self.wait_for_workers() #wait for response from each worker
            self.n_workers = n_workers

        print(f"Я manager №{self.my_id} ! Я запустился!")
        self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body='')

    def init_thread(self):
        self.thread_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.thread_channel = self.thread_connection.channel()

        self.thread_channel.exchange_declare(exchange='manager_broadcast', exchange_type='fanout')
        result = self.thread_channel.queue_declare(queue='', exclusive=True)
        self.manager_broadcast_queue = result.method.queue
        self.thread_channel.queue_bind(exchange='manager_broadcast', queue=self.manager_broadcast_queue)

        time.sleep(1)

        if self.state == 'KING':
            self.heartbeat_loop()
        else:
            self.watchdog_loop()

    def heartbeat_loop(self):
        while self.state == "KING":
            try:
                msg = json.dumps({"purpose": "HEARTBEAT", "id": self.my_id})
                self.thread_channel.basic_publish(exchange='manager_broadcast', routing_key='', body=msg)
                time.sleep(self.heartbeat_frequency)
            except Exception as e:
                print(f"Ошибка в heartbeat_loop: {e}")
                break

    def watchdog_loop(self):
        last_seen = time.time()

        def on_broadcast(ch, method, props, body):
            nonlocal last_seen
            try:
                msg = json.loads(body.decode())
            except Exception:
                return

            purpose = msg.get("purpose")
            sender_id = msg.get("id")

            if purpose == "HEARTBEAT" and sender_id == self.king_id:
                last_seen = time.time()

            elif purpose == 'ELECTION':
                if sender_id <= self.my_id:
                    self.thread_channel.basic_publish(exchange='manager_broadcast', routing_key='', body=json.dumps({"purpose": "OK", "id": self.my_id}))
                    self.run_election()
                else:
                    self.state = 'WAIT_FOR_COORDINATOR'

            elif purpose == 'OK' and self.my_id < sender_id:
                self.state = 'WAIT_FOR_COORDINATOR'

            elif purpose == 'COORDINATOR' and sender_id != self.king_id:
                print(f"Менеджер {self.my_id}: да здравствует новый король {sender_id}")
                self.king_id = msg["id"]
                self.state = "REPLICA"
                last_seen = time.time()

        self.thread_channel.basic_consume(queue=self.manager_broadcast_queue, on_message_callback=on_broadcast, auto_ack=True)

        while True:
            self.thread_connection.process_data_events(time_limit=self.heartbeat_check_frequency)
            if time.time() - last_seen > self.timeout_duration and self.state == 'REPLICA':
                self.run_election()

    def run_election(self):
        if self.state != 'REPLICA':
            return

        self.state = 'ELECTION'

        self.thread_channel.basic_publish(exchange='manager_broadcast', routing_key='', body=json.dumps({"purpose": "ELECTION", "id": self.my_id}))

        deadline = time.time() + self.election_timeout_duration

        while time.time() < deadline:
            self.thread_connection.process_data_events(time_limit=0.5)

        if self.state == 'ELECTION':
            print(f"Менеджер {self.my_id}: никого старше нет, я новый KING!")
            self.state = 'KING'
            self.king_id = self.my_id
            self.thread_channel.basic_publish(exchange='manager_broadcast', routing_key='', body=json.dumps({"purpose": "COORDINATOR", "id": self.my_id}))
            self.heartbeat_loop()
        else:
            self.watchdog_loop()

    def wait_for_workers(self):
        def semaphore(ch, method, properties, body):
            self.tmp_n_workers -= 1
            if self.tmp_n_workers == 0:
                self.rmq_channel.stop_consuming()

        self.tmp_n_workers = self.n_workers
        self.rmq_channel.basic_consume(queue='from_worker_to_manager', auto_ack=True, on_message_callback=semaphore)
        self.rmq_channel.start_consuming()

    def get_work_from_client(self):
        def wait_from_client(ch, method, properties, body):
            self.command = body.decode("utf-8")
            self.rmq_channel.stop_consuming()

        self.rmq_channel.basic_consume(queue=self.manager_queue, auto_ack=True, on_message_callback=wait_from_client)
        self.rmq_channel.start_consuming()

    def analyze_find_results(self):
        def wait_from_worker(ch, method, properties, body):
            self.text = body.decode("utf-8")
            if self.text != '':
                self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body=self.text)
            self.tmp_n_workers -= 1
            if self.tmp_n_workers == 0:
                self.rmq_channel.stop_consuming()

        self.tmp_n_workers = self.n_workers
        self.rmq_channel.basic_consume(queue='from_worker_to_manager', auto_ack=True, on_message_callback=wait_from_worker)
        self.rmq_channel.start_consuming()
        self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body="That's all")

    def run(self):
        while True:
            self.get_work_from_client()

            parts = self.command.split()
            cmd = parts[0].lower()

            if self.state != 'KING':
                continue

            self.rmq_channel.basic_publish(exchange='yell_at_workers', routing_key='', body=self.command)

            if cmd == "find":
                self.analyze_find_results()
            else:
                self.wait_for_workers()
                self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body='')

if __name__ == "__main__":
    manager = Manager(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
    manager.run()