import pika
import sys
import json
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

        self.timeout_duration = 5
        self.election_timeout_duration = 10

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()

        self.rmq_channel.exchange_declare(exchange='yell_at_manager', exchange_type='fanout')
        result = self.rmq_channel.queue_declare(queue='', exclusive=True)
        self.manager_queue = result.method.queue
        self.rmq_channel.queue_bind(exchange='yell_at_manager', queue=self.manager_queue)

        self.rmq_channel.exchange_declare(exchange='manager_broadcast', exchange_type='fanout')
        result = self.rmq_channel.queue_declare(queue='', exclusive=True)
        self.manager_broadcast_queue = result.method.queue
        self.rmq_channel.queue_bind(exchange='manager_broadcast', queue=self.manager_broadcast_queue)

        self.rmq_channel.queue_declare(queue='from_manager_to_client', durable=True)

        self.rmq_channel.queue_declare(queue='from_worker_to_manager', durable=True)

        self.rmq_channel.exchange_declare(exchange='yell_at_workers', exchange_type='fanout')

        def graceful_shutdown(signum, frame):
            print(f"Менеджер {self.my_id}: получен SIGTERM, закрываю соединение...")
            self.rmq_connection.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, graceful_shutdown)

        if self.state == 'KING':
            self.n_workers = n_workers * 2
            self.wait_for_workers() #wait for response from each worker
            self.n_workers = n_workers

        print(f"Я manager №{self.my_id} ! Я запустился!")
        self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body='')

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

    def run_election(self):
        received_ids = []

        election_msg = json.dumps({"purpose": "ELECTION", "id": self.my_id})
        self.rmq_channel.basic_publish(exchange='manager_broadcast', routing_key='', body=election_msg)

        def handle_election_message(ch, method, properties, body):
            msg = json.loads(body.decode("utf-8"))
            if msg.get("purpose") == "ELECTION":
                received_ids.append(msg.get("id"))

        self.rmq_channel.basic_consume(queue=self.manager_broadcast_queue, auto_ack=True, on_message_callback=handle_election_message)

        self.rmq_connection.call_later(self.election_timeout_duration, self.rmq_channel.stop_consuming)
        self.rmq_channel.start_consuming()

        self.state = 'KING' if self.my_id == max(received_ids + [self.my_id]) else 'REPLICA'

        if self.state == "KING":
            print(f"Менеджер {self.my_id}: я KING!")

            king_msg = json.dumps({"purpose": "KING", "id": self.my_id})
            self.rmq_channel.basic_publish(exchange='manager_broadcast', routing_key='', body=king_msg)

            self.king_id = self.my_id
        else:
            def handle_king_message(ch, method, properties, body):
                msg = json.loads(body.decode("utf-8"))
                if msg.get("purpose") == "KING":
                    self.king_id = msg.get("id")
                    self.rmq_channel.stop_consuming()
                    print(f"Менеджер {self.my_id}: обновил KING -> {self.king_id}")

            self.rmq_channel.basic_consume(queue=self.manager_broadcast_queue, auto_ack=True, on_message_callback=handle_king_message)
            self.rmq_channel.start_consuming()

    def run(self):
        flag_after_election = False

        def on_timeout():
            self.rmq_channel.stop_consuming()
            self.state = 'ELECTION'

        while True:
            if not flag_after_election:
                self.get_work_from_client()
            else:
                flag_after_election = False

            parts = self.command.split()
            cmd = parts[0].lower()

            if self.state == 'KING':
                self.rmq_channel.basic_publish(exchange='manager_broadcast', routing_key='', body='')
                self.rmq_channel.basic_consume(queue=self.manager_broadcast_queue, auto_ack=True, on_message_callback=lambda ch, method, props, body: self.rmq_channel.stop_consuming())
                self.rmq_channel.start_consuming() #Так как мы себе тоже это отправили!
            else:
                timer = self.rmq_connection.call_later(self.timeout_duration, on_timeout)

                def king_is_alive(ch, method, properties, body):
                    timer.cancel()
                    self.rmq_channel.stop_consuming()

                self.rmq_channel.basic_consume(queue=self.manager_broadcast_queue, auto_ack=True, on_message_callback=king_is_alive)
                self.rmq_channel.start_consuming()
                if self.state == 'ELECTION':
                    print(f"Менеджер №{self.my_id} установил не активность лидера. Был таймаут {self.timeout_duration} секунд! Приступаем к выборам.")
                    self.run_election()
                    if self.state == 'KING':
                        flag_after_election = True
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