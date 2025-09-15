import pika
import sys

class Manager:
    def wait_for_workers(self):
        def semaphore(ch, method, properties, body):
            self.tmp_number_workers -= 1
            if self.tmp_number_workers == 0:
                self.rmq_channel.stop_consuming()

        self.tmp_number_workers = self.number_workers
        self.rmq_channel.basic_consume(queue='from_worker_to_manager', auto_ack=True, on_message_callback=semaphore)
        self.rmq_channel.start_consuming()

    def get_work_from_client(self):
        def wait_from_client(ch, method, properties, body):
            self.command = body.decode("utf-8")
            self.rmq_channel.stop_consuming()

        self.rmq_channel.basic_consume(queue='from_client_to_manager', auto_ack=True, on_message_callback=wait_from_client)
        self.rmq_channel.start_consuming()

    def analyze_find_results(self):
        def wait_from_worker(ch, method, properties, body):
            self.text = body.decode("utf-8")
            if self.text != '':
                self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body=self.text)
            self.tmp_number_workers -= 1
            if self.tmp_number_workers == 0:
                self.rmq_channel.stop_consuming()

        self.tmp_number_workers = self.number_workers
        self.rmq_channel.basic_consume(queue='from_worker_to_manager', auto_ack=True, on_message_callback=wait_from_worker)
        self.rmq_channel.start_consuming()
        self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body="That's all")

    def __init__(self, k):
        self.tmp_number_workers = k
        self.number_workers = k
        self.command = None
        self.load_type = None

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()

        self.rmq_channel.queue_declare(queue='from_client_to_manager', durable=True)
        self.rmq_channel.queue_declare(queue='from_manager_to_client', durable=True)

        self.rmq_channel.queue_declare(queue='from_worker_to_manager', durable=True)

        self.rmq_channel.exchange_declare(exchange='yell_at_workers', exchange_type='fanout')

        self.wait_for_workers()

        self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body='')

        print("Я manager! Я запустился!")

    def run(self):
        while True:
            self.get_work_from_client()

            parts = self.command.split()
            cmd = parts[0].lower()

            self.rmq_channel.basic_publish(exchange='yell_at_workers', routing_key='', body=self.command)

            if cmd == "find":
                self.analyze_find_results()
            else:
                self.wait_for_workers()
                self.rmq_channel.basic_publish(exchange='', routing_key='from_manager_to_client', body='')

if __name__ == "__main__":
    user_input = sys.argv[1]
    manager = Manager(int(user_input))
    manager.run()