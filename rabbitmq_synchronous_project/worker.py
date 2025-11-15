import pika
import sys
import os
import re
import signal


def find_sentences_with_word(text, search_word):
    lines = text.strip().split('\n')
    found_sentences = []

    for line in lines:
        parts = line.split('\t')
        if len(parts) >= 3:
            text_part = '\t'.join(parts[3:])

            if re.search(r'\b' + re.escape(search_word) + r'\b', text_part, re.IGNORECASE):
                found_sentences.append(text_part.strip())

    return found_sentences


def replace_first_three_columns(line):
    parts = line.split('\t')
    if len(parts) >= 3:
        return '-\t-\t-' + '\t'.join(parts[3:])
    return line


class Worker:
    def __init__(self, k, n):
        self.my_number = k
        self.number_workers = n
        self.command = None
        self.load_method = None
        self.worker_file_paths = []

        self.timeout_duration = 5

        self.state = "WORKING" if self.my_number < self.number_workers else 'REPLICA' #WORKING or REPLICA or WORKING_ALONE
        self.my_sibling = (self.my_number + self.number_workers) % (2 * self.number_workers)

        self.sibling_queue = f'from_{min(self.my_number, self.my_sibling)}_to_{max(self.my_number, self.my_sibling)}'

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()

        self.rmq_channel.queue_declare(queue='from_worker_to_manager', durable=True)

        self.rmq_channel.queue_declare(queue=self.sibling_queue, durable=True)

        self.rmq_channel.exchange_declare(exchange='yell_at_workers', exchange_type='fanout')
        result = self.rmq_channel.queue_declare(queue='', exclusive=True)
        self.worker_queue = result.method.queue
        self.rmq_channel.queue_bind(exchange='yell_at_workers', queue=self.worker_queue)

        def graceful_shutdown(signum, frame):
            print(f"Хранитель {self.my_number}: получен SIGTERM, закрываю соединение...")
            self.rmq_connection.close()
            sys.exit(0)

        signal.signal(signal.SIGTERM, graceful_shutdown)

        self.rmq_channel.basic_publish(exchange='', routing_key='from_worker_to_manager', body='')
        print(f"Я worker {self.my_number}! Я запустился!")

    def get_work_from_client(self):
        def wait_from_client(ch, method, properties, body):
            self.command = body.decode("utf-8")
            self.rmq_channel.stop_consuming()

        self.rmq_channel.basic_consume(queue=self.worker_queue, auto_ack=True, on_message_callback=wait_from_client)
        self.rmq_channel.start_consuming()

    def process_files(self, search_word):
        results = []

        for file_path in self.worker_file_paths:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            sentences = find_sentences_with_word(content, search_word)
            processed_sentences = []
            for sentence in sentences:
                processed = replace_first_three_columns(sentence)
                processed_sentences.append(processed)
            for i in processed_sentences:
                results.append("Worker №" + str(self.my_number) + " found " + search_word + " in \"" + str(os.path.basename(file_path)) + "\": " + i)

        return results

    def run(self):
        flag_after_funeral = False

        def on_timeout():
            self.rmq_channel.stop_consuming()
            self.state = 'WORKING_ALONE'

        while True:
            if not flag_after_funeral:
                self.get_work_from_client()
            else:
                flag_after_funeral = False

            parts = self.command.split()
            cmd = parts[0].lower()

            if self.state == 'WORKING':
                self.rmq_channel.basic_publish(exchange='', routing_key=self.sibling_queue, body='')
            elif self.state == 'REPLICA':
                timer = self.rmq_connection.call_later(self.timeout_duration, on_timeout)

                def sibling_is_alive(ch, method, properties, body):
                    timer.cancel()
                    self.rmq_channel.stop_consuming()

                self.rmq_channel.basic_consume(queue=self.sibling_queue, auto_ack=True, on_message_callback=sibling_is_alive)
                self.rmq_channel.start_consuming()

                if self.state == 'WORKING_ALONE':
                    flag_after_funeral = True
                    print(f"Хранитель №{self.my_number} установил не активность хранителя №{self.my_sibling}. Таймаут {self.timeout_duration} секунд!")

            if cmd == "find" and self.state != 'REPLICA':
                if self.load_method == "e" or self.my_number <= 1:
                    messages = self.process_files(parts[1])
                else:
                    messages = []
                self.rmq_channel.basic_publish(exchange='', routing_key='from_worker_to_manager', body='\n'.join(messages))


            elif cmd == "load":
                file_paths = []
                for filename in os.listdir(parts[1]):
                    file_path = os.path.join(parts[1], filename)
                    if os.path.isfile(file_path):
                        file_paths.append(file_path)
                file_paths.sort(key=lambda x: os.path.getsize(x), reverse=True)

                if parts[2].lower() == 'e' or self.number_workers == 1:
                    self.worker_file_paths = file_paths[self.my_number:: self.number_workers]
                    self.load_method = 'e'
                else:
                    self.worker_file_paths = file_paths[self.number_workers:] if self.my_number == 0 else [file_paths[self.my_number - 1]]
                    self.load_method = 'u'

                if self.state != 'REPLICA':
                    self.rmq_channel.basic_publish(exchange='', routing_key='from_worker_to_manager', body='')

            elif cmd == "purge":
                self.worker_file_paths = []
                if self.state != 'REPLICA':
                    self.rmq_channel.basic_publish(exchange='', routing_key='from_worker_to_manager', body='')

if __name__ == "__main__":
    worker = Worker(int(sys.argv[1]), int(sys.argv[2]))
    worker.run()