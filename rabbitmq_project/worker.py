import pika
import sys
import os
import re

class Worker:
    def __init__(self, k, n):
        self.my_number = k
        self.number_workers = n
        self.command = None
        self.worker_file_paths = None
        self.load_method = None
        self.worker_file_paths = []

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()

        self.rmq_channel.queue_declare(queue='from_worker_to_manager', durable=True)

        self.rmq_channel.exchange_declare(exchange='yell_at_workers', exchange_type='fanout')
        result = self.rmq_channel.queue_declare(queue='', exclusive=True)
        self.worker_queue = result.method.queue
        self.rmq_channel.queue_bind(exchange='yell_at_workers', queue=self.worker_queue)

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
            sentences = self.find_sentences_with_word(content, search_word)
            processed_sentences = []
            for sentence in sentences:
                processed = self.replace_first_three_columns(sentence)
                processed_sentences.append(processed)
            for i in processed_sentences:
                results.append("Worker №" + str(self.my_number) + " found " + search_word + " in \"" + str(os.path.basename(file_path)) + "\": " + i)

        return results

    def find_sentences_with_word(self, text, search_word):
        lines = text.strip().split('\n')
        found_sentences = []

        for line in lines:
            parts = line.split('\t')
            if len(parts) >= 3:
                text_part = '\t'.join(parts[3:])

                if re.search(r'\b' + re.escape(search_word) + r'\b', text_part, re.IGNORECASE):
                    found_sentences.append(text_part.strip())

        return found_sentences

    def replace_first_three_columns(self, line):
        parts = line.split('\t')
        if len(parts) >= 3:
            return '-\t-\t-' + '\t'.join(parts[3:])
        return line

    def run(self):
        while True:
            self.get_work_from_client()

            parts = self.command.split()
            cmd = parts[0].lower()

            if cmd == "find":
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
                self.rmq_channel.basic_publish(exchange='', routing_key='from_worker_to_manager', body='')

            elif cmd == "purge":
                self.worker_file_paths = None
                self.rmq_channel.basic_publish(exchange='', routing_key='from_worker_to_manager', body='')

if __name__ == "__main__":
    worker = Worker(int(sys.argv[1]), int(sys.argv[2]))
    worker.run()
