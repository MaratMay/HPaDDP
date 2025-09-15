import subprocess
import sys
import os
import signal
import pika


class Client:
    def __init__(self, k):
        self.rmq_channel = None
        self.rmq_connection = None
        self.processes = []
        self.k = k
        print("Клиент запущен. Введите 'start' для запуска системы.")

    def wait_for_response(self):
        self.rmq_channel.basic_consume(queue='from_manager_to_client', auto_ack=True, on_message_callback=lambda ch, method, props, body: self.rmq_channel.stop_consuming())
        self.rmq_channel.start_consuming()

    def start_system(self):
        print(f"Запускаю систему с {self.k} хранителями...")

        manager_process = subprocess.Popen([sys.executable, "manager.py", str(self.k)])
        self.processes.append(manager_process)

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()
        self.rmq_channel.queue_declare(queue='from_client_to_manager', durable=True)
        self.rmq_channel.queue_declare(queue='from_manager_to_client', durable=True)

        print(f"Менеджер запущен (PID: {manager_process.pid})")

        for i in range(self.k):
            worker_process = subprocess.Popen([sys.executable, "worker.py", str(i), str(self.k)])
            self.processes.append(worker_process)
            print(f"Хранитель {i} запущен (PID: {worker_process.pid})")

        self.wait_for_response()
        print("Система готова к работе!")

    def stop_system(self):
        if not self.processes:
            print("Нет запущенных процессов для остановки")
            return

        print("Останавливаю систему...")

        for process in self.processes:
            try:
                # Для Linux/Mac
                if os.name == 'posix':
                    os.kill(process.pid, signal.SIGTERM)
                # Для Windows
                else:
                    process.terminate()
                print(f"Процесс {process.pid} остановлен")
            except:
                print(f"Не удалось остановить процесс {process.pid}")

        self.processes = []
        print("✅ Система остановлена")

    def run(self):

        flag_start: bool = False
        flag_load: bool = False

        try:
            while True:
                command: str = input("> ").strip()

                if not command:
                    continue

                parts = command.split()
                cmd = parts[0].lower()

                if flag_start:
                    if cmd == "find":
                        if flag_load:
                            if len(parts) != 2:
                                print("Недопустимое количество атрибутов.")
                                continue

                            self.rmq_channel.basic_publish(exchange='', routing_key='from_client_to_manager', body=command)

                            def find_result(ch, method, properties, body):
                                body_str = body.decode("utf-8")
                                print(body_str)
                                if body_str == "That's all":
                                    self.rmq_channel.stop_consuming()

                            self.rmq_channel.basic_consume(queue='from_manager_to_client', auto_ack=True, on_message_callback=find_result)
                            self.rmq_channel.start_consuming()

                        else:
                            print("Перед поиском произведите загрузку.")
                            continue


                    elif cmd == "load":
                        if len(parts) != 3:
                            print("Неправильный ввод программы.")
                            continue
                        if parts[1].lower() not in os.listdir('.'):
                            print(f"Папка {parts[1].lower()} отсутствует в директории ")
                            continue
                        if parts[2].lower() != 'e' and parts[2].lower() != 'u':
                            print(f"Неизвестный способ загрузки: {parts[2].lower()}")
                            continue

                        print(f"Приступаю к загрузке папки {parts[1].lower()} способом загрузки {parts[2].lower()}")
                        self.rmq_channel.basic_publish(exchange='', routing_key='from_client_to_manager', body=command)

                        self.wait_for_response()
                        flag_load = True
                        print(f"Папка {parts[1].lower()} была загружена успешно!")

                    elif cmd == "purge":
                        if flag_load:
                            print('Выполняю отчистку узлов...')

                            self.rmq_channel.basic_publish(exchange='', routing_key='from_client_to_manager', body=command)

                            self.wait_for_response()
                            flag_load = False
                        print("Отчистка узлов выполнена.")

                    elif cmd == 'exit':
                        self.stop_system()
                        break

                    elif cmd == 'start':
                        print(f"Система уже запущена.")

                    else:
                        print(f"Неизвестная команда: {cmd}")

                else:
                    if cmd == 'start':
                        self.start_system()
                        flag_start = True

                    else:
                        print(f"Для выполнения команд необходим запуск. Введите \'start\'")

        except KeyboardInterrupt:
            print(f"Принудительное завершение...")
            self.stop_system()
        finally:
            print(f"Клиент завершен")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        user_input = sys.argv[1]
        if user_input.isdigit():
            client = Client(int(user_input))
            client.run()
            sys.exit(0)
        else:
            print("Неверный запуск программы. \n Принудительное завершение...")
            sys.exit(0)
    else:
        print("Неверный запуск программы. \n Принудительное завершение...")
        sys.exit(0)