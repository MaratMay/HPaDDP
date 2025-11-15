import subprocess
import sys
import os
import signal
import pika
import time

class Client:
    def __init__(self, n_workers, n_managers):
        self.rmq_channel = None
        self.rmq_connection = None
        self.processes_managers = []
        self.processes_workers = []
        self.n_workers = n_workers
        self.n_managers = n_managers
        self.tmp_n_managers = n_managers
        self.dead_workers = set()
        self.timeout_duration = 20
        print("Клиент запущен. Введите 'start' для запуска системы.")

    def start_system(self):
        print(f"Запускаю систему с {self.n_workers} хранителями и {self.n_managers} 'подстраховками' менеджера...")

        for i in range(self.n_managers + 1):
            manager_process = subprocess.Popen([sys.executable, "manager.py", str(i), str(self.n_workers), str(self.n_managers + 1)])
            self.processes_managers.append(manager_process)
            print(f"Менеджер {i} запущен (PID: {manager_process.pid})")

        self.rmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rmq_channel = self.rmq_connection.channel()
        self.rmq_channel.exchange_declare(exchange='yell_at_manager', exchange_type='fanout')
        self.rmq_channel.queue_declare(queue='from_manager_to_client', durable=True)

        for i in range(self.n_workers * 2):
            worker_process = subprocess.Popen([sys.executable, "worker.py", str(i), str(self.n_workers)])
            self.processes_workers.append(worker_process)
            print(f"Хранитель {i} запущен (PID: {worker_process.pid})")

        def semaphore(ch, method, properties, body):
            self.tmp_n_managers -= 1
            if self.tmp_n_managers == 0:
                self.rmq_channel.stop_consuming()

        self.tmp_n_managers = self.n_managers + 1
        self.rmq_channel.basic_consume(queue='from_manager_to_client', auto_ack=True, on_message_callback=semaphore)
        self.rmq_channel.start_consuming() # wait for response from each manager

        print("Система готова к работе!")

    def stop_system(self):
        if not self.processes_workers and not self.processes_managers:
            print("Нет запущенных процессов для остановки")
            return

        print("Останавливаю систему...")

        for process in self.processes_workers + self.processes_managers:
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

        self.processes_managers = []
        self.processes_workers = []
        print("✅ Система остановлена")

    def run(self):

        flag_start: bool = False
        flag_load: bool = False
        flag_managers: bool = False

        try:
            while True:
                time.sleep(0.5)
                command: str = input("> ").strip()

                if not command:
                    continue

                parts = command.split()
                cmd = parts[0].lower()

                if flag_start:
                    if cmd == 'exit':
                        self.stop_system()
                        break

                    elif cmd == 'start':
                        print(f"Система уже запущена.")

                    elif flag_managers:
                        if cmd == "find":
                            if flag_load:
                                if len(parts) != 2:
                                    print("Недопустимое количество атрибутов.")
                                    continue

                                self.rmq_channel.basic_publish(exchange='yell_at_manager', routing_key='', body=command)

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
                            self.rmq_channel.basic_publish(exchange='yell_at_manager', routing_key='', body=command)

                            self.rmq_channel.basic_consume(queue='from_manager_to_client', auto_ack=True, on_message_callback=lambda ch, method, props, body: self.rmq_channel.stop_consuming())
                            self.rmq_channel.start_consuming()

                            flag_load = True
                            print(f"Папка {parts[1].lower()} была загружена успешно!")

                        elif cmd == "purge":
                            if flag_load:
                                print('Выполняю отчистку узлов...')

                                self.rmq_channel.basic_publish(exchange='yell_at_manager', routing_key='', body=command)

                                self.rmq_channel.basic_consume(queue='from_manager_to_client', auto_ack=True, on_message_callback=lambda ch, method, props, body: self.rmq_channel.stop_consuming())
                                self.rmq_channel.start_consuming()

                                flag_load = False
                            print("Отчистка узлов выполнена.")

                        elif cmd == "kill":
                            if len(parts) > 3 or len(parts) == 1:
                                print("Неправильный ввод программы.")
                                continue

                            victim = parts[1].lower()

                            if victim == 'manager' and len(parts) == 2:
                                victims_process = self.processes_managers.pop()
                                if not self.processes_managers:
                                    flag_managers = False

                            elif victim == 'worker' and len(parts) == 3:
                                if not parts[2].isdigit():
                                    print("Неправильный ввод команды")
                                    continue

                                worker_number = int(parts[2])

                                if worker_number in self.dead_workers:
                                    print(f"Хранитель {worker_number} уже не работает")
                                    continue

                                elif worker_number >= self.n_workers or worker_number < 0:
                                    print(f"Недопустимый номер ({worker_number}) хранителя. Используйте номер < {self.n_workers} и >= 0")
                                    continue

                                victims_process = self.processes_workers[worker_number]
                                self.dead_workers.add(worker_number)

                            else:
                                print("Неправильный ввод команды")
                                continue

                            try:
                                if os.name == 'posix': # Для Linux/Mac
                                    os.kill(victims_process.pid, signal.SIGTERM)
                                else: # Для Windows
                                    victims_process.terminate()
                                print(f"Процесс {victims_process} остановлен")

                            except:
                                print(f"Не удалось остановить процесс {victims_process}")

                        else:
                            print(f"Неизвестная команда: {cmd}")

                    else:
                        print("Менеджеров больше нет. Завершите программу, вызвав exit")
                        continue

                else:
                    if cmd == 'start':
                        self.start_system()
                        flag_start = True
                        flag_managers = True

                    else:
                        print(f"Для выполнения команд необходим запуск. Введите \'start\'")

        except KeyboardInterrupt:
            print(f"Принудительное завершение...")
            self.stop_system()
        finally:
            print(f"Клиент завершен")


if __name__ == "__main__":
    if len(sys.argv) > 2:
        user_input1 = sys.argv[1]
        user_input2 = sys.argv[2]
        if user_input1.isdigit() and user_input2.isdigit():
            client = Client(int(user_input1), int(user_input2))
            client.run()
            sys.exit(0)
        else:
            print("Неверный запуск программы. Оба аргумента должны быть числами.\nПринудительное завершение...")
            sys.exit(0)
    else:
        print("Неверный запуск программы. Требуется два аргумента.\nПринудительное завершение...")
        sys.exit(0)