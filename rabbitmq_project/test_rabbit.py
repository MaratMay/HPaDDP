# test_rabbit.py
import pika
import time
import threading


def test_rabbitmq():
    print("🐇 ===== НАЧИНАЕМ ТЕСТ RABBITMQ ===== 🐇")
    connection = None
    try:
        # 1. Пытаемся подключиться
        print("1. Пытаюсь подключиться к RabbitMQ на localhost...")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=pika.PlainCredentials('guest', 'guest')  # стандартные логин/пароль
            )
        )
        channel = connection.channel()
        print("   ✅ УСПЕХ: Подключение установлено!")

        # 2. Создаем тестовую очередь
        print("2. Создаю тестовую очередь 'test_queue'...")
        channel.queue_declare(queue='test_queue')
        print("   ✅ Очередь создана!")

        # 3. Отправляем тестовое сообщение
        print("3. Отправляю тестовое сообщение...")
        message_body = 'Привет от PyCharm и Docker!'
        channel.basic_publish(
            exchange='',
            routing_key='test_queue',
            body=message_body,
            properties=pika.BasicProperties(delivery_mode=2)  # сообщение сохранится при перезагрузке
        )
        print(f"   ✅ Сообщение отправлено: '{message_body}'")

        # 4. Получаем сообщение обратно
        print("4. Пытаюсь получить сообщение из очереди...")

        # Создаем список для результата (чтобы получить его из callback функции)
        received_messages = []

        def callback(ch, method, properties, body):
            print(f"   ✅ Сообщение получено: '{body.decode()}'")
            received_messages.append(body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)  # подтверждаем обработку
            # Прерываем цикл потребления после первого сообщения
            ch.basic_cancel(consumer_tag=method.consumer_tag)

        # Подписываемся на очередь
        channel.basic_consume(queue='test_queue', on_message_callback=callback)

        # Запускаем потребление в отдельном потоке с таймаутом
        def consume_with_timeout():
            try:
                channel.start_consuming()
            except Exception as e:
                print(f"   ⚠️ Потребление остановлено: {e}")

        consumer_thread = threading.Thread(target=consume_with_timeout)
        consumer_thread.daemon = True
        consumer_thread.start()

        # Ждем сообщение не более 5 секунд
        timeout = 5
        for i in range(timeout):
            if received_messages:
                break
            time.sleep(1)

        # Проверяем результат
        if received_messages:
            print("   ✅ ТЕСТ ПРОЙДЕН! Сообщение успешно отправлено и получено!")
            if received_messages[0] == message_body:
                print("   ✅ Содержание сообщения совпадает!")
            else:
                print(f"   ⚠️ Ошибка: получено '{received_messages[0]}', ожидалось '{message_body}'")
        else:
            print("   ❌ ТЕСТ ПРОВАЛЕН: сообщение не получено в течение 5 секунд")
            print("   Возможные причины:")
            print("   - RabbitMQ не запущен")
            print("   - Порт 5672 закрыт")
            print("   - Ошибка сети")

    except Exception as e:
        print(f"   ❌ ОШИБКА: {e}")
        print("   Возможные причины:")
        print("   - Docker контейнер с RabbitMQ не запущен")
        print("   - Неправильные параметры подключения")
        print("   - Сетевые проблемы")
    finally:
        # Закрываем соединение
        if connection and not connection.is_closed:
            connection.close()
            print("5. Соединение закрыто.")

        # Даем время на завершение потока
        time.sleep(1)
        print("🐇 ===== ТЕСТ ЗАВЕРШЕН ===== 🐇")


if __name__ == "__main__":
    test_rabbitmq()
    # Пауза чтобы прочитать результат
    time.sleep(3)