from collections import deque
from datetime import datetime, timedelta

class Logger:
    def __init__(self, expiration_time=10, max_size=100):
        self.messages = {}
        self.queue = deque()
        self.expiration_time = expiration_time
        self.max_size = max_size

    def log_message(self, message, timestamp):
        timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        
        if message in self.messages and (timestamp_dt - self.messages[message]).total_seconds() < self.expiration_time:
            return False

        if len(self.messages) >= self.max_size:
            self._clear_all_messages()

        self.messages[message] = timestamp_dt
        self.queue.append((message, timestamp_dt))
        return True

    def _clear_all_messages(self):
        self.messages.clear()
        self.queue.clear()

    def clear_system(self, timestamp):
        timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        
        if all((timestamp_dt != ts) for _, ts in self.queue):
            self.messages.clear()
            self.queue.clear()

def process_message_stream(logger, message_stream):
    log_entries = []
    for message, timestamp in message_stream:
        if not message or (timestamp and not message.strip()):
            logger.clear_system(timestamp)
            log_entries.append(f"System cleared at {timestamp}")
        else:
            if logger.log_message(message, timestamp):
                log_entries.append(f"Logged message: '{message}' at {timestamp}")
            else:
                log_entries.append(f"Duplicate message ignored: '{message}' at {timestamp}")
    return log_entries

# Тесты с выводом журнала логов
def run_tests():
    logger = Logger()
    start_time = datetime(2023, 8, 3, 10, 0, 0)

    # Генерация 125 уникальных сообщений
    message_stream = [
        (f"Message {i}", (start_time + timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S'))
        for i in range(125)
    ]

    log_entries = process_message_stream(logger, message_stream)

    for entry in log_entries:
        print(entry)

    print("Current state of messages:", {msg: ts.strftime('%Y-%m-%d %H:%M:%S') for msg, ts in logger.messages.items()})
    print("Current state of queue:", [(msg, ts.strftime('%Y-%m-%d %H:%M:%S')) for msg, ts in logger.queue])

    # Проверка на пропуск сообщений
    print("\nTest: Skipping duplicates after max_size exceeded")
    logger = Logger()
    message_stream = [
        (f"Message {i}", (start_time + timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S'))
        for i in range(105)
    ]
    
    # Включаем 5 повторяющихся сообщений после очистки
    message_stream.extend([
        ("Message 0", (start_time + timedelta(seconds=125)).strftime('%Y-%m-%d %H:%M:%S')),
        ("Message 1", (start_time + timedelta(seconds=126)).strftime('%Y-%m-%d %H:%M:%S')),
        ("Message 2", (start_time + timedelta(seconds=127)).strftime('%Y-%m-%d %H:%M:%S')),
        ("Message 3", (start_time + timedelta(seconds=128)).strftime('%Y-%m-%d %H:%M:%S')),
        ("Message 4", (start_time + timedelta(seconds=129)).strftime('%Y-%m-%d %H:%M:%S')),
    ])

    log_entries = process_message_stream(logger, message_stream)

    for entry in log_entries:
        print(entry)

    print("Current state of messages:", {msg: ts.strftime('%Y-%m-%d %H:%M:%S') for msg, ts in logger.messages.items()})
    print("Current state of queue:", [(msg, ts.strftime('%Y-%m-%d %H:%M:%S')) for msg, ts in logger.queue])

    # Тест на обработку нескольких сообщений в одно и то же время
    print("\nTest: Multiple messages at the same time")
    logger = Logger()
    simultaneous_time = (start_time + timedelta(seconds=130)).strftime('%Y-%m-%d %H:%M:%S')
    message_stream = [
        ("Message A", simultaneous_time),
        ("Message B", simultaneous_time),
        ("Message A", simultaneous_time),  # Повторяющееся сообщение
        ("Message C", simultaneous_time),
        ("Message B", simultaneous_time),  # Повторяющееся сообщение
        ("Message D", simultaneous_time)
    ]

    log_entries = process_message_stream(logger, message_stream)

    for entry in log_entries:
        print(entry)

    print("Current state of messages:", {msg: ts.strftime('%Y-%m-%d %H:%M:%S') for msg, ts in logger.messages.items()})
    print("Current state of queue:", [(msg, ts.strftime('%Y-%m-%d %H:%M:%S')) for msg, ts in logger.queue])

# Запуск тестов
run_tests()
