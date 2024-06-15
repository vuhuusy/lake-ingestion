from time import sleep
import requests
from confluent_kafka import Producer

# Thiết lập Kafka Producer
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9091'
}
producer = Producer(**conf)

# Tên Kafka Topic
topic_name = 'wikimedia_recentchange'

def fetch_recent_changes():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            yield line

def delivery_report(err, msg):
    """
    Callback khi nhận được phản hồi từ Kafka
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}, partition[{msg.partition()}]")

def send_to_kafka(producer, topic, message):
    """
    Hàm để gửi message tới Kafka Topic
    """
    producer.produce(topic, message, callback=delivery_report)
    producer.poll(0)

def main(producer, topic):
    """
    Hàm lấy dữ liệu thay đổi gần đây từ Wikimedia và gửi tới Kafka
    """
    running = True
    
    try:
        for message in fetch_recent_changes():
            if not running:  # Kiểm tra biến cờ dừng lại
                break
            
            if message:
                if message.startswith(b'data:'):
                    send_to_kafka(producer, topic, message)
                    print(f"Message Sent")    
                
    except KeyboardInterrupt:
        running = False
        print("Stopping producer...")
        sleep(1)
        print("Producer stopped!")
    
    # Đảm bảo tất cả các message được gửi đi
    producer.flush()

if __name__ == '__main__':
    main(producer, topic_name)
