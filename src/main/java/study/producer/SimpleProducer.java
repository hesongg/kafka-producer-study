package study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String TOPIC_NAME = "test123";
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.ACKS_CONFIG, "1");

        /**
         * 필수 설정으로 프로듀서 정의
         * try-with-resource 로 초기 (producer close 필수)
         */
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {
            var messageValue = "testMessage for test123 topic";
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

            producer.send(record);
            logger.info("record send info: {}", record);

//            RecordMetadata metadata = producer.send(record).get();
//            logger.info("metadata info: {}", metadata.toString());

            producer.flush(); // accumulator 데이터 임의로 날림
        }
    }
}
