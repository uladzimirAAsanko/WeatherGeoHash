package by.sanko.hash;



import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {
    private static final String CONNECTION = "host.docker.internal:9094";
    private static final String CONSUMER_GROUP = "KafkaExampleConsumer";
    private static final String SUBSCRIBE_TOPIC = "weather-data";
    private static final String OUTPUT_TOPIC = "weather-data-hash";
    static Consumer<String, String> consumer = null;
    static Producer<String, String> producer = null;

    public static void main(String[] args) {
        init();
        char comma = ',';
        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                }
                else {
                    continue;
                }
            }
            consumerRecords.forEach(record -> {
               String value = record.value();
               int first_comma = value.indexOf(comma);
               Double lng = Double.parseDouble(value.substring(0, first_comma));
               Double lat = Double.parseDouble(value.substring(first_comma, value.indexOf(comma, first_comma +1 )));
               String hash = Generator.generateGeoHash(lat, lng);
               StringBuilder builder = new StringBuilder();
               builder.append(value).append(comma).append(hash);
               send(producer, builder.toString());
            });

            consumer.commitAsync();
        }
    }

    private static void send(Producer<String, String> producer, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, value);
        producer.send(record);
    }

    private static void init(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", CONNECTION);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(properties);
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CONNECTION);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(SUBSCRIBE_TOPIC));
    }
}
