package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        String bootStrapServer = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for ( int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World");
            //producer.send(record);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.info("Exception ", e);
                    } else {
                        logger.info("topic " + recordMetadata.topic() + " partition " + recordMetadata.partition() + " offset " + recordMetadata.offset() + " timestamp " + recordMetadata.timestamp());
                    }
                }
            });
        }
        producer.flush();
        //flush and close
        producer.close();

    }

    /**
     * to see the msg being produced, run following command
     * kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my_second_application
     */
}