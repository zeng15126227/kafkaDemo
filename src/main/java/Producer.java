import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        producer("20210721");
    }

    public static void producer(String topic) throws ExecutionException, InterruptedException {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.21.113:9092,172.16.21.115:9092,172.16.21.116:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,"item" + j, "val" + i);
                Future<RecordMetadata> send = producer.send(record);
                RecordMetadata rm = send.get();
                int partition = rm.partition();
                long offset = rm.offset();
                System.out.println("key:" + record.key() +
                        " val:" + record.value() +
                        " partition:" + partition +
                        " offset:" + offset);
            }
        }
    }
}
