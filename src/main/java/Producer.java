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
        producer("20210724_2");
    }

    public static void producer(String topic) throws ExecutionException, InterruptedException {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.21.113:9092,172.16.21.115:9092,172.16.21.116:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        p.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        p.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,"60000");
        //分析msg大小，通过batch发送消息，减少特殊大小batch的创建和回收，减少内存碎片
        p.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        //多个batch放入一个请求，传输到broker
        p.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"1048576");
        //发送出去的请求，但没有收到回复的最大请求数量，超过后阻塞
        p.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        //tcp发送buffer大小
        p.setProperty(ProducerConfig.SEND_BUFFER_CONFIG,"21768");
        //tcp接收buffer大小
        p.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG,"21768");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        for(;;){
            for (int i = 0; i < 100; i++) {
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
}
