
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        consumer();
    }

    public static void consumer() {
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.21.113:9092,172.16.21.115:9092,172.16.21.116:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g3");
        //earliest、latest、none
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //自动提交，在consumer发生问题时，可能导致数据丢失/重复消费
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5");
        //最大拉取条数
        //p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);
        consumer.subscribe(Arrays.asList("20210721"));


        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

            if(!records.isEmpty()){
                System.out.println("++++++++++" + records.count() + "+++++++++++");

                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();
                    System.out.println("key:" + record.key() +
                            " val:" + record.value() +
                            " partition:" + partition +
                            " offset:" + offset);
                }
            }

        }

    }
}
