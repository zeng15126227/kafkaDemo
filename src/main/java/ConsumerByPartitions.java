
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ConsumerByPartitions {
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
        //p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5");
        //最大拉取条数
        //p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        //分区自动负载均衡
        consumer.subscribe(Arrays.asList("20210724_2"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("失去分区");
                Iterator<TopicPartition> partitionIterator = collection.iterator();
                while(partitionIterator.hasNext()){
                    System.out.println(partitionIterator.next().partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("获得分区");
                Iterator<TopicPartition> partitionIterator = collection.iterator();
                while(partitionIterator.hasNext()){
                    System.out.println(partitionIterator.next().partition());
                }
            }
        });


        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            if(!records.isEmpty()){
                System.out.println("++++++++++" + records.count() + "+++++++++++");
                //按照分区消费数据
                Set<TopicPartition> partitions = records.partitions();
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> recordList = records.records(partition);
                    Iterator<ConsumerRecord<String, String>> iterator = recordList.iterator();
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();
                        int partition1 = record.partition();
                        long offset = record.offset();
                        System.out.println("key:" + record.key() +
                                " val:" + record.value() +
                                " partition:" + partition1 +
                                " offset:" + offset);

                    }
                }
            }

        }

    }
}
