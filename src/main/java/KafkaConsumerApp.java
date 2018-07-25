import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Ensure that kafka is up and running and ready to receive messages form producer.
 */
public class KafkaConsumerApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:92");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("group.id", "test");

        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);
        String[] topics = {"test0",
                "cmp.mailbox",
                "idx.export",
                "idx.metric",
                "idx.orchestration",
                "idx.project",
                "idx.rawmetrics",
                "idx.rosetta",
                "idx.statistic",
                "idx.system",
                "idx.template",
                "idx.user",
                "idx.workflow"};
        myConsumer.subscribe(Arrays.asList(topics));

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(10);
                for(ConsumerRecord r : records) {
                    System.out.println(String.format("Topic %s, Partition %d, Offset: %d, key %s, value %s",
                                                     r.topic(), r.partition(), r.offset(), r.key(), r.value()));
                    myConsumer.commitSync();
                }
            }
        }
        finally {
            myConsumer.close();
        }
    }

} 
