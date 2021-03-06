import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.common.utils.Utils.sleep;

/**
 * Ensure that kafka is up and running and ready to receive messages form producer.
 */
public class KafkaProducerApp {

    public static void main(String[] args) {

        // Used  by ProducerConfig
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:92");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> myProducer = new KafkaProducer<String, String>(props);
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
        String msg  ="Message to test topic: ";
        int counter = 0;
        try {
            System.out.println("Starting posting ... ");
            while (true) {
                for(String topic : topics) {
                    myProducer.send(new ProducerRecord(topic, msg + ":" + counter++));
                    System.out.println("posting " + msg + topic + ":" + counter);
                    sleep(5000);
                }
            }
        }
        finally {
            myProducer.close();
        }
    }

}
