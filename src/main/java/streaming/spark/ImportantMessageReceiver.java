package streaming.spark;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import streaming.kafka.entity.ImportantMessage;
import streaming.kafka.entity.Message;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

@Service
public class ImportantMessageReceiver {

    public void receive(JavaStreamingContext streamingContext, Map<String, Object> consumerConfigs){
        createImportantMessageInputDStream(streamingContext, consumerConfigs)
                .mapToPair(e -> new Tuple2<>(e.key(), e.value()))
                .map(Tuple2::_2)
                .foreachRDD(this::saveImportantMessages);
    }

    private JavaInputDStream<ConsumerRecord<String, ImportantMessage>> createImportantMessageInputDStream
            (JavaStreamingContext streamingContext,  Map<String, Object> consumerConfigs){

        Collection<String> topics = Arrays.asList("important");
        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, ImportantMessage>Subscribe(topics, consumerConfigs)
        );
    }

    private void saveImportantMessages(JavaRDD<ImportantMessage> messageRDD){
        javaFunctions(messageRDD)
                .writerBuilder("java_api", "important_messages", mapToRow(ImportantMessage.class))
                .saveToCassandra();
    }

}
