package streaming.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import streaming.kafka.entity.VoidMessage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

@Service
public class VoidMessageReceiver {

    public void receive(JavaStreamingContext streamingContext, Map<String, Object> consumerConfigs){
        createVoidMessageInputDStream(streamingContext, consumerConfigs)
                .mapToPair(e -> new Tuple2<>(e.key(), e.value()))
                .map(Tuple2::_2)
                .foreachRDD(this::saveVoidMessages);
    }

    private JavaInputDStream<ConsumerRecord<String, VoidMessage>> createVoidMessageInputDStream
            (JavaStreamingContext streamingContext,  Map<String, Object> consumerConfigs){

        Collection<String> topics = Arrays.asList("void");
        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, VoidMessage>Subscribe(topics, consumerConfigs)
        );
    }
    private void saveVoidMessages(JavaRDD<VoidMessage> messageRDD){
        javaFunctions(messageRDD)
                .writerBuilder("java_api", "void_messages", mapToRow(VoidMessage.class))
                .saveToCassandra();
    }

}
