package streaming.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import streaming.kafka.entity.ImportantMessage;
import streaming.kafka.entity.Message;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class MessageReceiverGeneric<T extends Message> {

    private Class<T> clazz;
    private List<String> topics;
    private String table;
    private String keyspace;

    public MessageReceiverGeneric(Class<T> clazz, List<String> topics, String keyspace, String table){
        this.clazz = clazz;
        this.topics = topics;
        this.table = table;
        this.keyspace = keyspace;
    }

    public void receive(JavaStreamingContext streamingContext, Map<String, Object> consumerConfigs){
        createImportantMessageInputDStream(streamingContext, consumerConfigs)
                .mapToPair(e -> new Tuple2<>(e.key(), e.value()))
                .map(Tuple2::_2)
                .foreachRDD(this::saveImportantMessages);
    }

    private JavaInputDStream<ConsumerRecord<String, T>> createImportantMessageInputDStream
            (JavaStreamingContext streamingContext,  Map<String, Object> consumerConfigs){
        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, T>Subscribe(topics, consumerConfigs)
        );
    }

    private void saveImportantMessages(JavaRDD<T> messageRDD){
        javaFunctions(messageRDD)
                .writerBuilder(keyspace, table, mapToRow(clazz))
                .saveToCassandra();
    }

}
