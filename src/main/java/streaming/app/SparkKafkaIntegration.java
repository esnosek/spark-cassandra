package streaming.app;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import streaming.message.ImportantMessage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@Service
@Log
public class SparkKafkaIntegration implements Serializable{

    @Autowired
    private SparkConf sparkConf;

    @Value("#{consumerConfigs}")
    private Map<String, Object> consumerConfigs;

    public void test() throws InterruptedException {

        Collection<String> topics = Arrays.asList("important");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaInputDStream<ConsumerRecord<String, ImportantMessage>> javaInputDStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, ImportantMessage>Subscribe(topics, consumerConfigs)
                );

        JavaPairDStream<String, ImportantMessage> javaPairDStream = javaInputDStream.mapToPair(e -> new Tuple2<>(e.key(), e.value()) );
        JavaDStream<ImportantMessage> msgDataStream = javaPairDStream.map(Tuple2::_2);

        msgDataStream.foreachRDD(rdd -> {

            CassandraJavaUtil.javaFunctions(rdd)
                    .writerBuilder("java_api", "important_messages", CassandraJavaUtil.mapToRow(ImportantMessage.class))
                    .saveToCassandra();

//                SparkSession spark = JavaSparkSession.getInstance(rdd.context().getConf());
//                Dataset<Row> msgDataFrame = spark.createDataFrame(rdd, ImportantMessage.class);
//                msgDataFrame.printSchema();
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
