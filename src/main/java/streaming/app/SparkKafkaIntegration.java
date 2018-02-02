package streaming.app;

import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
import streaming.message.Message;

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

        JavaInputDStream<ConsumerRecord<String, Message>> javaInputDStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, Message>Subscribe(topics, consumerConfigs)
                );

        JavaPairDStream<String, Message> javaPairDStream = javaInputDStream.mapToPair(e -> new Tuple2<>(e.key(), e.value()) );
        JavaDStream<Message> msgDataStream = javaPairDStream.map(e -> e._2());

        msgDataStream.foreachRDD(rdd -> {
                SparkSession spark = JavaSparkSession.getInstance(rdd.context().getConf());
                Dataset<Row> msgDataFrame = spark.createDataFrame(rdd, ImportantMessage.class);
                msgDataFrame.printSchema();
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
