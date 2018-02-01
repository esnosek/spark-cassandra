package streaming.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {


    @Value("${cassandra.host}")
    private String cassandraHost;

    @Value("${cassandra.port}")
    private String cassandraPort;

    @Bean
    public SparkConf sparkConf() {
        SparkConf conf = new SparkConf();
        conf.setAppName("Kafka-Spark-Cassandra");
        conf.setMaster("local[4]");
        conf.set("spark.cassandra.connection.host", cassandraHost);
        conf.set("spark.cassandra.connection.port", cassandraPort);
        return conf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

}
