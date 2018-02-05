package streaming.spark;

import lombok.extern.java.Log;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import streaming.kafka.entity.ImportantMessage;
import streaming.kafka.entity.VoidMessage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

@Service
@Log
public class MessageReceiver implements Serializable{

    @Autowired
    private SparkConf sparkConf;

//    @Autowired
//    private ImportantMessageReceiver importantMessageReceiver;
//
//    @Autowired
//    private VoidMessageReceiver voidMessageReceiver;

    @Value("#{consumerConfigs}")
    private Map<String, Object> consumerConfigs;

    public void receive() throws InterruptedException {

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        //importantMessageReceiver.receive(streamingContext,consumerConfigs);
        //voidMessageReceiver.receive(streamingContext,consumerConfigs);

        MessageReceiverGeneric<ImportantMessage> importantMessageReceiver = new MessageReceiverGeneric<>
                (ImportantMessage.class, Arrays.asList("important"), "java_api", "important_messages");
        importantMessageReceiver.receive(streamingContext, consumerConfigs);

        MessageReceiverGeneric<VoidMessage> voidMessageReceiver = new MessageReceiverGeneric<>
                (VoidMessage.class, Arrays.asList("void"), "java_api", "void_messages");
        voidMessageReceiver.receive(streamingContext, consumerConfigs);

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
