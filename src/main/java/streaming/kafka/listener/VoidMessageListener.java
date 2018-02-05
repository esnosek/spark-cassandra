package streaming.kafka.listener;

import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import streaming.kafka.entity.VoidMessage;

@Log
public class VoidMessageListener implements MessageListener<Integer, VoidMessage> {

    @Override
    public void onMessage(ConsumerRecord<Integer, VoidMessage> consumerRecord) {
        log.info("VOID MESSAGE: " + consumerRecord.value().toString());
    }
}
