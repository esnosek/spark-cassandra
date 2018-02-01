package streaming.listener;

import lombok.NoArgsConstructor;
import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import streaming.message.Message;

@Log
@NoArgsConstructor
public class MessageProducerListener implements ProducerListener<String, Message> {

    @Override
    public void onSuccess(String topic, Integer partition, String key, Message value, RecordMetadata recordMetadata) {
        log.info("Message: " + value + " SUCCESSFULLY delivered to topic: " + topic);
    }

    @Override
    public void onError(String topic, Integer partition, String key, Message value, Exception e) {
        log.info("ERROR during sending the message: " + value + " to topic: " + topic);
    }

    @Override
    public boolean isInterestedInSuccess() {
        return true;
    }

}
