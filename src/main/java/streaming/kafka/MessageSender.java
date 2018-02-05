package streaming.kafka;

import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import streaming.kafka.entity.Message;

@Log
@Service
public class MessageSender {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void send(String topic, Message message) {
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, message);
    }
}
