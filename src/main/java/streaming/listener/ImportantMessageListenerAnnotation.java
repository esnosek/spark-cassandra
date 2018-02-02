package streaming.listener;

import lombok.extern.java.Log;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import streaming.message.Message;

@Log
//@Service
public class ImportantMessageListenerAnnotation {

    @KafkaListener(id = "id", groupId = "group1", topics = {"important", "void"})
    public void listen(Message message) {
        log.info("ANNOTATION LISTENER: " + message.toString());
    }
}
