package streaming.kafka.listener;

import lombok.extern.java.Log;
import org.springframework.kafka.annotation.KafkaListener;
import streaming.kafka.entity.Message;

@Log
//@Service !Inactive!
public class ImportantMessageListenerAnnotation {

    @KafkaListener(id = "id", groupId = "group1", topics = {"important", "void"})
    public void listen(Message message) {
        log.info("ANNOTATION LISTENER: " + message.toString());
    }
}
