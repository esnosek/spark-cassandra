package streaming.app;

import org.apache.commons.lang.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import streaming.MessageSender;
import streaming.message.Message;
import streaming.message.VoidMessage;

@Service
public class VoidMessageRandomizer {

    private static final String TOPIC = "void";

    @Autowired
    private MessageSender messageSender;

    public void sendMessage() {
        String id = RandomStringUtils.randomNumeric(3);
        String text = RandomStringUtils.randomAlphanumeric(10);
        Message message = new VoidMessage(id, text);
        messageSender.send(TOPIC, message);
    }

}
