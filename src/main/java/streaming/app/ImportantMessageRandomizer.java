package streaming.app;

import org.apache.commons.lang.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import streaming.MessageSender;
import streaming.message.ImportantMessage;
import streaming.message.Message;

@Service
public class ImportantMessageRandomizer {

    private static final String TOPIC = "important";

    @Autowired
    private MessageSender messageSender;

    public void sendMessage() {
        String id = RandomStringUtils.randomNumeric(3);
        String text = RandomStringUtils.randomAlphanumeric(10);
        Message message = new ImportantMessage(id, text);
        messageSender.send(TOPIC, message);
    }
}
