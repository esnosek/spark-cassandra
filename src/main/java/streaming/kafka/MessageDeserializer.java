package streaming.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import streaming.kafka.entity.Message;

import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Message deserialize(String topic, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Message message = null;
        try {
            message = mapper.readValue(bytes, new MessageTypeConverter().apply(topic));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    @Override
    public void close() {

    }
}
