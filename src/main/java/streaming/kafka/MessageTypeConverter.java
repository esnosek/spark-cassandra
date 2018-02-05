package streaming.kafka;

import streaming.kafka.entity.ImportantMessage;
import streaming.kafka.entity.Message;
import streaming.kafka.entity.VoidMessage;

import java.util.function.Function;

import static javaslang.API.*;

public class MessageTypeConverter implements Function<String, Class<? extends Message>> {

    @Override
    public Class<? extends Message> apply(String topic) {
        return Match(topic).of(
                Case($("important"), ImportantMessage.class),
                Case($(), VoidMessage.class));

    }
}
