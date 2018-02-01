package streaming.message;

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
