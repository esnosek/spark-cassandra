package streaming.app;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MessageGenerator {

    @Autowired
    private ImportantMessageRandomizer importantMessageRandomizer;
    @Autowired
    private VoidMessageRandomizer voidMessageRandomizer;

    public void generate() throws InterruptedException {
        while (true) {
            importantMessageRandomizer.sendMessage();
            Thread.sleep(1000l);
            voidMessageRandomizer.sendMessage();
            Thread.sleep(1000l);
        }
    }
}
