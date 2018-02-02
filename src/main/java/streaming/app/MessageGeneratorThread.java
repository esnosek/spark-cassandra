package streaming.app;

public class MessageGeneratorThread implements Runnable {

    private MessageGenerator messageGenerator;

    public MessageGeneratorThread(MessageGenerator messageGenerator) {
        this.messageGenerator = messageGenerator;
    };

    @Override
    public void run() {
        try {
            messageGenerator.generate();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
