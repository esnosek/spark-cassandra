package streaming.app;

import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import streaming.app.cassandra.CassandraSchemaCreator;
import streaming.app.generator.MessageGenerator;
import streaming.app.generator.MessageGeneratorThread;
import streaming.spark.MessageReceiver;
@Log
@SpringBootApplication(scanBasePackages = {"streaming"})
public class Application implements CommandLineRunner {

    @Autowired
    private MessageGenerator messageGenerator;

    @Autowired
    private MessageReceiver messageReceiver;

    @Autowired
    private CassandraSchemaCreator cassandraSchemaCreator;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        cassandraSchemaCreator.createSchema();
        Thread generator = new Thread(new MessageGeneratorThread(messageGenerator), "generator");
        generator.start();
        messageReceiver.receive();
    }

}