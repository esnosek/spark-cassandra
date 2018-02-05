package streaming.app;

import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Log
@SpringBootApplication(scanBasePackages = {"streaming"})
public class Application implements CommandLineRunner {

    @Autowired
    private MessageGenerator messageGenerator;

    @Autowired
    private SparkKafkaIntegration sparkKafkaIntegration;

    @Autowired
    private CassandraCreator cassandraCreator;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        cassandraCreator.createSchema();
        Thread generator = new Thread(new MessageGeneratorThread(messageGenerator), "generator");
        generator.start();
        sparkKafkaIntegration.test();
    }

}