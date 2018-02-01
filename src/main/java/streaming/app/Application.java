package streaming.app;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import lombok.extern.java.Log;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Log
@SpringBootApplication(scanBasePackages = {"streaming"})
public class Application implements CommandLineRunner {

    @Autowired
    private JavaSparkContext sparkContext;

    @Autowired
    private MessageGenerator messageGenerator;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        CassandraConnector connector = CassandraConnector.apply(sparkContext.getConf());

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS java_api");
            session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
            session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
            session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
        }

        messageGenerator.generate();
    }

}