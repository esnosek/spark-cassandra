package streaming.app.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CassandraSchemaCreator {

    @Autowired
    private SparkConf sparkConf;


    public void createSchema(){
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        CassandraConnector connector = CassandraConnector.apply(javaSparkContext.getConf());

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS java_api");
            session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE java_api.important_messages (id TEXT PRIMARY KEY, text TEXT, type TEXT)");
            session.execute("CREATE TABLE java_api.void_messages (id TEXT PRIMARY KEY, text TEXT, type TEXT)");
        }

        javaSparkContext.close();
    }

}
