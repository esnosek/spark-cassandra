package test.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableCassandraRepositories(basePackages = "test.repository")
public class CassandraConfig extends AbstractCassandraConfiguration {

    @Value("${cassandra.host}")
    private String host;

    @Value("${cassandra.port}")
    private int port;

    @Value("${cassandra.keyspace}")
    private String keySpace;

    @Override
    protected String getKeyspaceName() {
        return keySpace;
    }

    @Override
    protected String getContactPoints() {
        return host;
    }

    @Override
    protected int getPort() {
        return port;
    }

    @Override
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

    @Override
    protected List<String> getStartupScripts() {
        String script = "CREATE KEYSPACE IF NOT EXISTS " + keySpace + " "
                + "WITH durable_writes = true "
                + "AND replication = { 'replication_factor' : 1, 'class' : 'SimpleStrategy' };";
        return Arrays.asList(script);
    }

}
