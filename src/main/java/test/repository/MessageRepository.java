package test.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;
import test.entity.Message;

@Repository
public interface MessageRepository extends CassandraRepository<Message> {
}
