package streaming.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import streaming.listener.ImportantMessageListener;
import streaming.listener.MessageProducerListener;
import streaming.listener.VoidMessageListener;
import streaming.message.Message;
import streaming.message.MessageDeserializer;
import streaming.message.MessageSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.hostname}")
    private String hostname;

    @Value("${kafka.groupId}")
    private String gruopId;

//    @Bean
//    public KafkaMessageListenerContainer importantMessageContainerFactory(){
//        ContainerProperties containerProps = new ContainerProperties("important");
//        containerProps.setMessageListener(new ImportantMessageListener());
//        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProps);
//    }
//
//    @Bean
//    public KafkaMessageListenerContainer voidMessageContainerFactory(){
//        ContainerProperties containerProps = new ContainerProperties("void");
//        containerProps.setMessageListener(new VoidMessageListener());
//        return new KafkaMessageListenerContainer<>(consumerFactory(), containerProps);
//    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, Message> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, Message> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean("consumerConfigs")
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, gruopId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class);
        return props;
    }

    @Bean
    public KafkaTemplate<String, Message> kafkaTemplate() {
        KafkaTemplate<String, Message> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setProducerListener(new MessageProducerListener());
        return kafkaTemplate;
    }

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic importantTopic() {
        return new NewTopic("important", 5, (short) 1);
    }

    @Bean
    public NewTopic voidTopic() {
        return new NewTopic("void", 5, (short) 1);
    }
}


