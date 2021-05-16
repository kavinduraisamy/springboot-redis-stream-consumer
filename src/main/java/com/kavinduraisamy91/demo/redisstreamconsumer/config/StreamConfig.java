package com.kavinduraisamy91.demo.redisstreamconsumer.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.Subscription;

import com.kavinduraisamy91.demo.model.Post;



@Configuration
public class StreamConfig {
	
	@Value("${stream.key:reaction-events}")
    private String streamKey;
	
	@Value("${stream.groupName:defaultGroup}")
	private String groupName;

	@Value("${stream.consumerName:defaultConsumer}")
	private String consumerName;
	
    @Autowired
    private StreamListener<String, ObjectRecord<String, Post>> streamListener;

    @Bean
    public Subscription subscription(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {
    	StreamMessageListenerContainerOptions<String, ObjectRecord<String, Post>> options = StreamMessageListenerContainer
                            .StreamMessageListenerContainerOptions
                            .builder()
                            .pollTimeout(Duration.ofSeconds(1))
                            .targetType(Post.class)
                            .build();
    	 StreamMessageListenerContainer<String, ObjectRecord<String, Post>> listenerContainer = StreamMessageListenerContainer
                                    .create(redisConnectionFactory, options);
    	 Subscription subscription = listenerContainer.receiveAutoAck(
                Consumer.from(groupName, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                streamListener);
        listenerContainer.start();
        return subscription;
    }

}
