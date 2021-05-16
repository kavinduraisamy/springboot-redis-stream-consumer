package com.kavinduraisamy91.demo.redisstreamconsumer.service;


import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import com.kavinduraisamy91.demo.model.Post;

@Service
public class ReactionConsumerService implements StreamListener<String, ObjectRecord<String, Post>>{

	@Override
	public void onMessage(ObjectRecord<String, Post> message) {
		System.out.println("Consumed message: "+message.getValue().getName());
		
	}

}
