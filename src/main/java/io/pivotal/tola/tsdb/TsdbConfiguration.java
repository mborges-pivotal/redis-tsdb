package io.pivotal.tola.tsdb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class TsdbConfiguration {
	
	@Autowired
	private JedisConnectionFactory jedisConnFactory;

	@Bean
	public StringRedisSerializer stringRedisSerializer() {
		StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
		return stringRedisSerializer;
	}

	@Bean
	public JacksonJsonRedisSerializer<Event> jacksonJsonRedisJsonSerializer() {
		JacksonJsonRedisSerializer<Event> jacksonJsonRedisJsonSerializer = new JacksonJsonRedisSerializer<>(Event.class);
		return jacksonJsonRedisJsonSerializer;
	}

	@Bean
	public RedisTemplate<String, Event> redisTemplate() {
		RedisTemplate<String, Event> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(jedisConnFactory);
		
		redisTemplate.setKeySerializer(stringRedisSerializer());
		redisTemplate.setValueSerializer(jacksonJsonRedisJsonSerializer());
		
		redisTemplate.setHashKeySerializer(stringRedisSerializer());
		//redisTemplate.setHashValueSerializer(jacksonJsonRedisJsonSerializer());
		
		return redisTemplate;
	}
	
}
