package com.practice.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.kafka.consumer.OrderDTO;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerializer implements Serializer<OrderDTO> {
    public static final Logger logger = LoggerFactory.getLogger(OrderSerializer.class.getName())
    ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public byte[] serialize(String topic, OrderDTO data) {
        byte[] serializedOrder = null;

        try {
            objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            logger.error("Json serialization exception:" + e.getMessage());

        }

    }
}
