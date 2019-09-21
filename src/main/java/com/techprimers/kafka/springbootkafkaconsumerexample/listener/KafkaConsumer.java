package com.techprimers.kafka.springbootkafkaconsumerexample.listener;

import java.util.HashMap;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.techprimers.kafka.springbootkafkaconsumerexample.BO.RegisterService;
import com.techprimers.kafka.springbootkafkaconsumerexample.model.Flight;

//@Service
//public class KafkaConsumer {
//
//    @KafkaListener(topics = "Kafka_Example", group = "group_id")
//    public void consume(String message) {
//        System.out.println("Consumed message: " + message);
//    }
//
//
//    @KafkaListener(topics = "Kafka_Json_Example", group = "group_json",
//            containerFactory = "mapKafkaListenerFactory")
//    public void consumeJson(Map map) {
//        System.out.println("Consumed JSON Message: " + map);
//    }
//}



@Service
public class KafkaConsumer {
	

	@KafkaListener(topics = "Kafka_Example", group = "group_id")
    @KafkaHandler
    public Map consume(String message) {
		ObjectMapper mapper = new ObjectMapper();
		//Map receivedMap = mapper.convertValue(message, HashMap.class);
		Map receivedMap = RegisterService.getInstance().split(message);
		
		String menu = (String) receivedMap.get("menu");
		Map resultMap = new HashMap<>();
		if("register".equalsIgnoreCase(menu)) {
			resultMap = RegisterService.getInstance().registerAirline(receivedMap);
		}
		else if("update".equalsIgnoreCase(menu)){
			resultMap = RegisterService.getInstance().updateAirline(receivedMap);
		}
		else {
			resultMap = RegisterService.getInstance().viewALlAirlines();
		}
        System.out.println("Result: \n" + resultMap);
       return resultMap;
    }


    @KafkaListener(topics = "Kafka_Json_Example", group = "group_json",
            containerFactory = "flightKafkaListenerFactory")
    @KafkaHandler
    public void consumeJson(Flight flight) {
        System.out.println("Consumed JSON Message: " + flight);
    }
}
