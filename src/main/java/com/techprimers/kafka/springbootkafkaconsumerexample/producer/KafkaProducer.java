package com.techprimers.kafka.springbootkafkaconsumerexample.producer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Map> kafkaTemplate;

    private static final String TOPIC = "Kafka_Example";

//    @GetMapping("/publish/{name}")
//    public String post(@PathVariable("name") final String name) {
//
//        kafkaTemplate.send(TOPIC, new HashMap<>());
//
//        return "Published successfully";
//    }
//    @GetMapping("/publish/{message}")
//    public String post(@PathVariable("message") final String message) {
//
//    		System.out.println("message: "+message);
//    		Map messageMap = new HashMap<>();
//    		messageMap.put("message", message);
//        kafkaTemplate.send(TOPIC, messageMap);
//
//        return "Published successfully";
//    }
    
	  @PostMapping("/register")
	  public String register(@RequestBody Map messageMap) {
		  messageMap.put("menu", "register");
	      kafkaTemplate.send(TOPIC, messageMap);
	      return "Published successfully";
	  }
	  
	  @PostMapping("/update")
	  public String update(@RequestBody Map messageMap) {
		  messageMap.put("menu", "update");
	      
		  kafkaTemplate.send(TOPIC, messageMap);
	      return "Published successfully";
	  }
	  
	  
	@GetMapping("/view")
	public String post() {
		Map messageMap = new HashMap<>();
		messageMap.put("message", "view_airline");
	    kafkaTemplate.send(TOPIC, messageMap);
	    return "Published successfully";
}

}
