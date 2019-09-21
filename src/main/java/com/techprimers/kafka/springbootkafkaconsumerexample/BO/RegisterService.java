package com.techprimers.kafka.springbootkafkaconsumerexample.BO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.techprimers.kafka.springbootkafkaconsumerexample.connection.MongoConnection;

public class RegisterService {

	private static RegisterService instance = null;
	public static synchronized RegisterService getInstance() {
		if (instance == null) {
			instance = new RegisterService();
		}
		return instance;
	}
	public Map split(String message) {
		
		message = message.substring(1, message.length()-1);
		message = message.replaceAll("\"", "");
		String[] messageSplit = message.split(",");
		
		System.out.println("message: "+ message);
		
		Map resultMap = new HashMap<>();
		
		for(int i =0; i< messageSplit.length; i++) {
			System.out.println(messageSplit[i]);
			String[] keyValues = messageSplit[i].split(":");
			resultMap.put(keyValues[0], keyValues[1]);
		}
		//resultMap.remove("menu");
		System.out.println("split result:"+ resultMap);
		return resultMap;
	}
	
	public Map viewALlAirlines() {
		MongoDatabase md = null;
		Map resultMap = new HashMap<>();
		try {
			md = MongoConnection.getDBConnection();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			System.out.println("failed to open connection");
		}
		
		MongoCollection collection = md.getCollection("airlinesData");
		
		FindIterable<Map> existingAirlines = null;
		
		List<Map> airlineList = new ArrayList<>();
		
		try {
			existingAirlines = collection.find();
			for(Map airline: existingAirlines) {
				airlineList.add(airline);
			}

			resultMap.put("responseCode", "00");
			resultMap.put("airlineList", airlineList);
			System.out.println(resultMap);
			//resultMap.put("", receivedMap);				
			}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("error");
		}
		return resultMap;
	}
	
	public Map registerAirline(Map receivedMap){
		MongoDatabase md = null;
		Map resultMap = new HashMap<>();
		try {
			md = MongoConnection.getDBConnection();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			System.out.println("failed to open connection");
		}
		
		MongoCollection collection = md.getCollection("airlinesData");
		
		String airlineCode = (String)receivedMap.get("code");
		String airlineName = (String)receivedMap.get("name");
		String airlineStatus = (String)receivedMap.get("status");
		
		Document query = new Document();
		query.append("$eq", airlineCode);
		Document search = new Document();
		search.append("code", query);
		
		FindIterable<Map> existingAirlines = null;
		receivedMap.remove("menu");
		
		try {
			existingAirlines = collection.find(search);
			for(Map airline: existingAirlines) {
				if(airlineCode.equals(airline.get("code"))) {
					resultMap.put("responseCode", "01");
					resultMap.put("responseMessage", "Airlines already exist");
					return resultMap;
				}
			}

			Document docToSave = new Document();
			docToSave.put("code", airlineCode);
			docToSave.put("name", airlineName);
			docToSave.put("status", airlineStatus);
			collection.insertOne(docToSave);
			resultMap.put("responseCode", "00");
			resultMap.put("responseMessage", "Success to add");
			//resultMap.put("", receivedMap);				
			}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("error");
		}
		return resultMap;
	}

	
	public Map updateAirline(Map receivedMap) {
		MongoDatabase md = null;
		Map resultMap = new HashMap<>();
		try {
			md = MongoConnection.getDBConnection();
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			System.out.println("failed to open connection");
		}
		
		MongoCollection collection = md.getCollection("airlinesData");
		
		String airlineCode = (String)receivedMap.get("code");
		String airlineName = (String)receivedMap.get("name");
		String airlineStatus = (String)receivedMap.get("status");
		
		Document query = new Document();
		query.append("$eq", airlineCode);
		Document search = new Document();
		search.append("code", query);
		
		FindIterable<Map> existingAirlines = null;
		receivedMap.remove("menu");
		
		try {
			existingAirlines = collection.find(search);
			boolean isExist = false;
			
			Document oldDoc= new Document();
			for(Map airline: existingAirlines) {
				if(airlineCode.equals(airline.get("code"))) {
					isExist = true;
					oldDoc.putAll(airline);
				}
			}
			if(!isExist) {
				resultMap.put("responseCode", "01");
				resultMap.put("responseMessage", "Airline data doesnt exist");
			}

			
			Document docToSave = new Document();
			docToSave.put("code", airlineCode);
			docToSave.put("name", airlineName);
			docToSave.put("status", airlineStatus);
			
			Document updateQuery = new Document();
			updateQuery.append("$set", updateQuery);
			
			Document query2 = new Document();
	        query2.append("code", airlineCode);
			
			collection.replaceOne(query2, docToSave);
			resultMap.put("responseCode", "00");
			resultMap.put("responseMessage", "Success to update");
			//resultMap.put("", receivedMap);				
			}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.out.println("error");
		}
		return resultMap;
	}
}
