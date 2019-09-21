package com.techprimers.kafka.springbootkafkaconsumerexample.connection;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoConnection {
	
//		private static Logger log=Logger.getLogger(MongoConnection.class);
		
		private static Boolean isUsingJNDI=null;
		
		public static MongoDatabase getDBConnection() {
			String connCfg="DEFAULT|mongohost|27017|mongotesting";
			MongoClient mongo = new MongoClient( "localhost" , 27017 );
			//MongoManager.init(connCfg);
			MongoDatabase md =  mongo.getDatabase("mongotesting");
			return md;
		}
		
		public static void main(String[] args) {
			
		}
	

}
