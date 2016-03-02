package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

	/* Define per task state here. (kv stores etc) */
	private KeyValueStore<String, Map<String, String>> driverMap;
	private KeyValueStore<String, HashMap<String, String>> blockMap;

	@Override
	@SuppressWarnings("unchecked")
	public void init(Config config, TaskContext context) throws Exception {
		//Initialize stuff (maybe the kv stores?)
		driverMap = (KeyValueStore<String, Map<String, String>>) context.getStore("driver-list");
		blockMap = (KeyValueStore<String, HashMap<String, String>>) context.getStore("driver-loc");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		// The main part of your code. Remember that all the messages for a particular partition
		// come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
		// at one task only, thereby enabling you to do stateful stream processing.
		Map<String, Object> incomingJsonStream = (Map<String, Object>) envelope.getMessage();
		String type = (String)incomingJsonStream.get("type");
		if(type.equals("DRIVER_LOCATION")) {
			String blockId = Integer.toString((Integer) incomingJsonStream.get("blockId"));
			String driverId = Integer.toString((Integer) incomingJsonStream.get("driverId"));
			
			HashMap<String, String> driverSet = blockMap.get(blockId);
			if(driverSet == null) {
				driverSet = new HashMap<String, String>();
			}
			
			driverSet.put(driverId, driverId);
			blockMap.put(blockId, driverSet);

			String latitude = Integer.toString((Integer) incomingJsonStream.get("latitude"));
			String longitude = Integer.toString((Integer) incomingJsonStream.get("longitude"));
			Map<String, String> driver = new HashMap<String, String>();
			driver.put("latitude", latitude);
			driver.put("longitude", longitude);
			driverMap.put(driverId, driver);
			
		} else if(type.equals("LEAVING_BLOCK")) {
			String status = (String) incomingJsonStream.get("status");
			if(status.equals("AVAILABLE")) {//if avaiable, update info in kv store
				String blockId = Integer.toString((Integer) incomingJsonStream.get("blockId"));
				String driverId = Integer.toString((Integer) incomingJsonStream.get("driverId"));

				HashMap<String, String> driverSet = blockMap.get(blockId);
				driverSet.remove(driverId);
				blockMap.put(blockId, driverSet);
				driverMap.delete(driverId);
			}
			
		} else if(type.equals("ENTERING_BLOCK")) {
			String status = (String) incomingJsonStream.get("status");
			if(status.equals("AVAILABLE")) {//if avaiable, update info in kv store
				String blockId = Integer.toString((Integer) incomingJsonStream.get("blockId"));
				String driverId = Integer.toString((Integer) incomingJsonStream.get("driverId"));

				HashMap<String, String> driverSet = blockMap.get(blockId);
				if(driverSet == null) {
					driverSet = new HashMap<String, String>();
				}
				driverSet.put(driverId, driverId);
				blockMap.put(blockId, driverSet);

				String latitude = Integer.toString((Integer) incomingJsonStream.get("latitude"));
				String longitude = Integer.toString((Integer) incomingJsonStream.get("longitude"));
				Map<String, String> driver = new HashMap<String, String>();
				driver.put("latitude", latitude);
				driver.put("longitude", longitude);
				driverMap.put(driverId, driver);
			}
			
		} else if(type.equals("RIDE_REQUEST")) {
			String blockId = Integer.toString((Integer) incomingJsonStream.get("blockId"));
			String riderId = Integer.toString((Integer) incomingJsonStream.get("riderId"));
			String riderLa = Integer.toString((Integer) incomingJsonStream.get("latitude"));
			String riderLo = Integer.toString((Integer) incomingJsonStream.get("longitude"));

			HashMap<String, String> driverSet = blockMap.get(blockId);
			Set<Map.Entry<String, String>> driverEntrySet = driverSet.entrySet();
			double minDistance = Double.MAX_VALUE;
			String myDriverId = null;
			for(Map.Entry<String, String> dId : driverEntrySet) {
				String driverId = dId.getKey();
				Map<String, String> driver = (Map<String, String>) driverMap.get(driverId);
				if(driver == null) continue;
				String driverLa = (String) driver.get("latitude");
				String driverLo = (String) driver.get("longitude");
				double distance = getDistance(driverLa, driverLo, riderLa, riderLo);
				if(distance < minDistance) {
					minDistance = distance;
					myDriverId = driverId;
				}
			}

			driverSet.remove(myDriverId);
			blockMap.put(blockId, driverSet);
			driverMap.delete(myDriverId);
			
			Map<String, String> riderDriver = new HashMap<String, String>();
			riderDriver.put("riderId", riderId);
			riderDriver.put("driverId", myDriverId);
			
			collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, 
					riderDriver));
			
		} else { //RIDE_COMPLETE
			String blockId = Integer.toString((Integer) incomingJsonStream.get("blockId"));
			String driverId = Integer.toString((Integer) incomingJsonStream.get("driverId"));

			HashMap<String, String> driverSet = blockMap.get(blockId);
			if(driverSet == null) {
				driverSet = new HashMap<String, String>();
			}
			
			driverSet.put(driverId, driverId);
			blockMap.put(blockId, driverSet);

			String latitude = Integer.toString((Integer) incomingJsonStream.get("latitude"));
			String longitude = Integer.toString((Integer) incomingJsonStream.get("longitude"));
			Map<String, String> driver = new HashMap<String, String>();
			driver.put("latitude", latitude);
			driver.put("longitude", longitude);
			driverMap.put(driverId, driver);
		}
	}

	@Override
	public void window(MessageCollector collector, TaskCoordinator coordinator) {
		//this function is called at regular intervals, not required for this project
	}

	/* calculate distance between driver and rider */
	private double getDistance(String driverLa, String driverLo, 
			String riderLa, String riderLo) {
		return Math.sqrt(Math.pow(Double.parseDouble(driverLa) - Double.parseDouble(riderLa), 2) 
				+ Math.pow(Double.parseDouble(driverLo) - Double.parseDouble(riderLo), 2));
	}
}
