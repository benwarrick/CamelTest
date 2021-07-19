package CamelTest.beans;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import org.bson.Document;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import java.util.zip.CRC32;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import javax.swing.text.NumberFormatter;

public class TestAggregationStrategy implements AggregationStrategy {
	public Exchange aggregate(Exchange original, Exchange resource) {
		System.out.println("Original: " + original.getMessage().getBody().toString());
		System.out.println("Resource: " + resource.getMessage().getBody().toString()); 
	
		// Parse the message payload -- the update from the exchange
		DocumentContext updateContext = JsonPath.parse(original.getMessage().getBody().toString());
		List<List<Double>> bids = updateContext.read("$.data.bids");
		List<List<Double>> asks = updateContext.read("$.data.asks");
		
		// Parse the resource payload, the data to be enriched from MongoDB
		DocumentContext resourceContext = JsonPath.parse(resource.getMessage().getBody());
		List<List<Double>> rBids = resourceContext.read("$.data.bids");
		List<List<Double>> rAsks = resourceContext.read("$.data.asks");
		
		// Check checksum... 
		DecimalFormat bidsFormat = new DecimalFormat("0.0###"); 
		String checksumString = ""; 
		for (int i=0; i<Math.max(rBids.size(),rAsks.size()); i++) {
			if (i==0) {
				if(rBids.get(i) != null) {
					checksumString = checksumString + String.valueOf(rBids.get(i).get(0)) + ":" + bidsFormat.format(rBids.get(i).get(1));
				}
				if(rAsks.get(i) != null) {
					checksumString = checksumString + ":" + String.valueOf(rAsks.get(i).get(0)) + ":" + bidsFormat.format(rAsks.get(i).get(1));
				}
			}
			else {
				if(rBids.get(i) != null) {
					checksumString = checksumString + ":" + String.valueOf(rBids.get(i).get(0)) + ":" + bidsFormat.format(rBids.get(i).get(1));
				}
				if(rAsks.get(i) != null) {
					checksumString = checksumString + ":" + String.valueOf(rAsks.get(i).get(0)) + ":" + bidsFormat.format(rAsks.get(i).get(1));
				}
			}
			 
		}
		CRC32 crc = new CRC32(); 
		crc.update(checksumString.getBytes());
		System.out.println("Before String: " + checksumString);
		System.out.println("Before CS: " + crc.getValue());
		
		

		// Add or update the OrderBook with the bid updates
		for (List<Double> bid : bids) {			
			// Check if the bid is already in the list
			int index = getByKey(bid.get(0), rBids);
			if (index > -1) {
				// Remove from list is the update has a qty of zero
				if (bid.get(1).equals(0.0)) {
					rBids.remove(index);
				}
				// Otherwise update
				else {
					rBids.get(index).set(1, bid.get(1));
				} 
			}
			// If not in the list, add it 
			else {
				if (Double.compare(bid.get(1), 0.0)>0) rBids.add(bid);
			} 
		}
		
		// Add or update the OrderBook with the ask updates
		for (List<Double> ask : asks) {			
			// Check if the ask is already in the list
			int index = getByKey(ask.get(0), rAsks);
			if (index > -1) {
				// Remove from list is the update has a qty of zero
				if (ask.get(1).equals(0.0)) {
					rAsks.remove(index);
				}
				// Otherwise update
				else {
					rAsks.get(index).set(1, ask.get(1));
				} 
			}
			// If not in the list, add it 
			else {
				System.out.println(ask.get(1));
				if (Double.compare(ask.get(1), 0.0)>0) rAsks.add(ask);
			} 
		}
		
		// Sort bids ascending 
		rBids.sort((o1, o2) -> o1.get(0).compareTo(o2.get(0)));
		// Sore asks descending 
		rAsks.sort((o1, o2) -> o2.get(0).compareTo(o1.get(0)));
		
		// Remove > 100 from the bottom
		while(rBids.size()>100) rBids.remove(rBids.size()-1);
		while(rAsks.size()>100) rAsks.remove(rAsks.size()-1); 
		
		// Check checksum... 
		NumberFormatter nf = new NumberFormatter();
		

		checksumString = ""; 
		for (int i=0; i<Math.max(rBids.size(),rAsks.size()); i++) {
			if (i==0) {
				if(rBids.get(i) != null) {
					checksumString = checksumString + rBids.get(i).get(0) + ":" + rBids.get(i).get(1);
				}
				if(rAsks.get(i) != null) {
					checksumString = checksumString + rAsks.get(i).get(0) + ":" + rAsks.get(i).get(1);
				}
			}
			else {
				if(rBids.get(i) != null) {
					checksumString = checksumString + ":" + rBids.get(i).get(0) + ":" + rBids.get(i).get(1);
				}
				if(rAsks.get(i) != null) {
					checksumString = checksumString + ":" + rAsks.get(i).get(0) + ":" + rAsks.get(i).get(1);
				}
			}
			 
		}
		CRC32 crc32 = new CRC32(); 
		crc32.update(checksumString.getBytes());
		System.out.println("The CS: " + updateContext.read("$.data.checksum"));
		System.out.println("My CS: " + crc32.getValue());
		
		for (List<Double> rBid : rBids) {
			System.out.println(rBid);
		}
		
		
		
		//System.out.println(resource.getMessage().getBody());
		
		//ArrayList test = (ArrayList) resource.getMessage().getBody(List.class);
		//Object test = resource.getMessage().getBody();
		//LinkedHashMap map = (LinkedHashMap) test; 
		//System.out.println("Test: " + map.get("channel"));

		
		//System.out.println("Resource: " + resource.getMessage().getBody()); 
		return resource; 
	}
	
	public int getByKey(Double key, List<List<Double>> list) {
		for (int i=0; i<list.size(); i++) {
			if (list.get(i).get(0).equals(key)) return i;
		}
		return -1;
	}
}