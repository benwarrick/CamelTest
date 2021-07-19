package CamelTest;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mongodb.MongoDbConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import CamelTest.beans.TestAggregationStrategy;
import CamelTest.beans.TestBean;
import com.mongodb.client.model.Filters; 

@Component
public class MySpringBootRouter extends RouteBuilder {
	
	@Override
	public void configure() {	
		AggregationStrategy aggregationStrategy = new TestAggregationStrategy(); 
		
		from("timer:myTimer?repeatCount=1")
		.setBody(simple("{\"channel\": \"orderbook\", \"market\": \"BTC/USD\", \"type\": \"update\", \"data\": {\"time\": 1626139758.986094, \"checksum\": 195176195, \"bids\": [[32965.0, 0.0], [33962.0, 3.4015]], \"asks\": [[34230.0, 3.7167], [31400.0, 0.0]], \"action\": \"update\"}}"))
		.setHeader(MongoDbConstants.CRITERIA, constant(Filters.eq("market", "BTC/USD")))
		.enrich("mongodb:mongo?database=k2_dev&collection=orderbooks&operation=findOneByQuery", aggregationStrategy)
		//.bean(TestBean.class, "getUpdatedOrderBook")
		.log("Test: ${body}")
		.to("mongodb:mongo?database=k2_dev&collection=orderbooks&operation=save");
		
		/*// Subscribe to channels
		from("timer:myTimer?repeatCount=1")
		.setBody(simple("{\"op\": \"subscribe\", \"channel\": \"trades\", \"market\": \"BTC/USD\"}"))
		.to("ahc-wss://ftx.us/ws/")
		//.setBody(simple("{\"op\": \"subscribe\", \"channel\": \"ticker\", \"market\": \"BTC/USD\"}"))
		//.to("ahc-wss://ftx.us/ws/")
		.setBody(simple("{\"op\": \"subscribe\", \"channel\": \"orderbook\", \"market\": \"BTC/USD\"}"))
		.to("ahc-wss://ftx.us/ws/");*/
		
		/*// Process messages
		from("ahc-wss://ftx.us/ws/")
		//.log("Message: ${body}")
		.choice()
			.when().jsonpath("$.[?(@.channel=='ticker')]",true)
				.log("Ticker: ${body}")
			.when().jsonpath("$.[?(@.channel=='orderbook')]",true)
				.log("OrderBook: ${body}")
			.otherwise()
				.log("Otherwise");*/
		
		/*// Ping to keep connection alive
		from("timer:timer2?fixedRate=true&period=15000")
		.setBody(simple("{\"op\": \"ping\"}"))
		.to("ahc-wss://ftx.us/ws/");*/
		
		/*from("timer:myTimer?repeatCount=1")
		.setBody(simple("${null}"))
		.toD("https://ftx.us/api/markets?httpMethod=GET&Accept=application/json")
		.log("Message:  ${body}")
		.bean(TestBean.class, "hello")
		.log("Test:  ${body}");*/
		
		/*from("jms:queue:k2.order.request::k2.order.request.CoinMetro")
			.log("Received a message - ${body}");*/
	
	
		/*from("timer:hello?period={{timer.period}}").routeId("hello")
            .transform().method("myBean", "saySomething")
            .filter(simple("${body} contains 'foo'"))
                .to("log:foo")
            .end()
            .to("stream:out");*/
	}
}
