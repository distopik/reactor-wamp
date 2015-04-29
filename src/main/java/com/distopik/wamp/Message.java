package com.distopik.wamp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

class Message {
	long     type;
	String   uri;
	JsonNode payload;
	
	public static final int HELLO        = 1;
	public static final int WELCOME      = 2;
	public static final int ABORT        = 3;
	public static final int CHALLENGE    = 4;
	public static final int AUTHENTICATE = 5;
	public static final int GOODBYE      = 6;
	
	public static final int SUBSCRIBE    = 32;
	public static final int UNSUBSCRIBE  = 32;
	public static final int EVENT        = 36;
	
	
	public static boolean verifyFormat(JsonNode node) {
		return node.isArray() && node.size() >= 2 && node.get(0).isIntegralNumber();
	}
	
	public Message(JsonNode node) {
		/* very much not true ... fix at some point */
		this.type    = node.get(0).asLong();
		this.uri     = node.get(1).asText();
		this.payload = node.get(2);
	}
	
	public Message(int type, String uri, JsonNode payload) {
		this.type    = type;
		this.uri     = uri;
		this.payload = payload;
	}
	
	public static Message create(JsonNode node) {
		return new Message(node);
	}
	
	public JsonNode toJson() {
		ArrayNode rv = JsonNodeFactory.instance.arrayNode();
		rv.add(type);
		if (uri != null)
			rv.add(uri);
		if (payload != null)
			rv.add(payload);
		
		return rv;
	}
	
	public long getType() {
		return type;
	}
	
	public String getUri() {
		return uri;
	}
}