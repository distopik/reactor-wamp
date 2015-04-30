package com.distopik.wamp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

class Message {
	public static final int HELLO        = 1;
	public static final int WELCOME      = 2;
	public static final int ABORT        = 3;
	public static final int CHALLENGE    = 4;
	public static final int AUTHENTICATE = 5;
	public static final int GOODBYE      = 6;
	
	public static final int ERROR        = 8;
	
	public static final int PUBLISH      = 16;
	public static final int PUBLISHED    = 17;
	
	
	public static final int SUBSCRIBE    = 32;
	public static final int SUBSCRIBED   = 33;
	public static final int UNSUBSCRIBE  = 34;
	public static final int UNSUBSCRIBED = 35;
	public static final int EVENT        = 36;
	
	public static final int CALL         = 48;
	public static final int CANCEL       = 49;
	public static final int RESULT       = 50;
	
	public static final int REGISTER     = 64;
	public static final int REGISTERED   = 65;
	public static final int UNREGISTER   = 66;
	public static final int UNREGISTERED = 67;
	public static final int INVOCATION   = 68;
	public static final int INTERRUPT    = 69;
	public static final int YIELD        = 70;
	
	
	public static final int LARGEST_MESSAGE_ID = YIELD;
	
	private long     type;
	private String   uri;
	private JsonNode payload;
	
	public static boolean removeNulls(Message node) {
		 return node != null;
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

	public void set(SpecItem item, JsonNode node) {
	}
}