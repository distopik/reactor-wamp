package com.distopik.wamp;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

class Message implements Serializable {
	private static final long serialVersionUID = 1L;
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
	
	private int        type;
	private String     uri;
	private long       sessionId;
	private ObjectNode details;
	private long       requestId;
	private ArrayNode  arguments;
	private ObjectNode argumentsKeywords;
	private long       publicationId;
	private long       subscriptionId;
	private long       registrationId;
	
	
	public Message() {
	}
	
	public Message(JsonNode node) {
		MessageSpec.read(node, this);
	}
	
	public Message(int type, Message msg) {
		this(msg);
		this.type = type;
	}

	public Message(Message msg) {
		this.type      = msg.type;
		this.uri       = msg.uri;
		this.sessionId = msg.sessionId;
		this.details   = msg.details == null ? null : msg.details.deepCopy();
		this.requestId = msg.requestId;
		this.arguments = msg.arguments == null ? null : msg.arguments.deepCopy();
		this.argumentsKeywords = msg.argumentsKeywords == null ? null : msg.argumentsKeywords.deepCopy();
		this.publicationId     = msg.publicationId;
		this.subscriptionId    = msg.subscriptionId;
		this.registrationId    = msg.registrationId;
	}

	public static boolean removeNulls(Message node) {
		 return node != null;
	}
	
	public JsonNode toJson() {
		return MessageSpec.write(this);
	}
	
	public int getType() {
		return type;
	}
	
	public String getUri() {
		return uri;
	}

	public void set(SpecItem item, JsonNode node) {
		switch(item) {
		case MessageTypeId:
			setType(node.asInt());
			break;
		case URI:
			setUri(node.asText());
			break;
		case SessionId:
			setSessionId(node.asLong());
			break;
		case Details:
			setDetails((ObjectNode) node);
			break;
		case RequestId:
			setRequestId(node.asLong());
			break;
		case Arguments:
			setArguments((ArrayNode) node);
			break;
		case ArgumentsKeywords:
			setArgumentsKeywords((ObjectNode) node);
			break;
		case PublicationId:
			setPublicationId(node.asLong());
			break;
		case SubscriptionId:
			setSubscriptionId(node.asLong());
			break;
		case RegistrationId:
			setRegistrationId(node.asLong());
		}
	}
	
	JsonNode get(SpecItem item) {
		switch(item) {
		case MessageTypeId:
			return JsonNodeFactory.instance.numberNode(getType());
		case URI:
			return JsonNodeFactory.instance.textNode(getUri());
		case SessionId:
			return JsonNodeFactory.instance.numberNode(getSessionId());
		case Details:
			return getDetails();
		case RequestId:
			return JsonNodeFactory.instance.numberNode(getRequestId());
		case Arguments:
			return getArguments();
		case ArgumentsKeywords:
			return getArgumentsKeywords();
		case PublicationId:
			return JsonNodeFactory.instance.numberNode(getPublicationId());
		case SubscriptionId:
			return JsonNodeFactory.instance.numberNode(getSubscriptionId());
		case RegistrationId:
			return JsonNodeFactory.instance.numberNode(getRegistrationId());
		default:
			return null;
		}
	}

	public long getSessionId() {
		return sessionId;
	}

	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}

	public long getRequestId() {
		return requestId;
	}

	public void setRequestId(long requestId) {
		this.requestId = requestId;
	}

	public ArrayNode getArguments() {
		return arguments;
	}

	public void setArguments(ArrayNode arguments) {
		this.arguments = arguments;
	}

	public ObjectNode getArgumentsKeywords() {
		return argumentsKeywords;
	}

	public void setArgumentsKeywords(ObjectNode argumentsKeywords) {
		this.argumentsKeywords = argumentsKeywords;
	}

	public long getPublicationId() {
		return publicationId;
	}

	public void setPublicationId(long publicationId) {
		this.publicationId = publicationId;
	}

	public void setType(int type) {
		this.type = type;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public void setDetails(ObjectNode details) {
		this.details = details;
	}
	
	public ObjectNode getDetails() {
		return details;
	}
	
	public long getSubscriptionId() {
		return subscriptionId;
	}
	
	public void setSubscriptionId(long subscriptionId) {
		this.subscriptionId = subscriptionId;
	}
	
	public long getRegistrationId() {
		return registrationId;
	}
	
	public void setRegistrationId(long registrationId) {
		this.registrationId = registrationId;
	}

	public Message deepCopy() {
		return new Message(getType(), this);
	}
}