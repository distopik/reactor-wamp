package com.distopik.wamp;

import static com.distopik.wamp.Message.*;
import static com.distopik.wamp.SpecItem.*;
import static com.fasterxml.jackson.databind.node.JsonNodeType.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

public class MessageSpec {
	private SpecItem[] items;
	private boolean[]  optional;
	
	private static final JsonNodeType[] expectedNodeTypes = new JsonNodeType[SpecItem.values().length]; 
	static {
		expectedNodeTypes[MessageTypeId.ordinal()]     = NUMBER;
		expectedNodeTypes[URI.ordinal()]               = STRING;
		expectedNodeTypes[Session.ordinal()]           = NUMBER;
		expectedNodeTypes[Details.ordinal()]           = OBJECT;
		expectedNodeTypes[RequestType.ordinal()]       = NUMBER;
		expectedNodeTypes[RequestId.ordinal()]         = NUMBER;
		expectedNodeTypes[Arguments.ordinal()]         = ARRAY;
		expectedNodeTypes[ArgumentsKeywords.ordinal()] = OBJECT;
		expectedNodeTypes[PublicationId.ordinal()]     = NUMBER;
		expectedNodeTypes[SubscriptionId.ordinal()]    = NUMBER;
		expectedNodeTypes[Topic.ordinal()]             = STRING;
	}
	
	public static final MessageSpec[] SPECS = new MessageSpec[LARGEST_MESSAGE_ID];
	static {
		SPECS[HELLO]        = new MessageSpec(MessageTypeId, URI, Details);
		SPECS[WELCOME]      = new MessageSpec(MessageTypeId, Session, Details);
		SPECS[ABORT]        = new MessageSpec(MessageTypeId, Details, URI);
		SPECS[GOODBYE]      = new MessageSpec(MessageTypeId, Details, URI); 
		SPECS[ERROR]        = new MessageSpec(MessageTypeId, RequestId, Details, URI, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
		SPECS[PUBLISH]      = new MessageSpec(MessageTypeId, RequestId, Details, URI, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
		SPECS[PUBLISHED]    = new MessageSpec(MessageTypeId, RequestId, PublicationId);
		SPECS[SUBSCRIBE]    = new MessageSpec(MessageTypeId, RequestId, Details, URI);
		SPECS[SUBSCRIBED]   = new MessageSpec(MessageTypeId, RequestId, SubscriptionId);
		SPECS[UNSUBSCRIBE]  = new MessageSpec(MessageTypeId, RequestId, SubscriptionId);
		SPECS[EVENT]        = new MessageSpec(MessageTypeId, SubscriptionId, PublicationId, Details, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
		SPECS[CALL]         = new MessageSpec(MessageTypeId, RequestId, Details, URI, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
		SPECS[RESULT]       = new MessageSpec(MessageTypeId, RequestId, Details, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
		SPECS[REGISTER]     = new MessageSpec(MessageTypeId, RequestId, Details, URI);
		SPECS[REGISTERED]   = new MessageSpec(MessageTypeId, RequestId, RegistrationId);
		SPECS[UNREGISTER]   = new MessageSpec(MessageTypeId, RequestId, RegistrationId);
		SPECS[UNREGISTERED] = new MessageSpec(MessageTypeId, RequestId);
		SPECS[INVOCATION]   = new MessageSpec(MessageTypeId, RequestId, RegistrationId, Details, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
		SPECS[YIELD]        = new MessageSpec(MessageTypeId, RequestId, Details, Arguments, ArgumentsKeywords).setOptionalFrom(Arguments);
	};
	
	private MessageSpec(SpecItem... items) {
		this.items = items;
		optional = new boolean[items.length];
	}
	
	private MessageSpec setOptionalFrom(SpecItem what) {
		for (int idx = items.length-1; idx > 0; idx--) {
			if (items[idx] == what) {
				for (int i = idx; i < items.length; i++)
					optional[i] = true;
				break;
			}
		}
		
		return this;
	}
	
	public static boolean read(JsonNode source, Message destination) {
		if (!source.isArray())
			return false;
		
		int messageTypeId = source.get(0).asInt();
		
		if (messageTypeId > SPECS.length || SPECS[messageTypeId] == null) {
			return false; /* unknown message */
		}
		
		return SPECS[messageTypeId].internalRead(source, destination);
	}
	
	private boolean internalRead(JsonNode source, Message destination) {
		for (int index = 0; index < items.length; index++) {
			if (!source.has(index)) {
				return optional[index];
			}
			JsonNode value = source.get(index);
			SpecItem type  = items[index];
			if (value.getNodeType() != expectedNodeTypes[type.ordinal()]) {
				return false;
			}
			
			destination.set(type, value);
			index++;
		}
		
		return true;
	}
}