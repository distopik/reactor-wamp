package com.distopik.wamp;

import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class Codecs {
	private Codecs() {}
	private static final ObjectMapper textMapper    = new ObjectMapper();
	private static final ObjectMapper msgpackMapper = new ObjectMapper(new MessagePackFactory());
	
	public static JsonNode readJson(String string) {
		try {
			return textMapper.readTree(string);
		} catch (Exception e) { throw new IllegalArgumentException("string", e); }
	}
	
	public static String writeJson(Message node) {
		return node.toJson().toString();
	}
	
	public static JsonNode readMsgpack(byte[] bytes) {
		try {
			return msgpackMapper.readTree(bytes);
		} catch (Exception e) { throw new IllegalArgumentException("bytes", e); }
	}
	
	public static byte[] writeMsgpack(Message msg) {
		try {
			return msgpackMapper.writeValueAsBytes(msg.toJson());
		} catch (JsonProcessingException e) { throw new IllegalArgumentException("msg", e); }
	}
}
