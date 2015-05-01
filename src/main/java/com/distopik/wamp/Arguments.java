package com.distopik.wamp;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Arguments {
	ArrayNode  getArrayArguments();
	ObjectNode getKeywordArguments();
}
