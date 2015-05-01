package com.distopik.wamp;

public interface Invocation {
	boolean invoke(Arguments args, Notification notifyWithResults);
}
