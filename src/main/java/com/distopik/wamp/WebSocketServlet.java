package com.distopik.wamp;

import java.util.concurrent.Executors;

import org.eclipse.jetty.io.MappedByteBufferPool;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import reactor.Environment;

public class WebSocketServlet extends org.eclipse.jetty.websocket.servlet.WebSocketServlet {
	private static final long serialVersionUID = 1L;
	private static final String WAMP_JSON_V2    = "wamp.2.json";
	private static final String WAMP_MSGPACK_V2 = "wamp.2.msgpack";

	@Override
	public void configure(WebSocketServletFactory factory) {
		factory.setCreator((req, resp) -> {
			if (req.getSubProtocols().contains(WAMP_JSON_V2)) {
				resp.setAcceptedSubProtocol(WAMP_JSON_V2);
				return WebSocketAdapter.createForText(Codecs::readJson, Codecs::writeJson);
			} else if (req.getSubProtocols().contains(WAMP_MSGPACK_V2)) {
				resp.setAcceptedSubProtocol(WAMP_MSGPACK_V2);
				return WebSocketAdapter.createForBytes(Codecs::readMsgpack, Codecs::writeMsgpack);
			} else {
				resp.setSuccess(false);
				return null;
			}
		});
	}
	
	public static void main(String[] args) throws Exception {
		Environment.initialize();
		WebSocketServlet servlet = new WebSocketServlet();
		
		ServletHandler handler = new ServletHandler();
		handler.addServletWithMapping(new ServletHolder(servlet), "/");

		HttpConfiguration httpConfig = new HttpConfiguration();
		httpConfig.setOutputBufferSize(32 * 1024);
		httpConfig.setRequestHeaderSize(8 * 1024);
		httpConfig.setResponseHeaderSize(8 * 1024);
		httpConfig.setSendDateHeader(true);

		HttpConnectionFactory connFac = new HttpConnectionFactory(httpConfig);

		final Server server = new Server(9000);

		try (ServerConnector connector = new ServerConnector(server,
				Executors.newFixedThreadPool(4),
				new ScheduledExecutorScheduler(), new MappedByteBufferPool(),
				1, 4, connFac)) {
			connector.setAcceptQueueSize(1000);
			connector.setReuseAddress(true);
		}

		server.setHandler(handler);
		server.start();
		System.in.read();
		server.stop();
	}
}
