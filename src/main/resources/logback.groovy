appender("STDOUT", ConsoleAppender) {
	encoder(PatternLayoutEncoder) {
	  pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{5} - %msg%n"
	}
  }
   
logger("org.eclipse.jetty.websocket", DEBUG)
logger("org.eclipse.jetty", INFO)
  root(DEBUG, ["STDOUT"])