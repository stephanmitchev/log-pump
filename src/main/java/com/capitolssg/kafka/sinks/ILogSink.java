package com.capitolssg.kafka.sinks;

public interface ILogSink {
	public void send(String key, String message);
	public void close();
}
