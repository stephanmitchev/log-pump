package com.capitolssg.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.capitolssg.kafka.sinks.ILogSink;
import com.capitolssg.kafka.sinks.KafkaSink;

public class LogPump {
	
	private static DateTime logLastDate = null;
	private static long interval = 0;
	private static float flowRate = 1;
	
	public static void main(String[] args) {
		
		// Usage
		if (args.length < 3 || args.length > 5) {
			System.out.print("Usage:\n"
					+ "java jar log-pump....jar logFile kafkaBrokerList kafkaTopic [rate_multiplier]");
			System.exit(1);
		}
		
		// load file
		File file = new File(args[0]);
		if (!file.exists() || file.isDirectory()) {
			System.out.print("You must provide path to a valid log file");
			System.exit(1);
		}
		
		// Use default pattern and format
		Pattern timePattern = Pattern.compile("^(\\d{4}\\-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2},\\d{3}).*");
		DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS");
		
		// Create the sink
		ILogSink sink = new KafkaSink(args[1], args[2], "kafka.serializer.StringEncoder", "com.capitolssg.kafka.tools.SimplePartitioner", false);
		
		// Control rate of flow
		if (args.length == 4)  {
			try {
				flowRate = Float.parseFloat(args[3]);
			}
			catch (NumberFormatException | NullPointerException e){
				e.printStackTrace();
			}
		}
		
		try (Scanner scanner = new Scanner(file)) {
			long i = 0;
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				Matcher m = timePattern.matcher(line);
		
				if (m.find()) {
			    	DateTime date = dateFormat.parseDateTime(m.group(1));
			    	interval =  logLastDate == null ? 0 : date.getMillis() - logLastDate.getMillis();
			    	// Fix anomalies
			    	interval = (long) Math.max(0, interval / flowRate);
			    	logLastDate = date;
			    	
			    	Thread.sleep(interval);
			    	sink.send(String.valueOf(date.getMillis()), line);
			    	
			    	// Print something visual
			    	if (i++ % 1000 == 1) {
			    		System.out.print('.');
			    	}
			    }
			}
	 
			scanner.close();
			
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			sink.close();	 
		}
	}
}
