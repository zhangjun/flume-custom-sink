/**
 * 
 */
package com.weibo.search.flume.sink.MCQ;

import java.io.OutputStream;

import org.apache.flume.serialization.EventSerializer;

/**
 * Title: OutputStreamWrapper.java
 * @author ewalker.zj@gmail.com
 * @date 2017Äê2ÔÂ27ÈÕ
 */
public class OutputStreamWrapper {

	/**
	 * 
	 */
	private OutputStream outputStream;
	private String dateTime;
	private EventSerializer serializer;
	/**
	 * @return the outputStream
	 */
	public OutputStream getOutputStream() {
		return outputStream;
	}
	/**
	 * @param outputStream the outputStream to set
	 */
	public void setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
	}
	/**
	 * @return the dateTime
	 */
	public String getDateTime() {
		return dateTime;
	}
	/**
	 * @param dateTime the dateTime to set
	 */
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}
	/**
	 * @return the serializer
	 */
	public EventSerializer getSerializer() {
		return serializer;
	}
	/**
	 * @param serializer the serializer to set
	 */
	public void setSerializer(EventSerializer serializer) {
		this.serializer = serializer;
	}
	
	
	
	

}
