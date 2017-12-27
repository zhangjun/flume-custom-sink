package com.weibo.search.flume.sink.MCQ;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
//import com.danga.MemCached.MemCachedClient;
//import com.danga.MemCached.SockIOPool;
import com.google.code.yanf4j.core.impl.StandardSocketOption;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;

import com.google.gson.internal.*;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.event.*;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.conf.Configurable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.text.SimpleDateFormat;

/**
 * Title: MCQSink.java
 * @author ewalker.zj@gmail.com
 * @date 2017Äê2ÔÂ8ÈÕ
 */
public class MCQSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(MCQSink.class);

	private static final int defaultBatchSize = 128;

	private static final String DEFAULT_FILE_PREFIX = "";

	private static final String DEFAULT_FILE_EXTENSION = "";
	
	private int batchSize = defaultBatchSize;
	
	private String host;
	private int port;
	private int timeout;
	private String queueName;

	//private SockIOPool pool;

	private String server;

	private MemcachedClient mcc;

	private String defaultFileds = null;

	private List<String> fieldsVal;
    private String fields;

	private Boolean isWriteFile;

	private String filePrefix;

	private File filePath;

	private String serializerType;

	private SinkCounter sinkCounter;

	private ConcurrentMap<String, OutputStreamWrapper> outputStreamManager = Maps.newConcurrentMap();

	private Context serializerContext;

	private String extension;

	private Boolean isTest;

	private int retryTimes = 3;

	//private String charset;
	
	public MCQSink() {
		
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		
		LinkedHashMap<String, Object> mapData = new LinkedHashMap<String, Object>();
		String sendBody;
		
	
		try {
			
			transaction.begin();
			int eventAttemptCounter = 0;
			OutputStreamWrapper outputStreamWrapper = null;
			
			for (int i = 0; i < batchSize; i ++){
				event = channel.take();
				
				if ( event == null){
					
					status = Status.BACKOFF;
					break;
					
				}else{
					//String dateTime = null;
					sinkCounter.incrementEventDrainAttemptCount();
					eventAttemptCounter++;
					
					//byte[] data = event.getBody();
					String data = new String(event.getBody());
					String msgBody = data;
					// String msgBody = data.substring(3);
					//logger.info("msgBody {}" + msgBody);
					
					if(this.isWriteFile){
						
						//debug, record delay
						String toFile = data;
						if(this.isTest){
							Date dt = new Date();
							SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							String nowtime = format.format(dt);
							toFile = msgBody + nowtime;
						}
						
						Event eventData = EventBuilder.withBody(toFile.getBytes());
						//String timestamp = event.getHeaders().get("timestamp");
						//SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						SimpleDateFormat hourFormat = new SimpleDateFormat("yyyyMMddHH");
						//String hourTime = hourFormat.format(secondFormat.parse(timestamp));
						Date date = null;
						
						try{
							date = getDate(event.getHeaders());
							String hourTime = hourFormat.format(date);
							
							//logger.info("timestamp {}", timestamp);
							//logger.info("timeExtra {}", hourTime);
							
							outputStreamWrapper = outputStreamManager.get(hourTime);
							if(outputStreamWrapper == null){
								outputStreamWrapper = createOutputStreamWrapper(hourTime);
								outputStreamManager.put(hourTime, outputStreamWrapper);
							}
							outputStreamWrapper.getSerializer().write(eventData);
							
						}catch(InputNotSpecifiedException ex){
							// ignore
							String errorMsg = "timestamp cannot be parsed, msg is {}" ;
							logger.error(errorMsg,  new String(event.getBody()));
						}
						
					}
					
					
					if(fieldsVal != null){
//						Gson gson = new GsonBuilder().disableHtmlEscaping().create();
//						JSONObject res;
						try{
							JSONObject res = JSON.parseObject(msgBody);
							for ( String field : fieldsVal){
								//logger.info("field data {}" + res.get(field));
								mapData.put(field, res.get(field));					
							}
							//Gson gson = new Gson();
							
							sendBody = JSON.toJSONString(mapData);
						} catch(Exception e){
							logger.warn("Event body is not  JSON format. ");
							sendBody = msgBody;
						}
						
					 //   LinkedHashMap<String, Object>  res = gson.fromJson(msgBody, LinkedHashMap.class);	
						//JsonReader reader = new JsonReader(new StringReader(msgBody));
						//JsonObject res = new JsonParser().parse(reader).getAsJsonObject();
						
						
						
					} else {
						sendBody = msgBody;
					}
					
					//logger.info("sendBody {}" + sendBody);
					
					// time test
//					Date dt = new Date();
//					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//					String nowtime = format.format(dt);
//					sendBody = sendBody + nowtime;
//					String timestamp = event.getHeaders().get("timestamp");
//					SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//					String logtime = secondFormat.format(timestamp);
//					logger.info("now time {}, logtime {}" ,  nowtime, logtime);
					int retryTimes = getRetryTimes();
					
					while(retryTimes > 0){
						
						try {
							boolean res = mcc.set(queueName, 0, sendBody);
							if(res == true){
								break;
							}
						}catch(MemcachedException e){
							String errMsg = "Memcached set failed, retry ...";
							logger.error(errMsg);
						}
						
						retryTimes --;
						TimeUnit.MILLISECONDS.sleep(500);
						
					}  // end while
					
				}  // end if
				
			}   // end for 
			
			if(outputStreamWrapper != null){
				outputStreamWrapper.getSerializer().flush();
				outputStreamWrapper.getOutputStream().flush();
			}

			transaction.commit();
			sinkCounter.addToEventDrainAttemptCount(eventAttemptCounter);
			
//		}catch(MemcachedException ex){
			
		}catch(InputNotSpecifiedException ex){
			// parsed timestamp failded£¬ ignore Error 
//			transaction.rollback();
//			String errorMsg = "timestamp cannot be parsed, msg is {}" ;
//			logger.error(errorMsg,  new String(event.getBody()));
			
		}catch (Exception ex) {
			
			transaction.rollback();
			String errorMsg = "Failed to process transaction";
			status = Status.BACKOFF;
			//logger.error(errorMsg);
			
			//  debug info 
			logger.error(new String(event.getBody()));
			
			throw new EventDeliveryException(errorMsg, ex);	
			
		} finally {
			
			transaction.close();
			
		}
		
		return status;
	}


	private int getRetryTimes() {
		// TODO Auto-generated method stub
		return retryTimes;
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.sink.AbstractSink#start()
	 */
	@Override
	public synchronized void start() {
		logger.info("Starting MCQ Sink {}", this.getName());
		sinkCounter.start();
		
		logger.info("connecting to "+ server);

		MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(server));
		builder.setConnectionPoolSize(10);
		builder.setSocketOption(StandardSocketOption.SO_SNDBUF, 32*1024);
		builder.setSocketOption(StandardSocketOption.TCP_NODELAY, false);
		builder.getConfiguration().setSessionIdleTimeout(10000);
		try {
			mcc = builder.build();
			mcc.setEnableHeartBeat(false);
			mcc.setOpTimeout(5000L);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.info("connect to MCQ failed.");
		}
		
		
		
		
//	java_memcached		
//		pool = SockIOPool.getInstance();
//		pool.setServers(new String[]{server});
//		pool.setInitConn(100);
//		pool.setMinConn(100);
//		pool.setMaxConn(1000);
//		pool.setMaintSleep(30);
//		pool.setMaxIdle(1000*60*60);
//		pool.setNagle(true);
//		pool.setSocketTO(60);
//		pool.setSocketConnectTO(0);
//		
//		pool.initialize();
//		
//		mcc = new MemCachedClient();
		// TODO Auto-generated method stub
		super.start();
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.sink.AbstractSink#stop()
	 */
	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		logger.info("Stopping MCQ sink {}", this.getName());
		//pool.shutDown();
		try {
			mcc.shutdown();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			logger.info("MCQ shut down failed.");
		}
		sinkCounter.stop();
		Collection<OutputStreamWrapper> outputStreamWrapperCollection = outputStreamManager.values();
		
		if(outputStreamWrapperCollection != null){
			for ( OutputStreamWrapper outputStreamWrapper : outputStreamWrapperCollection){
				try {
					destroyOutputStreamWrapper(outputStreamWrapper);
				} catch (EventDeliveryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		super.stop();
	}


	@Override
	public void configure(Context context) {
		// TODO Auto-generated method stub
		host = context.getString("host", "10.73.12.142");
		port = context.getInteger("port", 11233);
		queueName = context.getString("queuename", "querylog");
		//charset = context.getString("charset" , "unicode");
		batchSize = context.getInteger("batchSize", defaultBatchSize);
		server = host + ":" + port;
		
		fields = context.getString("fields", defaultFileds);
		isWriteFile = context.getBoolean("isWriteFile", false);
		isTest = context.getBoolean("isTest", false);
		//filePrefix =  context.getString("filePrefix", DEFAULT_FILE_PREFIX);
		
		if( isWriteFile == true){
			filePrefix =  Preconditions.checkNotNull(context.getString("filePrefix"), "file prefix is required to avoid write conflict.");
			String filePath = Preconditions.checkNotNull(context.getString("filePath"), "filePath is required.");
			
			this.filePath = new File(filePath);
			serializerType = context.getString("sink.serializer", "TEXT");
			serializerContext = new Context(context.getSubProperties("sink." + EventSerializer.CTX_PREFIX));
			extension  = context.getString("extension", DEFAULT_FILE_EXTENSION);
			
			Preconditions.checkNotNull(serializerType, "Serializer type is undefined");
			if(!this.filePath.exists()){
				if(!this.filePath.mkdirs()){
					throw new IllegalArgumentException("filePath is not a directory");
				}
			}else if(!this.filePath.canWrite()){
				throw new IllegalArgumentException("directory can not write");
			}
			
		}
		
		
		if(fields != null){
			fieldsVal = Arrays.asList(fields.split(","));
		}
		
		if(queueName == null) {
			throw new RuntimeException("Queue Name is not set");
		}
		
		
		if(sinkCounter == null){
			sinkCounter = new SinkCounter(getName());
		}
		
		logger.info("MCQ Sink Configured");
		
	}
	
	private OutputStreamWrapper createOutputStreamWrapper(String dateTime) throws EventDeliveryException {
		OutputStreamWrapper outputStreamWrapper = new OutputStreamWrapper();
		File currFile = getFileByTime(dateTime);
		logger.debug("opening output stream for file {}", currFile);
		try {
			
			OutputStream outputStream = new BufferedOutputStream(
					new FileOutputStream(currFile, true));
			EventSerializer serializer = EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);
			serializer.afterCreate();
			outputStreamWrapper.setOutputStream(outputStream);
			outputStreamWrapper.setSerializer(serializer);
			outputStreamWrapper.setDateTime(dateTime);
			sinkCounter.incrementConnectionCreatedCount();
			
		} catch (IOException e) {
			sinkCounter.incrementConnectionFailedCount();
			throw new EventDeliveryException("Failed to open file " + getFileByTime(dateTime) + " while delivering event", e);
		}
		return outputStreamWrapper;
	}

	
	private void destroyOutputStreamWrapper(OutputStreamWrapper outputStreamWrapper) throws EventDeliveryException {
		if(outputStreamWrapper.getOutputStream() != null){
			logger.debug("Closing file {}", getFileByTime(outputStreamWrapper.getDateTime()));
			
			try {
				
				outputStreamWrapper.getSerializer().flush();
				outputStreamWrapper.getSerializer().beforeClose();
				outputStreamWrapper.getOutputStream().close();
				sinkCounter.incrementConnectionClosedCount();
				
			} catch (IOException e) {
				
				sinkCounter.incrementConnectionFailedCount();
				throw new EventDeliveryException("Unable to close file" + getFileByTime(outputStreamWrapper.getDateTime()) + " while delivering event", e);
			} finally {
				outputStreamWrapper.setOutputStream(null);
				outputStreamWrapper.setSerializer(null);
			}
			
			outputStreamWrapper = null;
			
		}
		
	}
	
	
	private Date getDate(Map<String, String> headers) {
		String timestamp = headers.get("timestamp");
		Date date = null;
		if(StringUtils.isEmpty(timestamp)){
			throw new InputNotSpecifiedException("timestamp cannot be found in the Event Header.");
		}
		
		SimpleDateFormat secondFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try{
			date = secondFormat.parse(timestamp);
		}catch(Exception e){
			throw new InputNotSpecifiedException("timestamp cannot be parsed in Event Header.");
		}
		return date;
	}
	
	private File getFileByTime(String dateTime) {
		String dayTime = dateTime.substring(0, 8);
		File dataPath = new File(filePath, dayTime);
		if(!dataPath.exists()){
			dataPath.mkdir();
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append(this.filePrefix).append("_");
		sb.append(dateTime);
		if(extension.length() > 0){
			sb.append(".").append(extension);
		}
		File currFile = new File(dataPath, sb.toString());
		return currFile;
	}

	
}