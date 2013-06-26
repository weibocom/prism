package com.weibo.api.prism.storm.core;

import java.io.Serializable;
import java.util.Date;

import org.apache.log4j.Logger;

public class PrismScribeLog implements Serializable {
	private static final long serialVersionUID = 4824130067195672095L;
	private static final Logger LOG = Logger.getLogger(PrismScribeLog.class);
	
	// 生成日志的ip（也可能是域名）
	private String ip;
	
	// 日志的生成时间
	private Date date;
	
	// 日志请求id（该标识日志来源于哪个业务请求）
	private String requestID;
	
	// 模块，即业务模块
	private String module;
	
	// 日志类型（包含 access日志、useTime日志等）
	private String schema;
	
	// 数据类型（即实际输出的日志内容的数据格式，有string, json, pb等
	private DataType dataType;
	
	private Object data;//数据具体的类型取决于dataType。
	
	/**
	 * @return the ip
	 */
	public String getIp() {
		return ip;
	}

	/**
	 * @param ip the ip to set
	 */
	public void setIp(String ip) {
		this.ip = ip;
	}

	/**
	 * @return the date
	 */
	public Date getDate() {
		return date;
	}



	/**
	 * @param date the date to set
	 */
	public void setDate(Date date) {
		this.date = date;
	}

	/**
	 * @return the requestID
	 */
	public String getRequestID() {
		return requestID;
	}

	/**
	 * @param requestID the requestID to set
	 */
	public void setRequestID(String requestID) {
		this.requestID = requestID;
	}

	/**
	 * @return the module
	 */
	public String getModule() {
		return module;
	}


	/**
	 * @param module the module to set
	 */
	public void setModule(String module) {
		this.module = module;
	}

	/**
	 * @return the type
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.schema = type;
	}

	/**
	 * @return the dataType
	 */
	public DataType getDataType() {
		return dataType;
	}

	/**
	 * @param dataType the dataType to set
	 */
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}


	/**
	 * @return the data
	 */
	public Object getData() {
		return data;
	}

	/**
	 * @param data the data to set
	 */
	public void setData(Object data) {
		this.data = data;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		String SPLIT = " ";
		sb.append(ip).append(SPLIT).append(date).append(SPLIT).append(requestID).append(SPLIT).append(module).append(SPLIT).append(schema).append(SPLIT).append(dataType).append(SPLIT).append(data);
		return sb.toString();
	}



	public static enum DataType {
		UNKOWN,
		STRING,
		JSON,
		PB;
		
		public static DataType valueOf(byte t){
			switch (t) {
			case 0:
				return STRING;
			case 1:
				return JSON;
			case 2:
				return PB;
			default:
				LOG.warn("unknown data type:[" + t + "].");
				return UNKOWN;
			}
		}
	}
}
