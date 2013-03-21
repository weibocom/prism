package com.weibo.api.platform.prism.storm.core;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.log4j.Logger;

import com.weibo.api.platform.prism.storm.core.PrismScribeLog.DataType;

/**
 * 解析
 * @author crystal
 *
 */
public class ScribeLogScheme implements PrismScheme, Serializable {
	private static final long serialVersionUID = 7053901648969235963L;

	private static final Logger LOG = Logger.getLogger(ScribeLogScheme.class);
	
	@Override
	public PrismScribeLog deserialize(String msg) {
		String[] eles = StringUtils.split(msg, " ", 8);
		if(!validate(eles)){
			LOG.warn("not a validate prism scribe log:" + msg);
			return null;
		}
		try {
			PrismScribeLog psl = new PrismScribeLog();
			psl.setIp(parseIp(eles));
			psl.setDate(parseDate(eles));
			psl.setRequestID(parseRequestID(eles));
			psl.setModule(parseModule(eles));
			psl.setType(parseLogType(eles));
			psl.setDataType(parseDataType(eles));
			psl.setData(parseData(eles));
			
			return psl;
		} catch (Exception e) {
			LOG.error(
					"failed to deserialize message:\t" + msg + "\t"
							+ e.getMessage(), e);
		}
		return null;
	}

	private Object parseData(String[] eles) {
		return eles[7];
	}

	private DataType parseDataType(String[] eles) {
		return DataType.valueOf(Byte.parseByte(eles[6]));
	}

	private String parseLogType(String[] eles) {
		return eles[5];
	}

	private String parseModule(String[] eles) {
		return eles[4];
	}

	private String parseRequestID(String[] eles) {
		return eles[3];
	}

	String[] datePatterns = {"yyyy-MM-dd HH:mm:ss"};
	private Date parseDate(String[] eles) throws ParseException {
		String date = eles[1] + " " + eles[2];
		return DateUtils.parseDate(date, datePatterns);
	}

	private String parseIp(String[] eles) {
		String orgIP = eles[0];
		return StringUtils.substring(orgIP, 1, orgIP.length() - 1);
	}

	private boolean validate(String[] eles) {
		if(eles == null || eles.length < 8){
			return false;
		}
		return true;
	}
}
