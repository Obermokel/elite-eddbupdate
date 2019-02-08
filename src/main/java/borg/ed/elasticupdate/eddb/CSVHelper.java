package borg.ed.elasticupdate.eddb;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSVHelper
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public abstract class CSVHelper {

	static final Logger logger = LoggerFactory.getLogger(CSVHelper.class);

	public static String getAsString(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return value;
		}
	}

	public static Boolean getAsBoolean(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return "1".equals(value) || "true".equals(value);
		}
	}

	public static Integer getAsInt(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return Integer.valueOf(value);
		}
	}

	public static Long getAsLong(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return Long.valueOf(value);
		}
	}

	public static Float getAsFloat(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return Float.valueOf(value);
		}
	}

	public static Double getAsDouble(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return Double.valueOf(value);
		}
	}

	public static BigDecimal getAsBigDecimal(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return new BigDecimal(value);
		}
	}

	public static Date getAsDate(String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		} else {
			return new Date(1000L * Long.valueOf(value));
		}
	}

}
