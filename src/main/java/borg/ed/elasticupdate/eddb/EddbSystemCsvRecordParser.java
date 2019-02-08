package borg.ed.elasticupdate.eddb;

import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EddbSystemCsvRecordParser
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public class EddbSystemCsvRecordParser implements CSVRecordParser<EddbSystem> {

	static final Logger logger = LoggerFactory.getLogger(EddbSystemCsvRecordParser.class);

	@Override
	public EddbSystem parse(CSVRecord record) {
		EddbSystem result = new EddbSystem();

		result.setId(CSVHelper.getAsLong(record.get("id")));
		result.setEdsm_id(CSVHelper.getAsLong(record.get("edsm_id")));
		result.setUpdated_at(CSVHelper.getAsDate(record.get("updated_at")));
		result.setName(CSVHelper.getAsString(record.get("name")));
		result.setX(CSVHelper.getAsFloat(record.get("x")));
		result.setY(CSVHelper.getAsFloat(record.get("y")));
		result.setZ(CSVHelper.getAsFloat(record.get("z")));
		result.setPopulation(CSVHelper.getAsBigDecimal(record.get("population")));
		result.setGovernment(CSVHelper.getAsString(record.get("government")));
		result.setAllegiance(CSVHelper.getAsString(record.get("allegiance")));
		result.setState(CSVHelper.getAsString(record.get("state")));
		result.setSecurity(CSVHelper.getAsString(record.get("security")));
		result.setPrimary_economy(CSVHelper.getAsString(record.get("primary_economy")));
		result.setReserve_type(CSVHelper.getAsString(record.get("reserve_type")));
		result.setNeeds_permit(CSVHelper.getAsBoolean(record.get("needs_permit")));

		return result;
	}

}
