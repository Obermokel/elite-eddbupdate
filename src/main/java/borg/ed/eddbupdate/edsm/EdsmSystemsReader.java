package borg.ed.eddbupdate.edsm;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import borg.ed.eddbupdate.eddndump.EddnBufferThread;
import borg.ed.universe.data.Coord;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.util.MiscUtil;

public class EdsmSystemsReader {

	static final Logger logger = LoggerFactory.getLogger(EdsmSystemsReader.class);

	private final Gson gson = new Gson();
	private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Autowired
	private EddnBufferThread eddnBufferThread = null;

	@SuppressWarnings("unchecked")
	public void loadEdsmDumpIntoElasticsearch() throws IOException, InterruptedException {
		File dumpFile = new File("X:\\Spiele\\Elite Dangerous\\systemsWithCoordinates.json");

		logger.info("Reading " + dumpFile + "...");

		//		Map<String, Coord> coordByMap = new HashMap<String, Coord>();

		int lineNumber = 0;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(dumpFile)), "UTF-8"))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				try {
					lineNumber++;
					if (lineNumber % 100_000 == 0) {
						logger.debug("Line " + lineNumber);
					}

					line = line.trim();
					if (line.endsWith(",")) {
						line = line.substring(0, line.length() - 1);
					}

					LinkedHashMap<String, Object> data = this.gson.fromJson(line, LinkedHashMap.class);
					@SuppressWarnings("unused")
					BigDecimal id = MiscUtil.getAsBigDecimal(data.remove("id"));
					@SuppressWarnings("unused")
					BigDecimal id64 = MiscUtil.getAsBigDecimal(data.remove("id64"));
					String name = MiscUtil.getAsString(data.remove("name"));
					Map<String, Number> coordsMap = (Map<String, Number>) data.remove("coords");
					String dateString = MiscUtil.getAsString(data.remove("date"));

					if (!data.isEmpty()) {
						logger.warn("Unknown attributes: " + data);
					} else {
						Coord coords = new Coord(MiscUtil.getAsFloat(coordsMap.get("x")), MiscUtil.getAsFloat(coordsMap.get("y")), MiscUtil.getAsFloat(coordsMap.get("z")));
						//						ZonedDateTime date = ZonedDateTime.ofInstant(this.df.parse(dateString).toInstant(), ZoneId.of("Z"));
						Date date = this.df.parse(dateString);

						//						FSDJumpEvent fsdJumpEvent = new FSDJumpEvent();
						//						fsdJumpEvent.setEvent("FSDJump");
						//						fsdJumpEvent.setTimestamp(date);
						//						fsdJumpEvent.setStarSystem(name);
						//						fsdJumpEvent.setStarPos(coords);
						//						fsdJumpEvent.setPopulation(BigDecimal.ZERO);
						//						fsdJumpEvent.setSystemGovernment("None");
						//						fsdJumpEvent.setSystemAllegiance("None");
						//						fsdJumpEvent.setFactionState("None");
						//						fsdJumpEvent.setSystemSecurity("Anarchy");
						//						fsdJumpEvent.setSystemEconomy("None");
						//
						//						String md5 = StarSystem.generateId(coords);
						//						Coord oldCoords = coordByMap.put(md5, coords);
						//						if (oldCoords != null) {
						//							logger.warn(md5 + ": " + name + " @ " + coords);
						//						}
						//
						//						this.eddnBufferThread.buffer(date, null, fsdJumpEvent);

						StarSystem starSystem = new StarSystem();
						starSystem.setId(StarSystem.generateId(coords));
						starSystem.setUpdatedAt(date);
						starSystem.setCoord(coords);
						starSystem.setName(name);
						starSystem.setPopulation(BigDecimal.ZERO);
						this.eddnBufferThread.bufferStarSystem(starSystem);
					}
				} catch (JsonSyntaxException | ParseException | NullPointerException e) {
					logger.error("Failed to parse line '" + line + "'", e);
				}
			}
		}
	}

}
