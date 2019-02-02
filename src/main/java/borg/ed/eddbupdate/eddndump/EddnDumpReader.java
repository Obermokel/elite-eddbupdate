package borg.ed.eddbupdate.eddndump;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import borg.ed.universe.eddn.EddnElasticUpdater;
import borg.ed.universe.journal.JournalEventReader;
import borg.ed.universe.journal.events.AbstractJournalEvent;

/**
 * EddnDumpReader
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public class EddnDumpReader {

	static final Logger logger = LoggerFactory.getLogger(EddnDumpReader.class);

	private static final String SCHEMA_JOURNAL_v1 = "https://eddn.edcd.io/schemas/journal/1";
	private static final String SCHEMA_JOURNAL_v1_TEST = "https://eddn.edcd.io/schemas/journal/1/test";
	private static final String SCHEMA_COMMODITY_v3 = "https://eddn.edcd.io/schemas/commodity/3";
	private static final String SCHEMA_COMMODITY_v3_TEST = "https://eddn.edcd.io/schemas/commodity/3/test";
	private static final String SCHEMA_BLACKMARKET_v1 = "https://eddn.edcd.io/schemas/blackmarket/1";
	private static final String SCHEMA_SHIPYARD_v2 = "https://eddn.edcd.io/schemas/shipyard/2";
	private static final String SCHEMA_SHIPYARD_v2_TEST = "https://eddn.edcd.io/schemas/shipyard/2/test";
	private static final String SCHEMA_OUTFITTING_v2 = "https://eddn.edcd.io/schemas/outfitting/2";
	private static final String SCHEMA_OUTFITTING_v2_TEST = "https://eddn.edcd.io/schemas/outfitting/2/test";

	private final Gson gson = new Gson();

	@Autowired
	private JournalEventReader journalEventReader = null;

	@Autowired
	private EddnBufferThread eddnBufferThread = null;

	@Autowired
	private EddnElasticUpdater eddnElasticUpdater = null;

	public void loadEddnDumpsIntoElasticsearch() {
		this.eddnBufferThread.start();

		this.eddnElasticUpdater.setUpdateMinorFactions(false);

		this.readDumpsFromDir(new File("X:\\Spiele\\Elite Dangerous\\eddndump_until_3_3"));
		this.readDumpsFromDir(new File("X:\\Spiele\\Elite Dangerous\\eddndump_since_3_3"));
		this.readDumpsFromDir(new File(System.getProperty("user.home"), "eddndump"));
	}

	private void readDumpsFromDir(File eddnDumpDir) {
		if (eddnDumpDir.exists() && eddnDumpDir.isDirectory()) {
			File[] dumpFiles = eddnDumpDir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File file) {
					return file.getName().startsWith("eddndump") && file.getName().endsWith(".txt");
				}
			});
			Arrays.sort(dumpFiles, new Comparator<File>() {
				@Override
				public int compare(File f1, File f2) {
					return f1.getName().compareTo(f2.getName());
				}
			});
			for (File dumpFile : dumpFiles) {
				try {
					readDumpFile(dumpFile);
				} catch (IOException e) {
					logger.error("Failed to read " + dumpFile, e);
				}
			}
		}
	}

	private void readDumpFile(File dumpFile) throws IOException {
		logger.info("Reading " + dumpFile + "...");

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(dumpFile)), "UTF-8"))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				try {
					LinkedHashMap<String, Object> data = this.gson.fromJson(line, LinkedHashMap.class);
					String schemaRef = (String) data.get("$schemaRef");

					if (SCHEMA_JOURNAL_v1.equals(schemaRef)) {
						Map<String, Object> header = (Map<String, Object>) data.get("header");
						ZonedDateTime gatewayTimestamp = ZonedDateTime.parse((String) header.get("gatewayTimestamp"));
						String uploaderID = (String) header.get("uploaderID");
						Map<String, Object> message = (Map<String, Object>) data.get("message");

						AbstractJournalEvent journalEvent = this.journalEventReader.readLine(this.gson.toJson(message));

						this.eddnBufferThread.buffer(gatewayTimestamp, uploaderID, journalEvent);
					} else if (SCHEMA_JOURNAL_v1_TEST.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_COMMODITY_v3.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_COMMODITY_v3_TEST.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_BLACKMARKET_v1.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_SHIPYARD_v2.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_SHIPYARD_v2_TEST.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_OUTFITTING_v2.equals(schemaRef)) {
						// NOOP
					} else if (SCHEMA_OUTFITTING_v2_TEST.equals(schemaRef)) {
						// NOOP
					} else {
						logger.warn("Unknown schemaRef: " + schemaRef);
					}
				} catch (JsonSyntaxException | NullPointerException e) {
					logger.error("Failed to parse line '" + line + "'", e);
				}
			}
		}
	}

}
