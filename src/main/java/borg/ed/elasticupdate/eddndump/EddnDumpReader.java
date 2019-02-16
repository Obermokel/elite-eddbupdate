package borg.ed.elasticupdate.eddndump;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import borg.ed.galaxy.converter.JournalConverter;
import borg.ed.galaxy.elastic.ElasticBufferThread;
import borg.ed.galaxy.journal.JournalEventReader;
import borg.ed.galaxy.journal.events.AbstractJournalEvent;
import borg.ed.galaxy.journal.events.AbstractSystemJournalEvent;
import borg.ed.galaxy.journal.events.DockedEvent;
import borg.ed.galaxy.journal.events.ScanEvent;
import borg.ed.galaxy.model.Body;
import borg.ed.galaxy.model.StarSystem;

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
	private JournalConverter journalConverter = null;

	@Autowired
	private ElasticBufferThread eddnBufferThread = null;

	public void loadEddnDumpsIntoElasticsearch() throws InterruptedException {
		//this.readDumpsFromDir(new File("X:\\Spiele\\Elite Dangerous\\eddndump_until_3_3"));
		//this.readDumpsFromDir(new File("X:\\Spiele\\Elite Dangerous\\eddndump_since_3_3"));
		this.readDumpsFromDir(new File("X:\\Spiele\\Elite Dangerous\\eddndump_since_3_3_03"));
		//this.readDumpsFromDir(new File(System.getProperty("user.home"), "eddndump"));
	}

	private void readDumpsFromDir(File eddnDumpDir) throws InterruptedException {
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

	@SuppressWarnings("unchecked")
	private void readDumpFile(File dumpFile) throws IOException, InterruptedException {
		logger.info("Reading " + dumpFile + "...");

		int lineNumber = 0;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(dumpFile)), "UTF-8"))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				try {
					lineNumber++;
					if (lineNumber % 100_000 == 0) {
						logger.debug(String.format(Locale.US, "Line %,d", lineNumber));
					}

					LinkedHashMap<String, Object> data = this.gson.fromJson(line, LinkedHashMap.class);
					String schemaRef = (String) data.get("$schemaRef");

					if (SCHEMA_JOURNAL_v1.equals(schemaRef)) {
						Map<String, Object> message = (Map<String, Object>) data.get("message");
						String event = (String) message.get("event");
						String messageLine = line.substring(line.indexOf("\"message\":") + "\"message\":".length(), line.length() - 1);

						//AbstractJournalEvent journalEvent = this.journalEventReader.readLine(this.gson.toJson(message), event);
						AbstractJournalEvent journalEvent = this.journalEventReader.readLine(messageLine, event);

						if (journalEvent instanceof AbstractSystemJournalEvent) {
							this.handleAbstractSystemJournalEvent((AbstractSystemJournalEvent) journalEvent);
						} else if (journalEvent instanceof ScanEvent) {
							this.handleScanEvent((ScanEvent) journalEvent);
						} else if (journalEvent instanceof DockedEvent) {
							this.handleDockedEvent((DockedEvent) journalEvent);
						} else {
							logger.warn("Unknown journal event: " + journalEvent);
						}
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

	private void handleAbstractSystemJournalEvent(AbstractSystemJournalEvent event) throws InterruptedException {
		StarSystem starSystem = this.journalConverter.abstractSystemJournalEventToStarSystem(event);

		starSystem.setUpdatedAt(Date.from(event.getTimestamp().toInstant()));

		this.eddnBufferThread.bufferStarSystem(starSystem);
	}

	private void handleScanEvent(ScanEvent event) throws InterruptedException {
		Body body = this.journalConverter.scanEventToBody(event);

		body.setUpdatedAt(Date.from(event.getTimestamp().toInstant()));

		// Only update with detailed scan events
		if (event.isDetailedScan()) {
			this.eddnBufferThread.bufferBody(body);
		}
	}

	private void handleDockedEvent(DockedEvent event) {
		// Do nothing
	}

}
