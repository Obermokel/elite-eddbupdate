package borg.ed.eddbupdate.eddndump;

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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import borg.ed.universe.converter.JournalConverter;
import borg.ed.universe.exceptions.NonUniqueResultException;
import borg.ed.universe.journal.JournalEventReader;
import borg.ed.universe.journal.events.AbstractJournalEvent;
import borg.ed.universe.journal.events.AbstractSystemJournalEvent;
import borg.ed.universe.journal.events.DockedEvent;
import borg.ed.universe.journal.events.ScanEvent;
import borg.ed.universe.model.Body;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.repository.BodyRepository;
import borg.ed.universe.repository.StarSystemRepository;
import borg.ed.universe.service.UniverseService;

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
	private StarSystemRepository starSystemRepository = null;

	@Autowired
	private BodyRepository bodyRepository = null;

	@Autowired
	private UniverseService universeService = null;

	@Autowired
	private EddnBufferThread eddnBufferThread = null;

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

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(dumpFile)), "UTF-8"))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				try {
					LinkedHashMap<String, Object> data = this.gson.fromJson(line, LinkedHashMap.class);
					String schemaRef = (String) data.get("$schemaRef");

					if (SCHEMA_JOURNAL_v1.equals(schemaRef)) {
						Map<String, Object> message = (Map<String, Object>) data.get("message");

						AbstractJournalEvent journalEvent = this.journalEventReader.readLine(this.gson.toJson(message));

						if (journalEvent instanceof AbstractSystemJournalEvent) {
							this.handleAbstractSystemJournalEvent((AbstractSystemJournalEvent) journalEvent);
						} else if (journalEvent instanceof ScanEvent) {
							this.handleScanEvent((ScanEvent) journalEvent);
						} else if (journalEvent instanceof DockedEvent) {
							this.handleDockedEvent((DockedEvent) journalEvent);
						} else {
							logger.warn("Unknown journal event: " + journalEvent);
						}

						//this.eddnBufferThread.buffer(gatewayTimestamp, uploaderID, journalEvent);
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

		try {
			StarSystem starSystem = this.universeService.findStarSystemByName(body.getStarSystemName());

			if (starSystem != null) {
				// Only update with pre-3.3 scan events, detailed scan events, or if the body does not yet exist
				if (event.getScanType() == null || ScanEvent.SCAN_TYPE_DETAILED.equals(event.getScanType()) || ScanEvent.SCAN_TYPE_NAV_BEACON_DETAIL.equals(event.getScanType())
						|| this.bodyRepository.findById(body.getId()) == null) {
					this.eddnBufferThread.bufferBody(body);
				}
			}
		} catch (NonUniqueResultException e) {
			logger.warn("Duplicate star system '" + body.getStarSystemName() + "'. Will delete all of them: " + e.getOthers());
			for (String id : e.getOtherIds()) {
				this.starSystemRepository.deleteById(id);
			}
		}
	}

	private void handleDockedEvent(DockedEvent event) {
		// TODO
	}

}
