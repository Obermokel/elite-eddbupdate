package borg.ed.eddbupdate.eddndump;

import borg.ed.universe.eddn.EddnElasticUpdater;
import borg.ed.universe.journal.JournalEventReader;
import borg.ed.universe.journal.events.AbstractJournalEvent;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * EddnDumpReader
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public class EddnDumpReader {

    static final Logger logger = LoggerFactory.getLogger(EddnDumpReader.class);

    private static final String SCHEMA_JOURNAL_v1 = "https://eddn.edcd.io/schemas/journal/1";

    private final Gson gson = new Gson();

    @Autowired
    private JournalEventReader journalEventReader = null;

    @Autowired
    private EddnElasticUpdater eddnElasticUpdater = null;

    public void loadEddnDumpsIntoElasticsearch() {
        File eddnDumpDir = new File(System.getProperty("user.home"), "eddndump");
        if (eddnDumpDir.exists() && eddnDumpDir.isDirectory()) {
            File[] dumpFiles = eddnDumpDir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.getName().startsWith("eddndump") && file.getName().endsWith(".txt");
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
                LinkedHashMap<String, Object> data = this.gson.fromJson(line, LinkedHashMap.class);
                String schemaRef = (String) data.get("$schemaRef");

                if (SCHEMA_JOURNAL_v1.equals(schemaRef)) {
                    Map<String, Object> header = (Map<String, Object>) data.get("header");
                    ZonedDateTime gatewayTimestamp = ZonedDateTime.parse((String) header.get("gatewayTimestamp"));
                    String uploaderID = (String) header.get("uploaderID");
                    Map<String, Object> message = (Map<String, Object>) data.get("message");

                    AbstractJournalEvent journalEvent = this.journalEventReader.readLine(this.gson.toJson(message));

                    this.eddnElasticUpdater.onNewJournalMessage(gatewayTimestamp, uploaderID, journalEvent);
                } else {
                    //logger.warn("Unknown schemaRef: " + schemaRef);
                }
            }
        }
    }

}
