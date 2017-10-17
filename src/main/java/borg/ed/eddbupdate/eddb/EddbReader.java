package borg.ed.eddbupdate.eddb;

import borg.ed.eddbupdate.eddb.EddbBody.MaterialsShare;
import borg.ed.universe.constants.Allegiance;
import borg.ed.universe.constants.AtmosphereType;
import borg.ed.universe.constants.BodyAtmosphere;
import borg.ed.universe.constants.Economy;
import borg.ed.universe.constants.Element;
import borg.ed.universe.constants.Government;
import borg.ed.universe.constants.PlanetClass;
import borg.ed.universe.constants.RingClass;
import borg.ed.universe.constants.StarClass;
import borg.ed.universe.constants.State;
import borg.ed.universe.constants.SystemSecurity;
import borg.ed.universe.constants.TerraformingState;
import borg.ed.universe.constants.VolcanismType;
import borg.ed.universe.data.Coord;
import borg.ed.universe.exceptions.NonUniqueResultException;
import borg.ed.universe.model.Body;
import borg.ed.universe.model.Body.AtmosphereShare;
import borg.ed.universe.model.Body.MaterialShare;
import borg.ed.universe.model.Body.Ring;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.repository.BodyRepository;
import borg.ed.universe.repository.StarSystemRepository;
import borg.ed.universe.service.UniverseService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * EddbReader
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public class EddbReader {

    static final Logger logger = LoggerFactory.getLogger(EddbReader.class);

    private static final File BASE_DIR = new File(System.getProperty("user.home"), ".eddbdata");

    @Autowired
    private UniverseService universeService = null;
    @Autowired
    private StarSystemRepository systemRepo = null;
    @Autowired
    private BodyRepository bodyRepo = null;

    public void loadEddbDataIntoElasticsearch() throws IOException {
        if (!BASE_DIR.exists()) {
            BASE_DIR.mkdirs();
        }

        long start = System.currentTimeMillis();

        File edsmFile = new File(BASE_DIR, "systemsWithCoordinates.json");
        this.downloadIfUpdated("https://www.edsm.net/dump/systemsWithCoordinates.json", edsmFile);
        Map<Long, EdsmSystem> edsmSystemsById = this.loadEdsmSystemsById(edsmFile);

        File systemsPopulatedFile = new File(BASE_DIR, "systems_populated.jsonl");
        this.downloadIfUpdated("https://eddb.io/archive/v5/systems_populated.jsonl", systemsPopulatedFile);
        this.readSystemsPopulatedJsonlIntoRepo(systemsPopulatedFile, edsmSystemsById, universeService, systemRepo);

        File systemsFile = new File(BASE_DIR, "systems.csv");
        this.downloadIfUpdated("https://eddb.io/archive/v5/systems.csv", systemsFile);
        this.readSystemsCsvIntoRepo(systemsFile, edsmSystemsById, universeService, systemRepo);

        File bodiesFile = new File(BASE_DIR, "bodies.jsonl");
        this.downloadIfUpdated("https://eddb.io/archive/v5/bodies.jsonl", bodiesFile);
        this.readBodiesJsonlIntoRepo(bodiesFile, universeService, bodyRepo);

        long end = System.currentTimeMillis();
        logger.info("Downloaded data in " + DurationFormatUtils.formatDuration(end - start, "H:mm:ss"));
    }

    private Map<Long, EdsmSystem> loadEdsmSystemsById(File edsmFile) throws IOException {
        logger.debug("Reading " + edsmFile.getName());

        //@formatter:off
		final Gson gson = new GsonBuilder()
				//.registerTypeAdapter(Date.class, new SecondsSinceEpochDeserializer())
				//.registerTypeAdapter(Boolean.class, new BooleanDigitDeserializer())
				.setDateFormat("yyyy-MM-dd HH:mm:ss").serializeNulls().setPrettyPrinting().create();
		//@formatter:on

        Map<Long, EdsmSystem> edsmSystemsById = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(edsmFile), "UTF-8"))) {
            EdsmSystem[] entities = (EdsmSystem[]) gson.fromJson(reader, Array.newInstance(EdsmSystem.class, 0).getClass());
            for (EdsmSystem entity : entities) {
                edsmSystemsById.put(entity.getId(), entity);
            }
        }

        return edsmSystemsById;
    }

    private void readSystemsCsvIntoRepo(File file, Map<Long, EdsmSystem> edsmSystemsById, UniverseService uniserv, StarSystemRepository repo) throws IOException {
        final DateFormat dfEta = new SimpleDateFormat("MMM dd @ HH:mm", Locale.US);
        final int batchSize = 1000;
        final List<StarSystem> batch = new ArrayList<>(batchSize);
        final int total = this.countLines(file) - 1;

        EddbSystemCsvRecordParser csvRecordParser = new EddbSystemCsvRecordParser();

        logger.debug("Reading " + file.getName());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().parse(reader);

            long startBatch = System.currentTimeMillis();
            int n = 0;
            for (CSVRecord record : records) {
                try {
                    EddbSystem eddbSystem = csvRecordParser.parse(record);
                    StarSystem starSystem = uniserv.findStarSystemByName(eddbSystem.getName());
                    if (starSystem == null) {
                        starSystem = this.eddbSystemToStarSystem(eddbSystem);
                    }
                    EdsmSystem edsmSystem = edsmSystemsById.get(eddbSystem.getEdsm_id());
                    starSystem.setCreatedAt(edsmSystem != null ? edsmSystem.getCreatedAt() : eddbSystem.getUpdated_at());
                    starSystem.setFirstDiscoveredBy(null);
                    batch.add(starSystem);
                    if (batch.size() >= batchSize) {
                        repo.saveAll(batch);
                        batch.clear();
                    }

                    if (++n % batchSize == 0) {
                        long millis = System.currentTimeMillis() - startBatch;
                        double entitiesPerSec = ((double) batchSize / Math.max(1, millis)) * 1000d;
                        int entitiesRemaining = total - n;
                        double secondsRemaining = entitiesRemaining / entitiesPerSec;
                        Date eta = new Date(System.currentTimeMillis() + (long) (secondsRemaining * 1000));
                        logger.info(String.format(Locale.US, "Imported %,d of %,d %s (%.1f/sec) -- ETA %s", n, total, file.getName().substring(0, file.getName().lastIndexOf(".")), entitiesPerSec, dfEta.format(eta)));
                        startBatch = System.currentTimeMillis();
                    }
                } catch (NonUniqueResultException e) {
                    logger.warn("Corrupt line in " + file + ": " + record, e);
                }
            }
            if (!batch.isEmpty()) {
                repo.saveAll(batch);
            }
        }
    }

    private void readSystemsPopulatedJsonlIntoRepo(File file, Map<Long, EdsmSystem> edsmSystemsById, UniverseService uniserv, StarSystemRepository repo) throws IOException {
        final DateFormat dfEta = new SimpleDateFormat("MMM dd @ HH:mm", Locale.US);
        final int batchSize = 1000;
        final List<StarSystem> batch = new ArrayList<>(batchSize);
        final int total = this.countLines(file) - 1;

        //@formatter:off
		final Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new SecondsSinceEpochDeserializer())
				.registerTypeAdapter(Boolean.class, new BooleanDigitDeserializer()).setDateFormat("yyyy-MM-dd HH:mm:ss").serializeNulls().setPrettyPrinting()
				.create();
		//@formatter:on

        logger.debug("Reading " + file.getName());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            long startBatch = System.currentTimeMillis();
            int n = 0;
            String line = reader.readLine();
            while (line != null) {
                try {
                    EddbSystem eddbSystem = gson.fromJson(line, EddbSystem.class);
                    StarSystem starSystem = uniserv.findStarSystemByName(eddbSystem.getName());
                    if (starSystem == null) {
                        starSystem = this.eddbSystemToStarSystem(eddbSystem);
                    }
                    EdsmSystem edsmSystem = edsmSystemsById.get(eddbSystem.getEdsm_id());
                    starSystem.setCreatedAt(edsmSystem != null ? edsmSystem.getCreatedAt() : eddbSystem.getUpdated_at());
                    starSystem.setFirstDiscoveredBy(null);
                    batch.add(starSystem);
                    if (batch.size() >= batchSize) {
                        repo.saveAll(batch);
                        batch.clear();
                    }

                    if (++n % batchSize == 0) {
                        long millis = System.currentTimeMillis() - startBatch;
                        double entitiesPerSec = ((double) batchSize / Math.max(1, millis)) * 1000d;
                        int entitiesRemaining = total - n;
                        double secondsRemaining = entitiesRemaining / entitiesPerSec;
                        Date eta = new Date(System.currentTimeMillis() + (long) (secondsRemaining * 1000));
                        logger.info(String.format(Locale.US, "Imported %,d of %,d %s (%.1f/sec) -- ETA %s", n, total, file.getName().substring(0, file.getName().lastIndexOf(".")), entitiesPerSec, dfEta.format(eta)));
                        startBatch = System.currentTimeMillis();
                    }
                } catch (JsonSyntaxException e) {
                    logger.warn("Corrupt line in " + file + ": " + line, e);
                } catch (NonUniqueResultException e) {
                    logger.warn("Corrupt line in " + file + ": " + line, e);
                }
                line = reader.readLine();
            }
            if (!batch.isEmpty()) {
                repo.saveAll(batch);
            }
        }
    }

    private StarSystem eddbSystemToStarSystem(EddbSystem eddbSystem) {
        StarSystem result = new StarSystem();

        result.setId(null);
        result.setEddbId(eddbSystem.getId());
        result.setEdsmId(eddbSystem.getEdsm_id());
        result.setCreatedAt(null);
        result.setUpdatedAt(eddbSystem.getUpdated_at());
        result.setCoord(new Coord(eddbSystem.getX(), eddbSystem.getY(), eddbSystem.getZ()));
        result.setName(eddbSystem.getName());
        result.setPopulation(eddbSystem.getPopulation());
        result.setGovernment(Government.fromJournalValue(eddbSystem.getGovernment()));
        result.setAllegiance(Allegiance.fromJournalValue(eddbSystem.getAllegiance()));
        result.setSecurity(SystemSecurity.fromJournalValue(eddbSystem.getSecurity()));
        result.setEconomy(Economy.fromJournalValue(eddbSystem.getPrimary_economy()));
        result.setNeedsPermit(Boolean.TRUE.equals(eddbSystem.getNeeds_permit()) ? Boolean.TRUE : null);
        result.setState(State.fromJournalValue(eddbSystem.getState()));

        return result;
    }

    private void readBodiesJsonlIntoRepo(File file, UniverseService uniserv, BodyRepository repo) throws IOException {
        final DateFormat dfEta = new SimpleDateFormat("MMM dd @ HH:mm", Locale.US);
        final int batchSize = 1000;
        final List<Body> batch = new ArrayList<>(batchSize);
        final int total = this.countLines(file) - 1;

        //@formatter:off
		final Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new SecondsSinceEpochDeserializer())
				.registerTypeAdapter(Boolean.class, new BooleanDigitDeserializer()).setDateFormat("yyyy-MM-dd HH:mm:ss").serializeNulls().setPrettyPrinting()
				.create();
		//@formatter:on

        logger.debug("Reading " + file.getName());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            long startBatch = System.currentTimeMillis();
            int n = 0;
            String line = reader.readLine();
            while (line != null) {
                try {
                    EddbBody eddbBody = gson.fromJson(line, EddbBody.class);
                    Body body = uniserv.findBodyByName(eddbBody.getName());
                    if (body == null) {
                        body = this.eddbBodyToBody(eddbBody);
                        if (body != null) {
                            StarSystem starSystem = universeService.findStarSystemByEddbId(eddbBody.getSystem_id());
                            if (starSystem != null) {
                                body.setCoord(starSystem.getCoord());
                                body.setStarSystemId(starSystem.getId());
                                body.setStarSystemName(starSystem.getName());
                                body.setReserves(starSystem.getReserves());
                            }
                        }
                    }
                    if (body != null) {
                        body.setCreatedAt(eddbBody.getCreated_at());
                        body.setFirstDiscoveredBy(null);
                        if (body.getCoord() != null) {
                            batch.add(body);
                        }
                    }
                    if (batch.size() >= batchSize) {
                        repo.saveAll(batch);
                        batch.clear();
                    }

                    if (++n % batchSize == 0) {
                        long millis = System.currentTimeMillis() - startBatch;
                        double entitiesPerSec = ((double) batchSize / Math.max(1, millis)) * 1000d;
                        int entitiesRemaining = total - n;
                        double secondsRemaining = entitiesRemaining / entitiesPerSec;
                        Date eta = new Date(System.currentTimeMillis() + (long) (secondsRemaining * 1000));
                        logger.info(String.format(Locale.US, "Imported %,d of %,d %s (%.1f/sec) -- ETA %s", n, total, file.getName().substring(0, file.getName().lastIndexOf(".")), entitiesPerSec, dfEta.format(eta)));
                        startBatch = System.currentTimeMillis();
                    }
                } catch (JsonSyntaxException e) {
                    logger.warn("Corrupt line in " + file + ": " + line, e);
                } catch (NonUniqueResultException e) {
                    logger.warn("Corrupt line in " + file + ": " + line, e);
                }
                line = reader.readLine();
            }
            if (!batch.isEmpty()) {
                repo.saveAll(batch);
            }
        }
    }

    private Body eddbBodyToBody(EddbBody eddbBody) {
        try {
            Body result = new Body();

            result.setId(null);
            result.setEddbId(eddbBody.getId());
            result.setEdsmId(null);
            result.setCreatedAt(eddbBody.getCreated_at());
            result.setUpdatedAt(eddbBody.getUpdated_at());
            result.setCoord(null);
            result.setName(eddbBody.getName());
            result.setDistanceToArrival(eddbBody.getDistance_to_arrival());
            result.setStarClass(StarClass.fromJournalValue(eddbBody.getSpectral_class()));
            try {
                result.setPlanetClass(PlanetClass.fromJournalValue(eddbBody.getType_name()));
            } catch (IllegalArgumentException e) {
                if (eddbBody.getType_name().toLowerCase().contains("neutron")) {
                    result.setStarClass(StarClass.N);
                } else if (eddbBody.getType_name().toLowerCase().contains("hole")) {
                    result.setStarClass(StarClass.H);
                } else if (eddbBody.getType_name().toLowerCase().contains("hole")) {
                    result.setStarClass(StarClass.H);
                }
            }
            result.setSurfaceTemperature(eddbBody.getSurface_temperature());
            result.setAge(eddbBody.getAge());
            result.setSolarMasses(eddbBody.getSolar_masses());
            result.setVolcanismType(VolcanismType.fromJournalValue(eddbBody.getVolcanism_type_name()));
            result.setAtmosphereType(AtmosphereType.fromJournalValue(eddbBody.getAtmosphere_type_name()));
            result.setTerraformingState(TerraformingState.fromJournalValue(eddbBody.getTerraforming_state_name()));
            result.setEarthMasses(eddbBody.getEarth_masses());
            result.setRadius(eddbBody.getRadius());
            result.setGravity(eddbBody.getGravity());
            result.setSurfacePressure(eddbBody.getSurface_pressure());
            result.setOrbitalPeriod(eddbBody.getOrbital_period());
            result.setSemiMajorAxis(eddbBody.getSemi_major_axis());
            result.setOrbitalEccentricity(eddbBody.getOrbital_eccentricity());
            result.setOrbitalInclination(eddbBody.getOrbital_inclination());
            result.setArgOfPeriapsis(eddbBody.getArg_of_periapsis());
            result.setRotationalPeriod(eddbBody.getRotational_period());
            result.setTidallyLocked(eddbBody.getIs_rotational_period_tidally_locked());
            result.setAxisTilt(eddbBody.getAxis_tilt());
            result.setIsLandable(eddbBody.getIs_landable());
            result.setReserves(null);
            result.setRings(this.ringsToRings(eddbBody.getRings()));
            result.setAtmosphereShares(this.sharesToAtmosphereShares(eddbBody.getAtmosphere_composition()));
            result.setMaterialShares(this.sharesToMaterialShares(eddbBody.getMaterials()));

            return result;
        } catch (IllegalArgumentException e) {
            logger.warn(e.getMessage());
            return null;
        }
    }

    private List<Ring> ringsToRings(List<borg.ed.eddbupdate.eddb.EddbBody.Ring> list) {
        if (list == null || list.isEmpty()) {
            return null;
        } else {
            List<Ring> result = new ArrayList<>(list.size());
            for (borg.ed.eddbupdate.eddb.EddbBody.Ring ringData : list) {
                Ring ring = new Ring();
                ring.setName(ringData.getName());
                ring.setRingClass(RingClass.fromJournalValue(ringData.getRing_type_name()));
                ring.setMassMT(ringData.getRing_mass());
                ring.setInnerRadius(ringData.getRing_inner_radius());
                ring.setOuterRadius(ringData.getRing_outer_radius());
                result.add(ring);
            }
            return result;
        }
    }

    private List<AtmosphereShare> sharesToAtmosphereShares(List<borg.ed.eddbupdate.eddb.EddbBody.AtmosphereShare> list) {
        if (list == null || list.isEmpty()) {
            return null;
        } else {
            List<AtmosphereShare> result = new ArrayList<>(list.size());
            for (borg.ed.eddbupdate.eddb.EddbBody.AtmosphereShare data : list) {
                AtmosphereShare share = new AtmosphereShare();
                share.setName(BodyAtmosphere.fromJournalValue(data.getAtmosphere_component_name()));
                share.setPercent(data.getShare());
                result.add(share);
            }
            return result;
        }
    }

    private List<MaterialShare> sharesToMaterialShares(List<MaterialsShare> list) {
        if (list == null || list.isEmpty()) {
            return null;
        } else {
            List<MaterialShare> result = new ArrayList<>(list.size());
            for (MaterialsShare data : list) {
                MaterialShare share = new MaterialShare();
                share.setName(Element.fromJournalValue(data.getMaterial_name()));
                share.setPercent(data.getShare());
                result.add(share);
            }
            return result;
        }
    }

    private int countLines(File file) throws IOException {
        int total = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            while (reader.readLine() != null) {
                total++;
            }
        }
        return total;
    }

    private boolean downloadIfUpdated(String url, File file) throws IOException {
        //if (!file.exists() || (System.currentTimeMillis() - file.lastModified() > DateUtils.MILLIS_PER_DAY / 2L && headLastModified(url) > file.lastModified())) {
        if (!file.exists() || headLastModified(url) > file.lastModified()) {
            backup(file);
            download(url, file);
            return true;
        } else {
            return false;
        }
    }

    private long backup(File sourceFile) throws IOException {
        //        File backupDir = new File(BASE_DIR, "backup");
        //        if (!backupDir.exists()) {
        //            backupDir.mkdirs();
        //        }
        //
        //        InputStream in = null;
        //        ZipArchiveOutputStream out = null;
        //        try {
        //            String basename = sourceFile.getName().substring(0, sourceFile.getName().lastIndexOf(".")); // w/o dot
        //            String extension = sourceFile.getName().substring(sourceFile.getName().lastIndexOf(".")); // w/ dot
        //            String lastModified = new SimpleDateFormat("yyyy-MM-dd").format(sourceFile.lastModified());
        //            File backupFile = new File(backupDir, basename + "." + lastModified + ".zip");
        //
        //            if (!sourceFile.exists() || backupFile.exists()) {
        //                return 0L;
        //            } else {
        //                in = new FileInputStream(sourceFile);
        //                out = new ZipArchiveOutputStream(backupFile);
        //                out.putArchiveEntry(out.createArchiveEntry(sourceFile, basename + "." + lastModified + extension));
        //                logger.debug(String.format(Locale.US, "Backup of %s to %s (%.1f MB)", sourceFile.getName(), backupFile.getName(), sourceFile.length() / 1048576.0));
        //                long bytes = IOUtils.copyLarge(in, out);
        //                out.closeArchiveEntry();
        //                return bytes;
        //            }
        //        } finally {
        //            if (out != null) {
        //                out.close();
        //            }
        //            if (in != null) {
        //                in.close();
        //            }
        //        }
        return 0L;
    }

    private long download(String url, File file) throws IOException {
        HttpURLConnection conn = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept-Encoding", "gzip");
            conn.setConnectTimeout(60000);
            conn.setReadTimeout(600000);
            int responseCode = conn.getResponseCode();
            String responseMessage = conn.getResponseMessage();
            if (responseCode != 200) {
                throw new IOException("Response Code " + responseCode + " (" + responseMessage + ")");
            } else {
                if ("gzip".equals(conn.getContentEncoding())) {
                    in = new GZIPInputStream(conn.getInputStream());
                } else {
                    in = conn.getInputStream();
                }
                out = new FileOutputStream(file, false);
                logger.debug(String.format(Locale.US, "Download of %s (%.1f MB)", url, headContentLength(url) / 1048576.0));
                return IOUtils.copyLarge(in, out);
            }
        } finally {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private long headLastModified(String url) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("HEAD");
            return conn.getLastModified();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private long headContentLength(String url) throws IOException {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("HEAD");
            return conn.getContentLengthLong();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

}
