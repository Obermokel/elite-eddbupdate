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
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import borg.ed.eddbupdate.eddndump.EddnBufferThread;
import borg.ed.universe.constants.AtmosphereType;
import borg.ed.universe.constants.BodyAtmosphere;
import borg.ed.universe.constants.BodyComposition;
import borg.ed.universe.constants.Element;
import borg.ed.universe.constants.PlanetClass;
import borg.ed.universe.constants.StarClass;
import borg.ed.universe.constants.TerraformingState;
import borg.ed.universe.constants.VolcanismType;
import borg.ed.universe.exceptions.NonUniqueResultException;
import borg.ed.universe.model.Body;
import borg.ed.universe.model.Body.AtmosphereShare;
import borg.ed.universe.model.Body.BodyShare;
import borg.ed.universe.model.Body.MaterialShare;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.service.UniverseService;
import borg.ed.universe.util.MiscUtil;

public class EdsmBodiesReader {

	static final Logger logger = LoggerFactory.getLogger(EdsmBodiesReader.class);

	private final Gson gson = new Gson();
	private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Autowired
	private UniverseService universeService = null;

	@Autowired
	private EddnBufferThread eddnBufferThread = null;

	@SuppressWarnings("unchecked")
	public void loadEdsmDumpIntoElasticsearch() throws IOException, InterruptedException {
		File dumpFile = new File("X:\\Spiele\\Elite Dangerous\\bodies.json");

		logger.info("Reading " + dumpFile + "...");

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
					if (line.length() <= 1) {
						continue;
					}
					if (line.endsWith(",")) {
						line = line.substring(0, line.length() - 1);
					}

					LinkedHashMap<String, Object> data = this.gson.fromJson(line, LinkedHashMap.class);
					data.remove("id");
					data.remove("id64");
					data.remove("bodyId");
					String name = MiscUtil.getAsString(data.remove("name"));
					data.remove("discovery"); // commander, date
					String type = MiscUtil.getAsString(data.remove("type"));
					String subType = MiscUtil.getAsString(data.remove("subType"));
					data.remove("offset"); // Whatever...
					data.remove("parents"); // Parent bodies
					BigDecimal distanceToArrival = MiscUtil.getAsBigDecimal(data.remove("distanceToArrival"));
					BigDecimal surfaceTemperature = MiscUtil.getAsBigDecimal(data.remove("surfaceTemperature")); // K
					BigDecimal age = MiscUtil.getAsBigDecimal(data.remove("age")); // MY
					BigDecimal solarMasses = MiscUtil.getAsBigDecimal(data.remove("solarMasses"));
					BigDecimal solarRadius = MiscUtil.getAsBigDecimal(data.remove("solarRadius"));
					Boolean isMainStar = MiscUtil.getAsBooleanObject(data.remove("isMainStar"));
					Boolean isScoopable = MiscUtil.getAsBooleanObject(data.remove("isScoopable"));
					String spectralClass = MiscUtil.getAsString(data.remove("spectralClass"));
					String luminosity = MiscUtil.getAsString(data.remove("luminosity"));
					BigDecimal absoluteMagnitude = MiscUtil.getAsBigDecimal(data.remove("absoluteMagnitude"));
					String volcanismType = MiscUtil.getAsString(data.remove("volcanismType"));
					String atmosphereType = MiscUtil.getAsString(data.remove("atmosphereType"));
					String terraformingState = MiscUtil.getAsString(data.remove("terraformingState"));
					BigDecimal earthMasses = MiscUtil.getAsBigDecimal(data.remove("earthMasses"));
					BigDecimal radius = MiscUtil.getAsBigDecimal(data.remove("radius")); // km
					BigDecimal gravity = MiscUtil.getAsBigDecimal(data.remove("gravity")); // G
					BigDecimal surfacePressure = MiscUtil.getAsBigDecimal(data.remove("surfacePressure")); // Atmospheres
					BigDecimal orbitalPeriod = MiscUtil.getAsBigDecimal(data.remove("orbitalPeriod")); // d
					BigDecimal semiMajorAxis = MiscUtil.getAsBigDecimal(data.remove("semiMajorAxis")); // AU
					BigDecimal orbitalEccentricity = MiscUtil.getAsBigDecimal(data.remove("orbitalEccentricity"));
					BigDecimal orbitalInclination = MiscUtil.getAsBigDecimal(data.remove("orbitalInclination")); // °
					BigDecimal argOfPeriapsis = MiscUtil.getAsBigDecimal(data.remove("argOfPeriapsis")); // °
					BigDecimal rotationalPeriod = MiscUtil.getAsBigDecimal(data.remove("rotationalPeriod")); // d
					Boolean rotationalPeriodTidallyLocked = MiscUtil.getAsBooleanObject(data.remove("rotationalPeriodTidallyLocked"));
					BigDecimal axialTilt = MiscUtil.getAsBigDecimal(data.remove("axialTilt")); // °
					Boolean isLandable = MiscUtil.getAsBooleanObject(data.remove("isLandable"));
					String systemName = MiscUtil.getAsString(data.remove("systemName"));
					String updateTimeString = MiscUtil.getAsString(data.remove("updateTime"));
					Map<String, Number> atmosphereCompositionMap = (Map<String, Number>) data.remove("atmosphereComposition");
					Map<String, Number> solidCompositionMap = (Map<String, Number>) data.remove("solidComposition");
					Map<String, Number> materialsMap = (Map<String, Number>) data.remove("materials");
					data.remove("belts");
					data.remove("rings"); // TODO
					data.remove("reserveLevel"); // TODO
					data.remove("systemId");
					data.remove("systemId64");

					StarSystem starSystem = this.universeService.findStarSystemByName(systemName);

					if (!data.isEmpty()) {
						logger.warn("Unknown attributes: " + data);
					} else if (starSystem == null) {
						//logger.warn("Star system '" + systemName + "' not found");
					} else {
						Date date = this.df.parse(updateTimeString);

						StarClass starClass = null;
						PlanetClass planetClass = null;
						List<AtmosphereShare> atmosphereShares = toAtmosphereShares(atmosphereCompositionMap);
						List<BodyShare> bodyShares = toBodyShares(solidCompositionMap);
						List<MaterialShare> materialShares = toMaterialShares(materialsMap);

						switch (type) {
						case "Star":
							starClass = subTypeToStarClass(subType);
							break;
						case "Planet":
							planetClass = subTypeToPlanetClass(subType);
							break;
						default:
							logger.warn("Unknown body type '" + type + "'");
							break;
						}

						Body body = new Body();
						body.setId(Body.generateId(starSystem.getCoord(), name, systemName));
						body.setUpdatedAt(date);
						body.setStarSystemId(starSystem.getId());
						body.setStarSystemName(starSystem.getName());
						body.setCoord(starSystem.getCoord());
						body.setName(name);
						body.setDistanceToArrival(distanceToArrival);
						body.setStarClass(starClass);
						body.setPlanetClass(planetClass);
						body.setSurfaceTemperature(surfaceTemperature);
						body.setAge(age);
						body.setSolarMasses(solarMasses);
						body.setSolarRadius(solarRadius);
						body.setIsMainStar(isMainStar);
						body.setIsScoopable(isScoopable);
						body.setSpectralClass(spectralClass);
						body.setLuminosity(luminosity);
						body.setAbsoluteMagnitude(absoluteMagnitude);
						body.setVolcanismType(VolcanismType.fromJournalValue(volcanismType));
						body.setAtmosphereType(AtmosphereType.fromJournalValue(atmosphereType));
						body.setTerraformingState(TerraformingState.fromJournalValue(terraformingState));
						body.setEarthMasses(earthMasses);
						body.setRadius(radius);
						body.setGravity(gravity);
						body.setSurfacePressure(surfacePressure);
						body.setOrbitalPeriod(orbitalPeriod);
						body.setSemiMajorAxis(semiMajorAxis);
						body.setOrbitalEccentricity(orbitalEccentricity);
						body.setOrbitalInclination(orbitalInclination);
						body.setArgOfPeriapsis(argOfPeriapsis);
						body.setRotationalPeriod(rotationalPeriod);
						body.setTidallyLocked(rotationalPeriodTidallyLocked);
						body.setAxisTilt(axialTilt);
						body.setIsLandable(isLandable);
						body.setAtmosphereShares(atmosphereShares);
						body.setBodyShares(bodyShares);
						body.setMaterialShares(materialShares);
						this.eddnBufferThread.bufferBody(body);
					}
				} catch (JsonSyntaxException | ParseException | NullPointerException e) {
					logger.error("Failed to parse line '" + line + "'", e);
				} catch (NonUniqueResultException e) {
					//logger.error("Failed to parse line '" + line + "': " + e);
				}
			}
		}
	}

	private List<AtmosphereShare> toAtmosphereShares(Map<String, Number> map) {
		if (map != null && !map.isEmpty()) {
			List<AtmosphereShare> result = new ArrayList<>(map.size());
			for (String name : map.keySet()) {
				AtmosphereShare element = new AtmosphereShare();
				element.setName(BodyAtmosphere.fromJournalValue(name));
				element.setPercent(MiscUtil.getAsBigDecimal(map.get(name)));
				result.add(element);
			}
			return result;
		}
		return null;
	}

	private List<BodyShare> toBodyShares(Map<String, Number> map) {
		if (map != null && !map.isEmpty()) {
			List<BodyShare> result = new ArrayList<>(map.size());
			for (String name : map.keySet()) {
				BodyShare element = new BodyShare();
				element.setName(BodyComposition.fromJournalValue(name));
				element.setPercent(MiscUtil.getAsBigDecimal(map.get(name)));
				result.add(element);
			}
			return result;
		}
		return null;
	}

	private List<MaterialShare> toMaterialShares(Map<String, Number> map) {
		if (map != null && !map.isEmpty()) {
			List<MaterialShare> result = new ArrayList<>(map.size());
			for (String name : map.keySet()) {
				MaterialShare element = new MaterialShare();
				element.setName(Element.fromJournalValue(name));
				element.setPercent(MiscUtil.getAsBigDecimal(map.get(name)));
				result.add(element);
			}
			return result;
		}
		return null;
	}

	private StarClass subTypeToStarClass(String subType) {
		switch (subType) {
		case "O (Blue-White) Star":
			return StarClass.O;
		case "B (Blue-White) Star":
			return StarClass.B;
		case "A (Blue-White) Star":
		case "A (Blue-White super giant) Star":
			return StarClass.A;
		case "F (White) Star":
		case "F (White super giant) Star":
			return StarClass.F;
		case "G (White-Yellow) Star":
		case "G (White-Yellow super giant) Star":
			return StarClass.G;
		case "K (Yellow-Orange) Star":
		case "K (Yellow-Orange giant) Star":
			return StarClass.K;
		case "M (Red dwarf) Star":
		case "M (Red giant) Star":
		case "M (Red super giant) Star":
			return StarClass.M;
		case "L (Brown dwarf) Star":
			return StarClass.L;
		case "T (Brown dwarf) Star":
			return StarClass.T;
		case "T Tauri Star":
			return StarClass.TTS;
		case "Y (Brown dwarf) Star":
			return StarClass.Y;
		case "Herbig Ae/Be Star":
			return StarClass.AEBE;
		case "White Dwarf (DA) Star":
			return StarClass.DA;
		case "White Dwarf (DAB) Star":
			return StarClass.DAB;
		case "White Dwarf (DC) Star":
			return StarClass.DC;
		case "Neutron Star":
			return StarClass.N;
		case "Black Hole":
			return StarClass.H;
		case "Wolf-Rayet Star":
			return StarClass.W;
		case "Wolf-Rayet C Star":
			return StarClass.WC;
		case "Wolf-Rayet N Star":
			return StarClass.WN;
		case "Wolf-Rayet NC Star":
			return StarClass.WNC;
		case "Wolf-Rayet O Star":
			return StarClass.WO;
		case "C Star":
			return StarClass.C;
		case "S-type Star":
			return StarClass.S;
		case "MS-type Star":
			return StarClass.MS;
		default:
			logger.warn("Unknown star subType '" + subType + "'");
			return null;
		}
	}

	private PlanetClass subTypeToPlanetClass(String subType) {
		PlanetClass planetClass = PlanetClass.fromJournalValue(subType);
		if (planetClass == null) {
			logger.warn("Unknown planet subType '" + subType + "'");
		}
		return planetClass;
	}

}
