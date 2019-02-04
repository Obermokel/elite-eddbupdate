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
import java.util.Locale;
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
import borg.ed.universe.constants.ReserveLevel;
import borg.ed.universe.constants.RingClass;
import borg.ed.universe.constants.StarClass;
import borg.ed.universe.constants.TerraformingState;
import borg.ed.universe.constants.VolcanismType;
import borg.ed.universe.exceptions.NonUniqueResultException;
import borg.ed.universe.model.Body;
import borg.ed.universe.model.Body.AtmosphereShare;
import borg.ed.universe.model.Body.BodyShare;
import borg.ed.universe.model.Body.MaterialShare;
import borg.ed.universe.model.Body.Ring;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.repository.StarSystemRepository;
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

	@Autowired
	private StarSystemRepository starSystemRepository = null;

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
						logger.debug(String.format(Locale.US, "Line %,d", lineNumber));
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
					String reserveLevelString = MiscUtil.getAsString(data.remove("reserveLevel"));
					List<Map<String, Object>> ringsList = (List<Map<String, Object>>) data.remove("rings");
					data.remove("belts"); // TODO
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
						ReserveLevel reserveLevel = ReserveLevel.fromJournalValue(reserveLevelString);
						List<Ring> rings = toRings(ringsList);

						switch (type) {
						case "Star":
							starClass = StarClass.fromJournalValue(subType);
							break;
						case "Planet":
							planetClass = PlanetClass.fromJournalValue(subType);
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
						body.setDistanceToArrivalLs(distanceToArrival);
						body.setStarClass(starClass);
						body.setPlanetClass(planetClass);
						body.setSurfaceTemperatureK(surfaceTemperature);
						body.setAgeMY(age);
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
						body.setRadiusKm(radius);
						body.setGravityG(gravity);
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
						body.setReserves(reserveLevel);
						body.setRings(rings);
						this.eddnBufferThread.bufferBody(body);
					}
				} catch (JsonSyntaxException | ParseException | NullPointerException e) {
					logger.error("Failed to parse line '" + line + "'", e);
				} catch (NonUniqueResultException e) {
					logger.warn("Duplicate star system. Will delete all of them: " + e.getOthers());
					for (String id : e.getOtherIds()) {
						this.starSystemRepository.deleteById(id);
					}
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

	private List<Ring> toRings(List<Map<String, Object>> list) {
		if (list != null && !list.isEmpty()) {
			List<Ring> result = new ArrayList<>(list.size());
			for (Map<String, Object> data : list) {
				Ring element = new Ring();
				element.setName(MiscUtil.getAsString(data.remove("name")));
				element.setRingClass(RingClass.fromJournalValue(MiscUtil.getAsString(data.remove("type"))));
				element.setMassMT(MiscUtil.getAsBigDecimal(data.remove("mass")));
				element.setInnerRadiusKm(MiscUtil.getAsBigDecimal(data.remove("innerRadius")));
				element.setOuterRadiusKm(MiscUtil.getAsBigDecimal(data.remove("outerRadius")));
				result.add(element);
			}
			return result;
		}
		return null;
	}

}
