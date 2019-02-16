package borg;

import java.math.BigDecimal;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import borg.ed.galaxy.GalaxyApplication;
import borg.ed.galaxy.constants.Element;
import borg.ed.galaxy.data.Coord;
import borg.ed.galaxy.model.Body;
import borg.ed.galaxy.model.Body.MaterialShare;
import borg.ed.galaxy.service.GalaxyService;

@Configuration
@Import(GalaxyApplication.class)
public class PolPlanet {

	static final Logger logger = LoggerFactory.getLogger(PolPlanet.class);

	private static final ApplicationContext APPCTX = new AnnotationConfigApplicationContext(PolPlanet.class);

	public static void main(String[] args) throws Exception {
		GalaxyService galaxyService = APPCTX.getBean(GalaxyService.class);
		MaterialShare ms = new MaterialShare();
		ms.setName(Element.POLONIUM);
		ms.setPercent(new BigDecimal("2.0"));
		Page<Body> page = galaxyService.findPlanetsHavingElementsNear(new Coord(), 100000.0f, Collections.singleton(ms), PageRequest.of(0, 10));
		System.out.println(page.getTotalElements());
		System.out.println(page.getContent());
	}

}
