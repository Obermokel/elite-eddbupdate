package borg.ed.elasticupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import borg.ed.elasticupdate.eddndump.EddnDumpReader;
import borg.ed.elasticupdate.edsm.EdsmBodiesReader;
import borg.ed.elasticupdate.edsm.EdsmSystemsReader;
import borg.ed.galaxy.GalaxyApplication;
import borg.ed.galaxy.eddn.EddnElasticUpdater;
import borg.ed.galaxy.elastic.ElasticBufferThread;

/**
 * ElasticUpdateApplication
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
@Configuration
@Import(GalaxyApplication.class)
public class ElasticUpdateApplication {

	static final Logger logger = LoggerFactory.getLogger(ElasticUpdateApplication.class);

	private static final ApplicationContext APPCTX = new AnnotationConfigApplicationContext(ElasticUpdateApplication.class);

	public static void main(String[] args) throws Exception {
		ElasticBufferThread elasticBufferThread = APPCTX.getBean(ElasticBufferThread.class);
		elasticBufferThread.start();

		EddnElasticUpdater eddnElasticUpdater = APPCTX.getBean(EddnElasticUpdater.class);
		eddnElasticUpdater.setUpdateMinorFactions(false);

		//APPCTX.getBean(EdsmSystemsReader.class).loadEdsmDumpIntoElasticsearch();
		//APPCTX.getBean(EdsmBodiesReader.class).loadEdsmDumpIntoElasticsearch();
		APPCTX.getBean(EddnDumpReader.class).loadEddnDumpsIntoElasticsearch();

		Thread.sleep(60_000);
	}

	@Bean
	public EdsmSystemsReader edsmSystemsReader() {
		return new EdsmSystemsReader();
	}

	@Bean
	public EdsmBodiesReader edsmBodiesReader() {
		return new EdsmBodiesReader();
	}

	@Bean
	public EddnDumpReader eddnDumpReader() {
		return new EddnDumpReader();
	}

}
