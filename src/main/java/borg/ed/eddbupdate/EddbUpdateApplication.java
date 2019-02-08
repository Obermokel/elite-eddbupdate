package borg.ed.eddbupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import borg.ed.eddbupdate.eddndump.ElasticBufferThread;
import borg.ed.eddbupdate.eddndump.EddnDumpReader;
import borg.ed.eddbupdate.edsm.EdsmBodiesReader;
import borg.ed.eddbupdate.edsm.EdsmSystemsReader;
import borg.ed.galaxy.GalaxyApplication;
import borg.ed.galaxy.eddn.EddnElasticUpdater;

/**
 * EddbUpdateApplication
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
@Configuration
@Import(GalaxyApplication.class)
public class EddbUpdateApplication {

	static final Logger logger = LoggerFactory.getLogger(EddbUpdateApplication.class);

	private static final ApplicationContext APPCTX = new AnnotationConfigApplicationContext(EddbUpdateApplication.class);

	public static void main(String[] args) throws Exception {
		ElasticBufferThread bufferThread = APPCTX.getBean(ElasticBufferThread.class);
		bufferThread.start();

		EddnElasticUpdater eddnElasticUpdater = APPCTX.getBean(EddnElasticUpdater.class);
		eddnElasticUpdater.setUpdateMinorFactions(false);

		APPCTX.getBean(EdsmSystemsReader.class).loadEdsmDumpIntoElasticsearch();
		APPCTX.getBean(EdsmBodiesReader.class).loadEdsmDumpIntoElasticsearch();
		APPCTX.getBean(EddnDumpReader.class).loadEddnDumpsIntoElasticsearch();

		Thread.sleep(60_000);
	}

	@Bean
	public ElasticBufferThread eddnBufferThread() {
		return new ElasticBufferThread();
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
