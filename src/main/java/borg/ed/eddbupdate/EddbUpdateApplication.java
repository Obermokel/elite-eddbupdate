package borg.ed.eddbupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import borg.ed.eddbupdate.eddb.EddbReader;
import borg.ed.universe.UniverseApplication;

/**
 * EddbUpdateApplication
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
@Configuration
@Import(UniverseApplication.class)
public class EddbUpdateApplication {

	static final Logger logger = LoggerFactory.getLogger(EddbUpdateApplication.class);

	private static final ApplicationContext APPCTX = new AnnotationConfigApplicationContext(EddbUpdateApplication.class);

	public static void main(String[] args) throws Exception {
		APPCTX.getBean(EddbReader.class).loadEddbDataIntoElasticsearch();
	}

	@Bean
	public EddbReader eddbReader() {
		return new EddbReader();
	}

}
