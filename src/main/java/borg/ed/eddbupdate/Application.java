package borg.ed.eddbupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import borg.ed.eddbupdate.eddb.EddbReader;

/**
 * Application
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
@Configuration
@Import(borg.ed.universe.Application.class)
public class Application {

	static final Logger logger = LoggerFactory.getLogger(Application.class);

	private static final ApplicationContext APPCTX = new AnnotationConfigApplicationContext(Application.class);

	public static void main(String[] args) throws Exception {
		APPCTX.getBean(EddbReader.class).loadEddbDataIntoElasticsearch();
	}

	@Bean
	public EddbReader eddbReader() {
		return new EddbReader();
	}

}
