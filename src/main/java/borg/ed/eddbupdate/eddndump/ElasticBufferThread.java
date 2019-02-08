package borg.ed.eddbupdate.eddndump;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import borg.ed.universe.model.Body;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.repository.BodyRepository;
import borg.ed.universe.repository.StarSystemRepository;

public class ElasticBufferThread extends Thread {

	static final Logger logger = LoggerFactory.getLogger(ElasticBufferThread.class);

	public volatile boolean shutdown = false;

	@Autowired
	private StarSystemRepository starSystemRepository = null;

	@Autowired
	private BodyRepository bodyRepository = null;

	private LinkedList<StarSystem> starSystemBuffer = new LinkedList<>();

	private LinkedList<Body> bodyBuffer = new LinkedList<>();

	public ElasticBufferThread() {
		this.setName("ElasticBufferThread");
		this.setDaemon(true);
	}

	@Override
	public void run() {
		logger.info(this.getName() + " started");

		this.flushBuffer();

		logger.info(this.getName() + " terminated");
	}

	void flushBuffer() {
		while (!Thread.currentThread().isInterrupted() && !this.shutdown) {
			try {
				if (this.starSystemBuffer.isEmpty()) {
					Thread.sleep(1);
				} else {
					List<StarSystem> starSystems = null;
					synchronized (this.starSystemBuffer) {
						starSystems = new ArrayList<>(this.starSystemBuffer);
						this.starSystemBuffer.clear();
						this.starSystemBuffer.notifyAll();
					}
					this.starSystemRepository.saveAll(starSystems);
				}

				if (this.bodyBuffer.isEmpty()) {
					Thread.sleep(1);
				} else {
					List<Body> bodies = null;
					synchronized (this.bodyBuffer) {
						bodies = new ArrayList<>(this.bodyBuffer);
						this.bodyBuffer.clear();
						this.bodyBuffer.notifyAll();
					}
					this.bodyRepository.saveAll(bodies);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public void bufferStarSystem(StarSystem starSystem) throws InterruptedException {
		synchronized (this.starSystemBuffer) {
			if (this.starSystemBuffer.size() >= 1000) {
				//logger.debug("StarSystem buffer full");
				this.starSystemBuffer.wait();
				//logger.debug("StarSystem buffer ready");
			}
			this.starSystemBuffer.addLast(starSystem);
			this.starSystemBuffer.notifyAll();
		}
	}

	public void bufferBody(Body body) throws InterruptedException {
		synchronized (this.bodyBuffer) {
			if (this.bodyBuffer.size() >= 1000) {
				//logger.debug("Body buffer full");
				this.bodyBuffer.wait();
				//logger.debug("Body buffer ready");
			}
			this.bodyBuffer.addLast(body);
			this.bodyBuffer.notifyAll();
		}
	}

}
