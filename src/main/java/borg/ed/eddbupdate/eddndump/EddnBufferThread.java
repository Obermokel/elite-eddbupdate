package borg.ed.eddbupdate.eddndump;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import borg.ed.universe.eddn.EddnElasticUpdater;
import borg.ed.universe.journal.events.AbstractJournalEvent;
import borg.ed.universe.model.Body;
import borg.ed.universe.model.StarSystem;
import borg.ed.universe.repository.BodyRepository;
import borg.ed.universe.repository.StarSystemRepository;
import lombok.Getter;
import lombok.Setter;

public class EddnBufferThread extends Thread {

	static final Logger logger = LoggerFactory.getLogger(EddnBufferThread.class);

	public volatile boolean shutdown = false;

	@Autowired
	private EddnElasticUpdater eddnElasticUpdater = null;

	@Autowired
	private StarSystemRepository starSystemRepository = null;

	@Autowired
	private BodyRepository bodyRepository = null;

	private LinkedList<BufferedEvent> buffer = new LinkedList<>();

	private LinkedList<StarSystem> starSystemBuffer = new LinkedList<>();

	private LinkedList<Body> bodyBuffer = new LinkedList<>();

	public EddnBufferThread() {
		this.setName("EddnBufferThread");
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
				if (this.buffer.isEmpty()) {
					Thread.sleep(1);
				} else {
					BufferedEvent el = null;
					synchronized (this.buffer) {
						el = this.buffer.removeFirst();
						if (this.buffer.size() >= 10) {
							this.buffer.notifyAll();
						}
					}
					this.eddnElasticUpdater.onNewJournalMessage(el.getGatewayTimestamp(), el.getUploaderID(), el.getJournalEvent());
				}

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

	public void buffer(ZonedDateTime gatewayTimestamp, String uploaderID, AbstractJournalEvent journalEvent) throws InterruptedException {
		synchronized (this.buffer) {
			if (this.buffer.size() >= 1000) {
				//logger.debug("Buffer full");
				this.buffer.wait();
				//logger.debug("Buffer ready");
			}
			this.buffer.addLast(new BufferedEvent(gatewayTimestamp, uploaderID, journalEvent));
			this.buffer.notifyAll();
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

	@Getter
	@Setter
	public static class BufferedEvent {

		private ZonedDateTime gatewayTimestamp = null;
		private String uploaderID = null;
		private AbstractJournalEvent journalEvent = null;

		public BufferedEvent(ZonedDateTime gatewayTimestamp, String uploaderID, AbstractJournalEvent journalEvent) {
			this.gatewayTimestamp = gatewayTimestamp;
			this.uploaderID = uploaderID;
			this.journalEvent = journalEvent;
		}

	}

}
