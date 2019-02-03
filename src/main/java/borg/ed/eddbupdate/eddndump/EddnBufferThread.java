package borg.ed.eddbupdate.eddndump;

import java.time.ZonedDateTime;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import borg.ed.universe.eddn.EddnElasticUpdater;
import borg.ed.universe.journal.events.AbstractJournalEvent;
import lombok.Getter;
import lombok.Setter;

public class EddnBufferThread extends Thread {

	static final Logger logger = LoggerFactory.getLogger(EddnBufferThread.class);

	public volatile boolean shutdown = false;

	@Autowired
	private EddnElasticUpdater eddnElasticUpdater = null;

	private LinkedList<BufferedEvent> buffer = new LinkedList<>();

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
