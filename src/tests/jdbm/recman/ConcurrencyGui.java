package jdbm.recman;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.*;
import javax.swing.border.TitledBorder;

import jdbm.Serializer;
import jdbm.helper.RecordManagerImpl;

public class ConcurrencyGui extends JPanel {

	public static void main(String[] args) {
		JFrame f = new JFrame();
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		f.getContentPane().add(new ConcurrencyGui());
		f.pack();
		f.setVisible(true);
	}

	CacheRecordManager3 recman = new CacheRecordManager3(new Stub());

	DefaultListModel statusList = new DefaultListModel();
	JList status = new JList(statusList);

	ConcurrencyGui() {
		setLayout(new BorderLayout());

		JPanel pp = new JPanel();
		add(pp, BorderLayout.CENTER);
		add(status, BorderLayout.SOUTH);

		pp.setLayout(new GridLayout(2, 5));
		JPanel xx = new JPanel();
		xx.setLayout(new GridLayout(4, 1));
		pp.add(xx);
		xx.setBorder(new TitledBorder("Exclusive Locks"));
		for (int i = 0; i < 4; i++) {
			xx.add(new LowLevelExclusiveLock());
		}

		for (int recno = 0; recno < 4; recno++) {
			xx = new JPanel();
			xx.setLayout(new GridLayout(4, 1));
			pp.add(xx);
			xx.setBorder(new TitledBorder("Locks on rec " + recno));
			for (int i = 0; i < 4; i++) {
				xx.add(new LowLevelNonexclusiveLock(recno));
			}
		}

		xx = new JPanel();
		xx.setLayout(new GridLayout(4, 1));
		pp.add(xx);
		xx.setBorder(new TitledBorder("Lockout ops"));
		for (int i = 0; i < 2; i++) {
			xx.add(new Defrag());
		}
		for (int i = 0; i < 2; i++) {
			xx.add(new Clearer());
		}

		for (int recno = 0; recno < 4; recno++) {
			xx = new JPanel();
			xx.setLayout(new GridLayout(4, 1));
			pp.add(xx);
			xx.setBorder(new TitledBorder("Fetch " + recno));
			for (int i = 0; i < 4; i++) {
				xx.add(new Fetcher(recno));
			}
		}
	}

	abstract class HighLevelThing extends JPanel {
		final JButton btn = new JButton();

		abstract void doThing() throws IOException;

		HighLevelThing(String t) {
			btn.setText(t);
			add(btn);
			btn.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					click();
				}

			});
		}

		private void click() {
			btn.setEnabled(false);
			setBackground(Color.red);
			new Thread(new Runnable() {
				public void run() {
					try {
						doThing();
					}
					catch (final IOException e) {}
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							btn.setEnabled(true);
							setBackground(null);
						}
					});

				}
			}).start();
		}
	}

	class Fetcher extends HighLevelThing {
		final long recno;

		Fetcher(long recno) {
			super("Fetch");
			this.recno = recno;
		}

		void doThing() throws IOException {
			recman.fetch(recno);
		}
	}

	class Defrag extends HighLevelThing {
		Defrag() {
			super("Defrag");
		}

		void doThing() throws IOException {
			recman.defrag();
		}
	}

	class Clearer extends HighLevelThing {
		Clearer() {
			super("Clear Cache");
		}

		void doThing() throws IOException {
			recman.clearCache();
		}
	}

	abstract class LowLevelThing extends JPanel {
		JToggleButton btn = new JToggleButton("Acquire Lock");

		LowLevelThing() {
			add(btn);

			btn.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					pressed();
				}

			});

		}

		private void pressed() {
			if (btn.isSelected()) {
				btn.setText("Blocked");
				setBackground(Color.red);
				btn.setEnabled(false);
				new Thread(new Runnable() {
					public void run() {
						try {
							getLock();
						}
						catch (InterruptedException e) {
							e.printStackTrace();
							setBackground(Color.black);
							btn.setText("Failed!");
							return;
						}

						SwingUtilities.invokeLater(new Runnable() {
							public void run() {
								btn.setEnabled(true);
								setBackground(Color.green);
								btn.setText("Release lock");
							}
						});
					}
				}).start();
			}
			else {
				btn.setEnabled(false);
				setBackground(Color.yellow);
				btn.setText("Releasing...");
				new Thread(new Runnable() {
					public void run() {
						releaseLock();
						SwingUtilities.invokeLater(new Runnable() {
							public void run() {
								btn.setEnabled(true);
								setBackground(getParent().getBackground());
								btn.setText("Acquire Lock");
							}
						});
					}
				}).start();

			}
		}

		abstract void getLock() throws InterruptedException;

		abstract void releaseLock();
	}

	class LowLevelExclusiveLock extends LowLevelThing {
		LowLevelExclusiveLock() {}

		@Override
		void getLock() throws InterruptedException {
			recman.dbLock.acquireExclusiveLock();
		}

		@Override
		void releaseLock() {
			recman.dbLock.releaseExclusiveLock();
		}
	}

	class LowLevelNonexclusiveLock extends LowLevelThing {
		final long rec;

		CacheRecordManager3.RefWrapper w;

		LowLevelNonexclusiveLock(long rec) {
			this.rec = rec;
		}

		@Override
		void getLock() throws InterruptedException {
			w = recman.lock(rec);
		}

		@Override
		void releaseLock() {
			recman.unlock(w);
		}

	}

	class Stub extends RecordManagerImpl {
		public void close() throws IOException {

		}

		public void commit() throws IOException {}

		public void defrag() throws IOException {
			String s = new String("DEFRAGGING");
			statusList.addElement(s);
			try {
				Thread.sleep(3000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			statusList.removeElement(s);
		}

		public void clearCache() throws IOException {
			String s = new String("CLEARING CACHE");
			statusList.addElement(s);
			try {
				Thread.sleep(3000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			statusList.removeElement(s);
		}

		public void delete(long recid) throws IOException {}

		public <A> A fetch(long recid, Serializer<A> serializer) throws IOException {
			String s = "Fetching item " + recid;
			statusList.addElement(s);
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			statusList.removeElement(s);
			return null;
		}

		public <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache) throws IOException {
			try {
				String s = "Fetching item " + recid;
				statusList.addElement(s);
				Thread.sleep(1000);
				statusList.removeElement(s);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}

		public long getNamedObject(String name) throws IOException {
			return 0;
		}

		public <A> long insert(A obj, Serializer<A> serializer) throws IOException {
			return 0;
		}

		public void rollback() throws IOException {

		}

		public void setNamedObject(String name, long recid) throws IOException {

		}

		public <A> void update(long recid, A obj, Serializer<A> serializer) throws IOException {

		}
	}
}