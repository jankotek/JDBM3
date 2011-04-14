package jdbm.helper;

import java.io.*;
import java.util.Random;

/**
 * An mechanism to protected storage from opening multiple times.
 * It uses lock file and watch dog threads.
 * Algorithm and documentation comes from H2 database by Thomas Mueller, reimplemented by Jan Kotek
 * <p>
 * If the lock file does not exist, it is created (using the atomic operation File.createNewFile). Then, the process waits a little bit (20 ms) and checks the file again. If the file was changed during this time, the operation is aborted. This protects against a race condition when one process deletes the lock file just after another one create it, and a third process creates the file again. It does not occur if there are only two writers.
 *  <p>
 * If the file can be created, a random number is inserted together with the locking method ('file'). Afterwards, a watchdog thread is started that checks regularly (every second once by default) if the file was deleted or modified by another (challenger) thread / process. Whenever that occurs, the file is overwritten with the old data. The watchdog thread runs with high priority so that a change to the lock file does not get through undetected even if the system is very busy. However, the watchdog thread does use very little resources (CPU time), because it waits most of the time. Also, the watchdog only reads from the hard disk and does not write to it.
 *  <p>
 * If the lock file exists and was recently modified, the process waits for some time (up to two seconds). If it was still changed, an exception is thrown (database is locked). This is done to eliminate race conditions with many concurrent writers. Afterwards, the file is overwritten with a new version (challenge). After that, the thread waits for 2 seconds. If there is a watchdog thread protecting the file, he will overwrite the change and this process will fail to lock the database. However, if there is no watchdog thread, the lock file will still be as written by this thread. In this case, the file is deleted and atomically created again. The watchdog thread is started in this case and the file is locked.
 * <p>
 * This algorithm is tested with over 100 concurrent threads. In some cases, when there are many concurrent threads trying to lock the database, they block each other (meaning the file cannot be locked by any of them) for some time. However, the file never gets locked by two threads at the same time. However using that many concurrent threads / processes is not the common use case. Generally, an application should throw an error to the user if it cannot open a database, and not try again in a (fast) loop.
 *
 */
public class LockFile {

    private final File lockFile;
    private final String randomStuff = ""+new Random().nextLong();

    private final long interval = 1000;
    private boolean locked = false;
    private boolean watchDogQuit = false;
    private final Object lock = new Object();


    private final Runnable watchDog = new Runnable() {
        public void run() {
            while(true)try{
                synchronized (lock){
                    //sleep for interval, and than check if out file was modified
                    lock.wait(interval);
                    //check if file is going to be unlocked
                    if(watchDogQuit) break;
                    if(wasFileModified())
                        createAndFillLockFile(); //if yes, overwrite with our changes
                }
            }catch(Exception e){
                //this thread must keep spinning
                e.printStackTrace();
            }
        }
    };


    public LockFile(File lockFile) {
        this.lockFile = lockFile;
    }

    public synchronized void lock() throws IOException {
        if(locked) throw new IllegalAccessError("already locked");
        //create new file
        if(!lockFile.exists()){
            //protect from race condition, give other watch dogs time to create file
            sleep(100);
        }

        if(!lockFile.exists()){
            //create new file and we are done
            createAndFillLockFile();
        }else{
            //lock file exists, overwrite it with our data
            createAndFillLockFile();
            //wait for other watch dogs to overwrite file
            sleep(interval * 4);
            //check if file was modified by other watch dog
            if(wasFileModified()){
                throw new IOException("Could not lock file, other instance is running: "+lockFile.getPath());
            }
        }

        //start watch dog thread
        Thread watchDogThread = new Thread(watchDog);
        watchDogThread.setName("JDBM file lock: "+lockFile);
        watchDogThread.setDaemon(true);
        watchDogThread.start();

        locked = true;
    }

    public synchronized void unlock(){
        if(!locked) throw new IllegalAccessError("not locked");

        //stop watch dog
        synchronized (lock){
            watchDogQuit = true;
            lock.notify();
        }

        if(lockFile.exists() && !lockFile.delete())
            lockFile.deleteOnExit();
        locked = false;
    }

    private void createAndFillLockFile() throws IOException {
        FileOutputStream out = new FileOutputStream(lockFile);
        out.write(randomStuff.getBytes());
        out.close();
    }

    private boolean wasFileModified() throws IOException{
        if(!lockFile.exists()) return true;
        FileInputStream in = new FileInputStream(lockFile);
        String content = new BufferedReader(new InputStreamReader(in)).readLine();
        in.close();
        return !randomStuff.equals(content);
    }

    private void sleep(long i) throws IOException {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
