package net.kotek.jdbm.junk;


import net.kotek.jdbm.*;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

//TODO 112444910
public class RandomInsertLongs {
    
    public static void main(String[] args) throws IOException {
        DB db = new DBMaker("/hugo/large/test"+System.currentTimeMillis())
                .disableTransactions()
                .enableHardCache()
                .build();

        Set<Long> m = db.createTreeSet("test");               
        Random r = new Random(234);

        long printEvery = (long) 1e7;
        long readEvery = (long) 1e5;
        long t = System.currentTimeMillis();

        for(long i = 1;;i++){
            
            m.add(makeLong(i));


            //make a few random reads
            if(i%readEvery == 0 && i>200000000){
                for(long j = 1;j<readEvery;j++){
                    long key = r.nextInt((int) i);
                    if(key>0 && !m.contains(makeLong(key)))
                        throw new InternalError(""+key);
                }
            }

            //print time for last round
            if(i%printEvery==0){
                long time = System.currentTimeMillis();
                System.out.println(i + " - "+(time-t)+" ms");
                t = time;
            }
        }

    }
    
    public static Long makeLong(long value){
        return ((long)(int)(value ^ (value >>> 32))) +
                ((long)(int)(((value+1) ^ ((value+1) >>> 32)))<<32);
    }
}
