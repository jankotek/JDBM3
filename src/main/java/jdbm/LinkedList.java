/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package jdbm;


import java.io.*;
import java.util.*;

/**
 * LinkedList which stores its nodes on disk.
 *
 * @author Jan Kotek
 */
class LinkedList<E> extends AbstractSequentialList<E>{



    private RecordManager2 recman;
    private long listrecid = 0;

    private int size = 0;

    private long first = 0;
    private long last = 0;
    private Serializer<E> valueSerializer;

    /** indicates that entry values should not be loaded during deserialization, used during defragmentation*/
    protected boolean loadValues = true;

    LinkedList(long first, long last, int size, Serializer<E> valueSerializer){
        this.first = first;
        this.last = last;
        this.size  = size;
        this.valueSerializer = valueSerializer;
    }

    LinkedList(RecordManager2 recman, long listrecid, Serializer<E> valueSerializer){
        this.recman = recman;
        this.listrecid = listrecid;
        if(valueSerializer!=null && !(valueSerializer instanceof Serializable))
            throw new IllegalArgumentException("Serializer does not implement Serializable");
        this.valueSerializer = valueSerializer;

    }

    void setPersistenceContext(RecordManager2 recman, long listrecid){
        this.recman = recman;
        this.listrecid = listrecid;
    }


    public ListIterator<E> listIterator(int index) {
        if(index < 0 || index>size)
            throw new IndexOutOfBoundsException();


        Iter iter = new Iter();
        iter.next = first;


        //scroll to requested position
        //TODO scroll from end, if beyond half
        for(int i = 0;i<index;i++){
            iter.next();
        }
        return iter;

    }


    public int size() {
        return size;
    }

    public boolean add(Object value){
        try{
            Entry e = new Entry(last,0,value);
            long recid = recman.insert(e,entrySerializer);

            //update old last Entry to point to new record
            if(last!=0){
                Entry oldLast = recman.fetch(last,entrySerializer);
                if(oldLast.next!=0) throw new Error();
                oldLast.next = recid;
                recman.update(last,oldLast,entrySerializer);
            }

            //update linked list
            last = recid;
            if(first == 0) first = recid;
            size++;
            recman.update(listrecid,this);
            modCount++;
            return true;
        }catch(IOException e){
            throw new IOError(e);
        }

    }

    private Entry<E> fetch(long recid){
        try{
            return recman.fetch(recid,entrySerializer);
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    /** called from Serialization object*/
    static LinkedList deserialize(DataInput is) throws IOException, ClassNotFoundException {
        long first = LongPacker.unpackLong(is);
        long last = LongPacker.unpackLong(is);
        int size = LongPacker.unpackInt(is);
        Serializer serializer = (Serializer) Utils.CONSTRUCTOR_SERIALIZER.deserialize(is);
        return new LinkedList(first,last,size,serializer);
    }

    void serialize(DataOutput out) throws IOException {
        LongPacker.packLong(out,first);
        LongPacker.packLong(out,last);
        LongPacker.packInt(out, size);
        Utils.CONSTRUCTOR_SERIALIZER.serialize(out,valueSerializer);
    }

    private final Serializer<Entry> entrySerializer = new Serializer<Entry>(){

        public void serialize(DataOutput out, Entry e) throws IOException {
            LongPacker.packLong(out,e.prev);
            LongPacker.packLong(out,e.next);
            if(valueSerializer!=null)
                valueSerializer.serialize(out,(E)e.value);
            else
                recman.defaultSerializer().serialize(out,e.value);
        }

        public Entry<E> deserialize(DataInput in) throws IOException, ClassNotFoundException {
            long prev = LongPacker.unpackLong(in);
            long next = LongPacker.unpackLong(in);
            Object value = null;
            if(loadValues)
               value = valueSerializer == null ? recman.defaultSerializer().deserialize(in) : valueSerializer.deserialize(in);
            return  new LinkedList.Entry(prev, next,value);
        }
    };

    static class Entry<E>{
        long prev = 0;
        long next = 0;

        E value;

        public Entry(long prev, long next, E value) {
            this.prev = prev;
            this.next = next;
            this.value = value;
        }
    }

    private final class Iter implements ListIterator<E> {

        private int expectedModCount = modCount;
        private int index = 0;

        private long prev = 0;
        private long next = 0;

        private byte lastOper = 0;

        public boolean hasNext() {
            return next!=0;
        }



        public E next() {
            if(next == 0) throw new NoSuchElementException();
            checkForComodification();

            Entry<E> e = fetch(next);

            prev = next;
            next = e.next;
            index++;
            lastOper = +1;
            return e.value;
        }

        public boolean hasPrevious() {
            return prev!=0;
        }

        public E previous() {
            checkForComodification();
            Entry<E> e = fetch(prev);
            next = prev;
            prev = e.prev;
            index--;
            lastOper = -1;
            return e.value;
        }

        public int nextIndex() {
            return index;
        }

        public int previousIndex() {
            return index-1;
        }

        public void remove() {
            checkForComodification();
            try{
            if(lastOper==1){
                //last operation was next() so remove previous element
                lastOper = 0;

                Entry<E> p = recman.fetch(prev,entrySerializer);
                //update entry before previous
                if(p.prev!=0){
                    Entry<E> pp = recman.fetch(p.prev,entrySerializer);
                    pp.next = p.next;
                    recman.update(p.prev,pp,entrySerializer);
                }
                //update entry after next
                if(p.next!=0){
                    Entry<E> pn = recman.fetch(p.next,entrySerializer);
                    pn.prev = p.prev;
                    recman.update(p.next,pn,entrySerializer);
                }
                //remove old record from recman
                recman.delete(prev);
                //update list
                if(first == prev)
                    first = next;
                if(last == prev)
                    last = next;
                size--;
                recman.update(listrecid,LinkedList.this);
                modCount++;
                expectedModCount++;
                //update iterator
                prev = p.prev;

            }else if(lastOper==-1){
                //last operation was prev() so remove next element
                lastOper = 0;

                Entry<E> n = recman.fetch(next,entrySerializer);
                //update entry before next
                if(n.prev!=0){
                    Entry<E> pp = recman.fetch(n.prev,entrySerializer);
                    pp.next = n.next;
                    recman.update(n.prev,pp,entrySerializer);
                }
                //update entry after previous
                if(n.next!=0){
                    Entry<E> pn = recman.fetch(n.next,entrySerializer);
                    pn.prev = n.prev;
                    recman.update(n.next,pn,entrySerializer);
                }
                //remove old record from recman
                recman.delete(next);
                //update list
                if(last == next)
                    last = prev;
                if(first == next)
                    first = prev;
                size--;
                recman.update(listrecid,LinkedList.this);
                modCount++;
                expectedModCount++;
                //update iterator
                next = n.next;

            }else
                throw new IllegalStateException();
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        public void set(E value) {
            checkForComodification();
            try{
            if(lastOper==1){
                //last operation was next(), so update previous item
                lastOper = 0;
                Entry<E> n = recman.fetch(prev,entrySerializer);
                n.value = value;
                recman.update(prev,n,entrySerializer);
            }else if(lastOper == -1){
                //last operation was prev() so update next item
                lastOper = 0;
                Entry<E> n = recman.fetch(next,entrySerializer);
                n.value = value;
                recman.update(next,n,entrySerializer);
            }else
                throw new IllegalStateException();
            }catch(IOException e){
                throw new IOError(e);
            }

        }

        public void add(E value) {
            checkForComodification();
            //use more efficient method if possible
            if(next == 0){
                LinkedList.this.add(value);
                expectedModCount++;
                return;
            }
            try{
                //insert new entry
                Entry<E> e = new Entry<E>(prev,next,value);
                long recid = recman.insert(e,entrySerializer);

                //update previous entry
                if(prev!=0){
                    Entry<E> p = recman.fetch(prev,entrySerializer);
                    if(p.next!=next) throw new Error();
                    p.next = recid;
                    recman.update(prev,p,entrySerializer);
                }

                //update next entry
                Entry<E> n = fetch(next);
                if(n.prev!=prev) throw new Error();
                n.prev = recid;
                recman.update(next,n,entrySerializer);

                //update List
                size++;
                recman.update(listrecid,LinkedList.this);

                //update iterator
                expectedModCount++;
                modCount++;
                prev = recid;

            }catch(IOException e){
                throw new IOError(e);
            }

        }

        final void checkForComodification() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
        }
    }

    /**
     * Copyes collection from one Recman to other, while keeping logical recids unchanged
     */
    static void defrag(long recid, RecordManagerStorage r1, RecordManagerStorage r2) throws IOException{
        try{
        byte[] data = r1.fetchRaw(recid);
        r2.forceInsert(recid,data);
        DataInputOutput in = new DataInputOutput();
        in.reset(data);
        LinkedList l = (LinkedList) r1.defaultSerializer().deserialize(in);
        l.loadValues = false;
        long current = l.first;
        long counter = 1;
        while(current!=0){
            data = r1.fetchRaw(current);
            in.reset(data);
            r2.forceInsert(current,data);

            Entry e = (Entry) l.entrySerializer.deserialize(in);
            current = e.next;
            if(counter++ %10000 == 0){
                r2.commit(); //make commit sometimes, so we wont run out of memory
            }
        }
        }catch(ClassNotFoundException e){
            throw new IOError(e);
        }

    }

}