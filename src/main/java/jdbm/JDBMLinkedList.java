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
class JDBMLinkedList<E> extends AbstractSequentialList<E>{


    private RecordManager recman;
    private long listrecid = 0;

    private int size = 0;

    private long first = 0;
    private long last = 0;

    JDBMLinkedList(long first, long last, int size){
        this.first = first;
        this.last = last;
        this.size  = size;
    }

    JDBMLinkedList(RecordManager recman, long listrecid){
        this.recman = recman;
        this.listrecid = listrecid;
    }

    void setRecmanAndListRedic(RecordManager recman, long listrecid){
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
            long recid = recman.insert(e);

            //update old last Entry to point to new record
            if(last!=0){
                Entry oldLast = recman.fetch(last);
                if(oldLast.next!=0) throw new Error();
                oldLast.next = recid;
                recman.update(last,oldLast);
            }

            //update linked list
            last = recid;
            if(first == 0) first = recid;
            size++;
            recman.update(listrecid,this);
            return true;
        }catch(IOException e){
            throw new IOError(e);
        }

    }

    private Entry<E> fetch(long recid){
        try{
            return recman.fetch(recid);
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    /** called from Serialization object*/
    static JDBMLinkedList deserialize(DataInput is) throws IOException {
        long first = LongPacker.unpackLong(is);
        long last = LongPacker.unpackLong(is);
        int size = LongPacker.unpackInt(is);
        return new JDBMLinkedList(first,last,size);
    }

    void serialize(DataOutput out) throws IOException {
        LongPacker.packLong(out,first);
        LongPacker.packLong(out,last);
        LongPacker.packInt(out, size);
    }



    //TODO entry is currently serialized with header,this adds one byte to each item, use custom serializer

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

    private class Iter implements ListIterator<E> {
        int index = 0;

        long prev = 0;
        long next = 0;

        byte lastOper = 0;

        public boolean hasNext() {
            return next!=0;
        }



        public E next() {
            if(next == 0) throw new NoSuchElementException();

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
            try{
            if(lastOper==1){
                //last operation was next() so remove previous element
                lastOper = 0;

                Entry<E> p = recman.fetch(prev);
                //update entry before previous
                if(p.prev!=0){
                    Entry<E> pp = recman.fetch(p.prev);
                    pp.next = p.next;
                    recman.update(p.prev,pp);
                }
                //update entry after next
                if(p.next!=0){
                    Entry<E> pn = recman.fetch(p.next);
                    pn.prev = p.prev;
                    recman.update(p.next,pn);
                }
                //remove old record from recman
                recman.delete(prev);
                //update list
                if(first == prev)
                    first = next;
                if(last == prev)
                    last = next;
                size--;
                recman.update(listrecid,JDBMLinkedList.this);
                //update iterator
                prev = p.prev;

            }else if(lastOper==-1){
                //last operation was prev() so remove next element
                lastOper = 0;

                Entry<E> n = recman.fetch(next);
                //update entry before next
                if(n.prev!=0){
                    Entry<E> pp = recman.fetch(n.prev);
                    pp.next = n.next;
                    recman.update(n.prev,pp);
                }
                //update entry after previous
                if(n.next!=0){
                    Entry<E> pn = recman.fetch(n.next);
                    pn.prev = n.prev;
                    recman.update(n.next,pn);
                }
                //remove old record from recman
                recman.delete(next);
                //update list
                if(last == next)
                    last = prev;
                if(first == next)
                    first = prev;
                size--;
                recman.update(listrecid,JDBMLinkedList.this);
                //update iterator
                next = n.next;

            }else
                throw new IllegalStateException();
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        public void set(E value) {
            try{
            if(lastOper==1){
                //last operation was next(), so update previous item
                lastOper = 0;
                Entry<E> n = recman.fetch(prev);
                n.value = value;
                recman.update(prev,n);
            }else if(lastOper == -1){
                //last operation was prev() so update next item
                lastOper = 0;
                Entry<E> n = recman.fetch(next);
                n.value = value;
                recman.update(next,n);
            }else
                throw new IllegalStateException();
            }catch(IOException e){
                throw new IOError(e);
            }

        }

        public void add(E value) {
            //use more efficient method if possible
            if(next == 0){
                JDBMLinkedList.this.add(value);
                return;
            }
            try{
                //insert new entry
                Entry<E> e = new Entry<E>(prev,next,value);
                long recid = recman.insert(e);

                //update previous entry
                if(prev!=0){
                    Entry<E> p = recman.fetch(prev);
                    if(p.next!=next) throw new Error();
                    p.next = recid;
                    recman.update(prev,p);
                }

                //update next entry
                Entry<E> n = fetch(next);
                if(n.prev!=prev) throw new Error();
                n.prev = recid;
                recman.update(next,n);

                //update List
                size++;
                recman.update(listrecid,JDBMLinkedList.this);

                //update iterator
                prev = recid;

            }catch(IOException e){
                throw new IOError(e);
            }

        }
    }

    /**
     * Copyes collection from one Recman to other, while keeping logical recids unchanged
     */
    static void defrag(long recid, BaseRecordManager r1, BaseRecordManager r2) throws IOException{
        try{
        byte[] data = r1.fetchRaw(recid);
        r2.forceInsert(recid,data);
        DataInput in = new DataInputStream(new ByteArrayInputStream(data));
        JDBMLinkedList l = (JDBMLinkedList) r1.defaultSerializer().deserialize(in);
        long current = l.first;
        while(current!=0){
            data = r1.fetchRaw(current);
            in = new DataInputStream(new ByteArrayInputStream(data));
            r2.forceInsert(current,data);
            //TODO this deserializes list entry, but we only need header (optimize with partial deserialization)
            Entry e = (Entry) r1.defaultSerializer().deserialize(in);
            current = e.next;
        }
        }catch(ClassNotFoundException e){
            throw new IOError(e);
        }

    }

}