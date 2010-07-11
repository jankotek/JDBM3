/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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

package jdbm.helper;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * B-Tree Map which uses primitive long as key. 
 * Main advantage is new instanceof of Long does not have to be created for each lookup.
 * <p>
 * This code comes from Android, which in turns comes to Apache Harmony. 
 * This class was modified to use primitive longs and stripped down to consume less space. 
 * <p>
 * Author of JDBM modifications: Jan Kotek
 * <p>
 * It is much slower then LongKeyChainedHashMap, but may be usefull in future for better licence. 
 * 
 *
 * @param <V>
 */
public class LongTreeMap<V> {

    private Entry<V> root;
    
    private int size;
    
    /** counts modifications to throw ConcurrentAccessException */
    private transient int modCount;
   
    
    /**
     * Returns the value of the mapping with the specified key.
     * 
     * @param key
     *            the key.
     * @return the value of the mapping with the specified key.
     * @throws ClassCastException
     *             if the key cannot be compared with the keys in this map.
     * @throws NullPointerException
     *             if the key is {@code null} and the comparator cannot handle
     *             {@code null}.
     * @since Android 1.0
     */
    public V get(long key) {
        Entry<V> node = find(key);
        if (node != null) {
            return node.value;
        }
        return null;
    }
    /**
     * Maps the specified key to the specified value.
     * 
     * @param key
     *            the key.
     * @param value
     *            the value.
     * @return the value of any previous mapping with the specified key or
     *         {@code null} if there was no mapping.
     * @throws ClassCastException
     *             if the specified key cannot be compared with the keys in this
     *             map.
     * @throws NullPointerException
     *             if the specified key is {@code null} and the comparator
     *             cannot handle {@code null} keys.
     * @since Android 1.0
     */
    public V put(long key, V value) {
        Entry<V> entry = rbInsert(key);
        V result = entry.value;
        entry.value = value;
        return result;
    }
    
    /**
     * Removes the mapping with the specified key from this map.
     * 
     * @param key
     *            the key of the mapping to remove.
     * @return the value of the removed mapping or {@code null} if no mapping
     *         for the specified key was found.
     * @throws ClassCastException
     *             if the specified key cannot be compared with the keys in this
     *             map.
     * @throws NullPointerException
     *             if the specified key is {@code null} and the comparator
     *             cannot handle {@code null} keys.
     * @since Android 1.0
     */
    public V remove(long key) {
        if (size == 0) {
            return null;
        }
        Entry<V> node = find(key);
        if (node == null) {
            return null;
        }
        V result = node.value;
        rbDelete(node);
        return result;
    }

    /**
     * Removes all mappings from this TreeMap, leaving it empty.
     * 
     * @see Map#isEmpty()
     * @see #size()
     * @since Android 1.0
     */
    	public void clear() {
    		root = null;
    		size = 0;
    		modCount++;
    	}
    
    /**
     * Entry is an internal class which is used to hold the entries of a
     * TreeMap.
     */
    private static class Entry<V>  {
        Entry<V> parent, left, right;
        
        long key;
        V value;

        boolean color;
    
        Entry(long key, V value) {
            this.key = key;
            this.value = value;
        }

        public String toString() {      
        	return super.toString()+" - "+key+" - "+value;
        }       
    }
    
  
	    /** 
	     * @return iterator over values in map	    
	     */
	    public Iterator<V> valuesIterator() {
	    	return new ValueIterator ();
	    }
	    
	    /** 
	     * @return iterator over keys in map	    
	     */
	    public LongIterator keyIterator() {
	    	return new LongIterator ();
	    }
	    	   
	  	   	    
	    private class MapIterator {

	        int expectedModCount;
	        Entry< V> node;
	        Entry< V> lastNode;

	        MapIterator() {
	            expectedModCount = modCount;
	            if(root!=null)
	            	node =  minimum(root);
	        }

	        public boolean hasNext() {
	            return node != null;
	        }

	        final public void remove() {
	            if (expectedModCount == modCount) {
	                if (lastNode != null) {
	                    rbDelete(lastNode);
	                    lastNode = null;
	                    expectedModCount++;
	                } else {
	                    throw new IllegalStateException();
	                }
	            } else {
	                throw new ConcurrentModificationException();
	            }
	        }

	        final void makeNext() {
	            if (expectedModCount != modCount) {
	                throw new ConcurrentModificationException();
	            } else if (node == null) {
	                throw new NoSuchElementException();
	            }
	            lastNode = node;
	            node = successor(node);
	            }
	    }
	    
	    private class ValueIterator extends MapIterator implements Iterator<V>{
            public V next() {
                makeNext();
                return lastNode.value;
            }
	    }
	    
	    public class LongIterator extends MapIterator implements Iterator<Long>{
            public Long next() {
                makeNext();
                return lastNode.key;
            }
            
            public long nextLong() {
                makeNext();
                return lastNode.key;
            }

	    }

		public boolean isEmpty() {
			return size == 0;
		}

		public int size(){
			return size;
		}
	    
		
		public String toString(){
			String s = this.getClass().getName();
			s+="[";
			LongIterator iter = keyIterator();
			boolean first = true; 

			while(iter.hasNext()){
				if(!first){
					s+=", ";
				}
				first=  false;
				long k = iter.nextLong();
				s+=k+"="+get(k);			
			}
			s+="]";
			return s;
		}
		
		
	    private Entry<V> find(long object) {
	        Entry<V> x = root;
	        while (x != null) {
//	            result = object != null ? object.compareTo(x.key) : comparator
//	                    .compare(key, x.key);
//	            if (result == 0) {
//	                return x;
//	            }
//	            x = result < 0 ? x.left : x.right;
	        	if(object == x.key)
	        		return x;
	        	x = object<x.key?x.left : x.right;
	        }
	        return null;
	    }
	    
	    private Entry<V> minimum(Entry<V> x) {
	        while (x.left != null) {
	            x = x.left;
	        }
	        return x;
	    }

	    Entry<V> successor(Entry<V> x) {
	        if (x.right != null) {
	            return minimum(x.right);
	        }
	        Entry<V> y = x.parent;
	        while (y != null && x == y.right) {
	            x = y;
	            y = y.parent;
	        }
	        return y;
	    }

	    void rbDelete(Entry< V> z) {
	        Entry<V> y = z.left == null || z.right == null ? z : successor(z);
	        Entry<V> x = y.left != null ? y.left : y.right;
	        if (x != null) {
	            x.parent = y.parent;
	        }
	        if (y.parent == null) {
	            root = x;
	        } else if (y == y.parent.left) {
	            y.parent.left = x;
	        } else {
	            y.parent.right = x;
	        }
	        modCount++;
	        if (y != z) {
	            z.key = y.key;
	            z.value = y.value;
	        }
	        if (!y.color && root != null) {
	            if (x == null) {
	                fixup(y.parent);
	            } else {
	                fixup(x);
	            }
	        }
	        size--;
	    }

	    private void fixup(Entry<V> x) {
	        Entry<V> w;
	        while (x != root && !x.color) {
	            if (x == x.parent.left) {
	                w = x.parent.right;
	                if (w == null) {
	                    x = x.parent;
	                    continue;
	                }
	                if (w.color) {
	                    w.color = false;
	                    x.parent.color = true;
	                    leftRotate(x.parent);
	                    w = x.parent.right;
	                    if (w == null) {
	                        x = x.parent;
	                        continue;
	                    }
	                }
	                if ((w.left == null || !w.left.color)
	                        && (w.right == null || !w.right.color)) {
	                    w.color = true;
	                    x = x.parent;
	                } else {
	                    if (w.right == null || !w.right.color) {
	                        w.left.color = false;
	                        w.color = true;
	                        rightRotate(w);
	                        w = x.parent.right;
	                    }
	                    w.color = x.parent.color;
	                    x.parent.color = false;
	                    w.right.color = false;
	                    leftRotate(x.parent);
	                    x = root;
	                }
	            } else {
	                w = x.parent.left;
	                if (w == null) {
	                    x = x.parent;
	                    continue;
	                }
	                if (w.color) {
	                    w.color = false;
	                    x.parent.color = true;
	                    rightRotate(x.parent);
	                    w = x.parent.left;
	                    if (w == null) {
	                        x = x.parent;
	                        continue;
	                    }
	                }
	                if ((w.left == null || !w.left.color)
	                        && (w.right == null || !w.right.color)) {
	                    w.color = true;
	                    x = x.parent;
	                } else {
	                    if (w.left == null || !w.left.color) {
	                        w.right.color = false;
	                        w.color = true;
	                        leftRotate(w);
	                        w = x.parent.left;
	                    }
	                    w.color = x.parent.color;
	                    x.parent.color = false;
	                    w.left.color = false;
	                    rightRotate(x.parent);
	                    x = root;
	                }
	            }
	        }
	        x.color = false;
	    }

	    private void leftRotate(Entry<V> x) {
	        Entry<V> y = x.right;
	        x.right = y.left;
	        if (y.left != null) {
	            y.left.parent = x;
	        }
	        y.parent = x.parent;
	        if (x.parent == null) {
	            root = y;
	        } else {
	            if (x == x.parent.left) {
	                x.parent.left = y;
	            } else {
	                x.parent.right = y;
	            }
	        }
	        y.left = x;
	        x.parent = y;
	    }

	    private void rightRotate(Entry<V> x) {
	        Entry<V> y = x.left;
	        x.left = y.right;
	        if (y.right != null) {
	            y.right.parent = x;
	        }
	        y.parent = x.parent;
	        if (x.parent == null) {
	            root = y;
	        } else {
	            if (x == x.parent.right) {
	                x.parent.right = y;
	            } else {
	                x.parent.left = y;
	            }
	        }
	        y.right = x;
	        x.parent = y;
	    }

	    private Entry<V> rbInsert(long object) {
	        boolean smaller = false;
	        Entry<V> y = null;
	        if (size != 0) {
	            Entry<V> x = root;
	            while (x != null) {
	                y = x;
//	                result = key != null ? key.compareTo(x.key) : comparator
//	                        .compare(object, x.key);
//	                if (result == 0) {
//	                    return x;
//	                }
//	                x = result < 0 ? x.left : x.right;
	                if(object == x.key)
	                	return x;
	                if(object<x.key){
	                	x = x.left;
	                	smaller = true;
	                }else{
	                	x = x.right;
	                	smaller = false;
	                }
	            }
	        }

	        size++;
	        modCount++;
	        Entry<V> z = new Entry<V>(object,null);
	        if (y == null) {
	            return root = z;
	        }
	        z.parent = y;
	        if (smaller) {
	            y.left = z;
	        } else {
	            y.right = z;
	        }
	        balance(z);
	        return z;
	    }

	    void balance(Entry<V> x) {
	        Entry<V> y;
	        x.color = true;
	        while (x != root && x.parent.color) {
	            if (x.parent == x.parent.parent.left) {
	                y = x.parent.parent.right;
	                if (y != null && y.color) {
	                    x.parent.color = false;
	                    y.color = false;
	                    x.parent.parent.color = true;
	                    x = x.parent.parent;
	                } else {
	                    if (x == x.parent.right) {
	                        x = x.parent;
	                        leftRotate(x);
	                    }
	                    x.parent.color = false;
	                    x.parent.parent.color = true;
	                    rightRotate(x.parent.parent);
	                }
	            } else {
	                y = x.parent.parent.left;
	                if (y != null && y.color) {
	                    x.parent.color = false;
	                    y.color = false;
	                    x.parent.parent.color = true;
	                    x = x.parent.parent;
	                } else {
	                    if (x == x.parent.left) {
	                        x = x.parent;
	                        rightRotate(x);
	                    }
	                    x.parent.color = false;
	                    x.parent.parent.color = true;
	                    leftRotate(x.parent.parent);
	                }
	            }
	        }
	        root.color = false;
	    }

	    
}

