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

package jdbm.helper;


/**
 * Tuple consisting of a key-value pair.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: Tuple.java,v 1.2 2001/05/19 14:02:00 boisvert Exp $
 */
public final class Tuple<K,V> {

    /**
     * Key
     */
    private K _key;


    /**
     * Value
     */
    private V _value;


    /**
     * Construct an empty Tuple.
     */
    public Tuple() {
        // empty
    }


    /**
     * Construct a Tuple.
     *
     * @param key The key.
     * @param value The value.
     */
    public Tuple( K key, V value ) {
        _key = key;
        _value = value;
    }


    /**
     * Get the key.
     */
    public K getKey() {
        return _key;
    }


    /**
     * Set the key.
     */
    public void setKey( K key ) {
        _key = key;
    }


    /**
     * Get the value.
     */
    public V getValue() {
        return _value;
    }


    /**
     * Set the value.
     */
    public void setValue( V value ) {
        _value = value;
    }

}
