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


package jdbm.btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;
import jdbm.helper.ComparableComparator;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.LongPacker;
import jdbm.helper.OpenByteArrayInputStream;
import jdbm.helper.OpenByteArrayOutputStream;
import jdbm.helper.Serialization;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

/**
 * Page of a Btree.
 * <p>
 * The page contains a number of key-value pairs.  Keys are ordered to allow
 * dichotomic search.
 * <p>
 * If the page is a leaf page, the keys and values are user-defined and
 * represent entries inserted by the user.
 * <p>
 * If the page is non-leaf, each key represents the greatest key in the
 * underlying BPages and the values are recids pointing to the children BPages.
 * The only exception is the rightmost BPage, which is considered to have an
 * "infinite" key value, meaning that any insert will be to the left of this
 * pseudo-key
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: BPage.java,v 1.6 2003/09/21 15:46:59 boisvert Exp $
 */
public final class BPage<K,V>
    implements Serializer<BPage<K,V>>
{

    private static final boolean DEBUG = false;



    /**
     * Parent B+Tree.
     */
    transient BTree<K,V> _btree;


    /**
     * This BPage's record ID in the PageManager.
     */
    protected transient long _recid;


    /**
     * Flag indicating if this is a leaf BPage.
     */
    protected boolean _isLeaf;


    /**
     * Keys of children nodes
     */
    protected K[] _keys;


    /**
     * Values associated with keys.  (Only valid if leaf BPage)
     */
    protected V[] _values;


    /**
     * Children pages (recids) associated with keys.  (Only valid if non-leaf BPage)
     */
    protected long[] _children;

    
    /**
     * Index of first used item at the page
     */
    protected int _first;


    /**
     * Previous leaf BPage (only if this BPage is a leaf)
     */
    protected long _previous;


    /**
     * Next leaf BPage (only if this BPage is a leaf)
     */
    protected long _next;

    /**
     * Return the B+Tree that is the owner of this {@link BPage}.
     */
    public BTree<K,V> getBTree() {
        return _btree;
    }

    /**
     * No-argument constructor used by serialization.
     */
    public BPage()
    {
        // empty
    }


    /**
     * Root page overflow constructor
     */
    @SuppressWarnings("unchecked")
	BPage( BTree<K,V> btree, BPage<K,V> root, BPage<K,V> overflow )
        throws IOException
    {
        _btree = btree;

        _isLeaf = false;

        _first = _btree._pageSize-2;

        _keys = (K[]) new Object[ _btree._pageSize ];
        _keys[ _btree._pageSize-2 ] = overflow.getLargestKey();
        _keys[ _btree._pageSize-1 ] = root.getLargestKey();

        _children = new long[ _btree._pageSize ];
        _children[ _btree._pageSize-2 ] = overflow._recid;
        _children[ _btree._pageSize-1 ] = root._recid;

        _recid = _btree._recman.insert( this, this );
    }


    /**
     * Root page (first insert) constructor.
     */
    @SuppressWarnings("unchecked")
	BPage( BTree<K,V> btree, K key, V value )
        throws IOException
    {
        _btree = btree;

        _isLeaf = true;

        _first = btree._pageSize-2;

        _keys = (K[]) new Object[ _btree._pageSize ];
        _keys[ _btree._pageSize-2 ] = key;
        _keys[ _btree._pageSize-1 ] = null;  // I am the root BPage for now

        _values = (V[]) new Object[ _btree._pageSize ];
        _values[ _btree._pageSize-2 ] = value;
        _values[ _btree._pageSize-1 ] = null;  // I am the root BPage for now

        _recid = _btree._recman.insert( this, this );
    }


    /**
     * Overflow page constructor.  Creates an empty BPage.
     */
    @SuppressWarnings("unchecked")
	BPage( BTree<K,V> btree, boolean isLeaf )
        throws IOException
    {
        _btree = btree;

        _isLeaf = isLeaf;

        // page will initially be half-full
        _first = _btree._pageSize/2;

        _keys = (K[]) new Object[ _btree._pageSize ];
        if ( isLeaf ) {
            _values = (V[]) new Object[ _btree._pageSize ];
        } else {
            _children = new long[ _btree._pageSize ];
        }

        _recid = _btree._recman.insert( this, this );
    }


    /**
     * Get largest key under this BPage.  Null is considered to be the
     * greatest possible key.
     */
    K getLargestKey()
    {
        return _keys[ _btree._pageSize-1 ];
    }


    /**
     * Return true if BPage is empty.
     */
    boolean isEmpty()
    {
        if ( _isLeaf ) {
            return ( _first == _values.length-1 );
        } else {
            return ( _first == _children.length-1 );
        }
    }


    /**
     * Return true if BPage is full.
     */
    boolean isFull() {
        return ( _first == 0 );
    }


    /**
     * Find the object associated with the given key.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key The key
     * @return TupleBrowser positionned just before the given key, or before
     *                      next greater key if key isn't found.
     */
    TupleBrowser<K,V> find( int height, K key )
        throws IOException
    {
        int index = findChildren( key );

        /*
        if ( DEBUG ) {
            System.out.println( "BPage.find() current: " + this
                                + " height: " + height);
        }
        */

        height -= 1;

        if ( height == 0 ) {
            // leaf BPage
            return new Browser<K,V>( this, index );
        } else {
            // non-leaf BPage
            BPage<K,V> child = childBPage( index );
            return child.find( height, key );
        }
    }


    /**
     * Find value associated with the given key.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key The key
     * @return TupleBrowser positionned just before the given key, or before
     *                      next greater key if key isn't found.
     */
    V findValue( int height, K key )
        throws IOException
    {
        int index = findChildren( key );

        /*
        if ( DEBUG ) {
            System.out.println( "BPage.find() current: " + this
                                + " height: " + height);
        }
        */

        height -= 1;

        if ( height == 0 ) {

            K key2 =   _keys[ index ];
//          // find returns the matching key or the next ordered key, so we must
//          // check if we have an exact match
          if ( key2==null || _btree._comparator.compare( key, key2 ) != 0 ) 
              return null;
            
            return _values[ index ];

            // leaf BPage
            //return new Browser<K,V>( this, index );
        } else {
            // non-leaf BPage
            BPage<K,V> child = childBPage( index );
            return child.findValue( height, key );
        }
    }


    /**
     * Find first entry and return a browser positioned before it.
     *
     * @return TupleBrowser positionned just before the first entry.
     */
    TupleBrowser<K,V> findFirst()
        throws IOException
    {
        if ( _isLeaf ) {
            return new Browser<K,V>( this, _first );
        } else {
            BPage<K,V> child = childBPage( _first );
            return child.findFirst();
        }
    }

    /** 
     * Deletes this BPage and all children pages from the record manager
     */
    void delete() 
        throws IOException
    {
        if (_isLeaf){
            if (_next != 0){
                BPage<K,V> nextBPage = loadBPage(_next);
                if (nextBPage._previous == _recid){ // this consistency check can be removed in production code
                    nextBPage._previous = _previous;
                    _btree._recman.update(nextBPage._recid, nextBPage, nextBPage);
                } else {
                    throw new Error("Inconsistent data in BTree");
                }
            }
            if (_previous != 0){
                BPage<K,V> previousBPage = loadBPage(_previous);
                if (previousBPage._next != _recid){ // this consistency check can be removed in production code
                    previousBPage._next = _next;
                    _btree._recman.update(previousBPage._recid, previousBPage, previousBPage);
                } else {
                    throw new Error("Inconsistent data in BTree");
                }
            }
        } else {
            int left = _first;
            int right = _btree._pageSize-1;

            for (int i = left; i <= right; i++){
                BPage<K,V> childBPage = loadBPage(_children[i]);
                childBPage.delete();
            }
        }
        
        _btree._recman.delete(_recid);
    }
    
    /**
     * Insert the given key and value.
     * <p>
     * Since the Btree does not support duplicate entries, the caller must
     * specify whether to replace the existing value.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key Insert key
     * @param value Insert value
     * @param replace Set to true to replace the existing value, if one exists.
     * @return Insertion result containing existing value OR a BPage if the key
     *         was inserted and provoked a BPage overflow.
     */
    InsertResult<K,V> insert( int height, K key, V value, boolean replace )
        throws IOException
    {
        InsertResult<K,V>  result;
        long          overflow;

        int index = findChildren( key );

        height -= 1;
        if ( height == 0 )  {

            result = new InsertResult<K,V>();

            // inserting on a leaf BPage
            overflow = -1;
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() Insert on leaf Bpage key=" + key
                                    + " value=" + value + " index="+index);
            }
            if ( compare( key, _keys[ index ] ) == 0 ) {
                // key already exists
                if ( DEBUG ) {
                    System.out.println( "Bpage.insert() Key already exists." ) ;
                }
                result._existing =  _values[ index ];
                if ( replace ) {
                    _values [ index ] = value;
                    _btree._recman.update( _recid, this, this );
                }
                // return the existing key
                return result;
            }
        } else {
            // non-leaf BPage
            BPage<K,V> child = childBPage( index );
            result = child.insert( height, key, value, replace );

            if ( result._existing != null ) {
                // return existing key, if any.
                return result;
            }

            if ( result._overflow == null ) {
                // no overflow means we're done with insertion
                return result;
            }

            // there was an overflow, we need to insert the overflow page
            // on this BPage
            if ( DEBUG ) {
                System.out.println( "BPage.insert() Overflow page: " + result._overflow._recid );
            }
            key = result._overflow.getLargestKey();
            overflow = result._overflow._recid;

            // update child's largest key
            _keys[ index ] = child.getLargestKey();

            // clean result so we can reuse it
            result._overflow = null;
        }

        // if we get here, we need to insert a new entry on the BPage
        // before _children[ index ]
        if ( !isFull() ) {
            if ( height == 0 ) {
                insertEntry( this, index-1, key, value );
            } else {
                insertChild( this, index-1, key, overflow );
            }
            _btree._recman.update( _recid, this, this );
            return result;
        }

        // page is full, we must divide the page
        int half = _btree._pageSize >> 1;
        BPage<K,V> newPage = new BPage<K,V>( _btree, _isLeaf );
        if ( index < half ) {
            // move lower-half of entries to overflow BPage,
            // including new entry
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() move lower-half of entries to overflow BPage, including new entry." ) ;
            }
            if ( height == 0 ) {
                copyEntries( this, 0, newPage, half, index );
                setEntry( newPage, half+index, key, value );
                copyEntries( this, index, newPage, half+index+1, half-index-1 );
            } else {
                copyChildren( this, 0, newPage, half, index );
                setChild( newPage, half+index, key, overflow );
                copyChildren( this, index, newPage, half+index+1, half-index-1 );
            }
        } else {
            // move lower-half of entries to overflow BPage,
            // new entry stays on this BPage
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() move lower-half of entries to overflow BPage. New entry stays" ) ;
            }
            if ( height == 0 ) {
                copyEntries( this, 0, newPage, half, half );
                copyEntries( this, half, this, half-1, index-half );
                setEntry( this, index-1, key, value );
            } else {
                copyChildren( this, 0, newPage, half, half );
                copyChildren( this, half, this, half-1, index-half );
                setChild( this, index-1, key, overflow );
            }
        }

        _first = half-1;

        // nullify lower half of entries
        for ( int i=0; i<_first; i++ ) {
            if ( height == 0 ) {
                setEntry( this, i, null, null );
            } else {
                setChild( this, i, null, -1 );
            }
        }

        if ( _isLeaf ) {
            // link newly created BPage
            newPage._previous = _previous;
            newPage._next = _recid;
            if ( _previous != 0 ) {
                BPage<K,V> previous = loadBPage( _previous );
                previous._next = newPage._recid;
                _btree._recman.update( _previous, previous, this );

            }
            _previous = newPage._recid;
        }

        _btree._recman.update( _recid, this, this );
        _btree._recman.update( newPage._recid, newPage, this );

        result._overflow = newPage;
        return result;
    }


    /**
     * Remove the entry associated with the given key.
     *
     * @param height Height of the current BPage (zero is leaf page)
     * @param key Removal key
     * @return Remove result object
     */
    RemoveResult<K,V> remove( int height, K key )
        throws IOException
    {
        RemoveResult<K,V> result;

        int half = _btree._pageSize / 2;
        int index = findChildren( key );

        height -= 1;
        if ( height == 0 ) {
            // remove leaf entry
            if ( compare( _keys[ index ], key ) != 0 ) {
                throw new IllegalArgumentException( "Key not found: " + key );
            }
            result = new RemoveResult<K,V>();
            result._value =  _values[ index ];
            removeEntry( this, index );

            // update this BPage
            _btree._recman.update( _recid, this, this );

        } else {
            // recurse into Btree to remove entry on a children page
            BPage<K,V> child = childBPage( index );
            result = child.remove( height, key );

            // update children
            _keys[ index ] = child.getLargestKey();
            _btree._recman.update( _recid, this, this );

            if ( result._underflow ) {
                // underflow occured
                if ( child._first != half+1 ) {
                    throw new IllegalStateException( "Error during underflow [1]" );
                }
                if ( index < _children.length-1 ) {
                    // exists greater brother page
                    BPage<K,V> brother = childBPage( index+1 );
                    int bfirst = brother._first;
                    if ( bfirst < half ) {
                        // steal entries from "brother" page
                        int steal = ( half - bfirst + 1 ) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        if ( child._isLeaf ) {
                            copyEntries( child, half+1, child, half+1-steal, half-1 );
                            copyEntries( brother, bfirst, child, 2*half-steal, steal );
                        } else {
                            copyChildren( child, half+1, child, half+1-steal, half-1 );
                            copyChildren( brother, bfirst, child, 2*half-steal, steal );
                        }                            

                        for ( int i=bfirst; i<bfirst+steal; i++ ) {
                            if ( brother._isLeaf ) {
                                setEntry( brother, i, null, null );
                            } else {
                                setChild( brother, i, null, -1 );
                            }
                        }

                        // update child's largest key
                        _keys[ index ] = child.getLargestKey();

                        // no change in previous/next BPage

                        // update BPages
                        _btree._recman.update( _recid, this, this );
                        _btree._recman.update( brother._recid, brother, this );
                        _btree._recman.update( child._recid, child, this );

                    } else {
                        // move all entries from page "child" to "brother"
                        if ( brother._first != half ) {
                            throw new IllegalStateException( "Error during underflow [2]" );
                        }

                        brother._first = 1;
                        if ( child._isLeaf ) {
                            copyEntries( child, half+1, brother, 1, half-1 );
                        } else {
                            copyChildren( child, half+1, brother, 1, half-1 );
                        }
                        _btree._recman.update( brother._recid, brother, this );


                        // remove "child" from current BPage
                        if ( _isLeaf ) {
                            copyEntries( this, _first, this, _first+1, index-_first );
                            setEntry( this, _first, null, null );
                        } else {
                            copyChildren( this, _first, this, _first+1, index-_first );
                            setChild( this, _first, null, -1 );
                        }
                        _first += 1;
                        _btree._recman.update( _recid, this, this );

                        // re-link previous and next BPages
                        if ( child._previous != 0 ) {
                            BPage<K,V> prev = loadBPage( child._previous );
                            prev._next = child._next;
                            _btree._recman.update( prev._recid, prev, this );
                        }
                        if ( child._next != 0 ) {
                            BPage<K,V> next = loadBPage( child._next );
                            next._previous = child._previous;
                            _btree._recman.update( next._recid, next, this );

                        }

                        // delete "child" BPage
                        _btree._recman.delete( child._recid );
                    }
                } else {
                    // page "brother" is before "child"
                    BPage<K,V> brother = childBPage( index-1 );
                    int bfirst = brother._first;
                    if ( bfirst < half ) {
                        // steal entries from "brother" page
                        int steal = ( half - bfirst + 1 ) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        if ( child._isLeaf ) {
                            copyEntries( brother, 2*half-steal, child,
                                         half+1-steal, steal );
                            copyEntries( brother, bfirst, brother,
                                         bfirst+steal, 2*half-bfirst-steal );
                        } else {
                            copyChildren( brother, 2*half-steal, child,
                                          half+1-steal, steal );
                            copyChildren( brother, bfirst, brother,
                                          bfirst+steal, 2*half-bfirst-steal );
                        }

                        for ( int i=bfirst; i<bfirst+steal; i++ ) {
                            if ( brother._isLeaf ) {
                                setEntry( brother, i, null, null );
                            } else {
                                setChild( brother, i, null, -1 );
                            }
                        }

                        // update brother's largest key
                        _keys[ index-1 ] = brother.getLargestKey();

                        // no change in previous/next BPage

                        // update BPages
                        _btree._recman.update( _recid, this, this );
                        _btree._recman.update( brother._recid, brother, this );
                        _btree._recman.update( child._recid, child, this );

                    } else {
                        // move all entries from page "brother" to "child"
                        if ( brother._first != half ) {
                            throw new IllegalStateException( "Error during underflow [3]" );
                        }

                        child._first = 1;
                        if ( child._isLeaf ) {
                            copyEntries( brother, half, child, 1, half );
                        } else {
                            copyChildren( brother, half, child, 1, half );
                        }
                        _btree._recman.update( child._recid, child, this );

                        // remove "brother" from current BPage
                        if ( _isLeaf ) {
                            copyEntries( this, _first, this, _first+1, index-1-_first );
                            setEntry( this, _first, null, null );
                        } else {
                            copyChildren( this, _first, this, _first+1, index-1-_first );
                            setChild( this, _first, null, -1 );
                        }
                        _first += 1;
                        _btree._recman.update( _recid, this, this );

                        // re-link previous and next BPages
                        if ( brother._previous != 0 ) {
                            BPage<K,V> prev = loadBPage( brother._previous );
                            prev._next = brother._next;
                            _btree._recman.update( prev._recid, prev, this );
                        }
                        if ( brother._next != 0 ) {
                            BPage<K,V> next = loadBPage( brother._next );
                            next._previous = brother._previous;
                            _btree._recman.update( next._recid, next, this );
                        }

                        // delete "brother" BPage
                        _btree._recman.delete( brother._recid );
                    }
                }
            }
        }

        // underflow if page is more than half-empty
        result._underflow = _first > half;

        return result;
    }


    /**
     * Find the first children node with a key equal or greater than the given
     * key.
     *
     * @return index of first children with equal or greater key.
     */
    private int findChildren( K key )
    {
        int left = _first;
        int right = _btree._pageSize-1;

        // binary search
        while ( left < right )  {
            int middle = ( left + right ) / 2;
            if ( compare( _keys[ middle ], key ) < 0 ) {
                left = middle+1;
            } else {
                right = middle;
            }
        }
        return right;
    }


    /**
     * Insert entry at given position.
     */
    private static <K,V> void insertEntry( BPage<K,V> page, int index,
                                     K key, V value )
    {
        K[] keys = page._keys;
        V[] values = page._values;
        int start = page._first;
        int count = index-page._first+1;

        // shift entries to the left
        System.arraycopy( keys, start, keys, start-1, count );
        System.arraycopy( values, start, values, start-1, count );
        page._first -= 1;
        keys[ index ] = key;
        values[ index ] = value;
    }


    /**
     * Insert child at given position.
     */
    private static <K,V> void  insertChild( BPage<K,V> page, int index,
                                     K key, long child )
    {
        K[] keys = page._keys;
        long[] children = page._children;
        int start = page._first;
        int count = index-page._first+1;

        // shift entries to the left
        System.arraycopy( keys, start, keys, start-1, count );
        System.arraycopy( children, start, children, start-1, count );
        page._first -= 1;
        keys[ index ] = key;
        children[ index ] = child;
    }
    
    /**
     * Remove entry at given position.
     */
    private static <K,V> void removeEntry( BPage<K,V> page, int index )
    {
        K[] keys = page._keys;
        V[] values = page._values;
        int start = page._first;
        int count = index-page._first;

        System.arraycopy( keys, start, keys, start+1, count );
        keys[ start ] = null;
        System.arraycopy( values, start, values, start+1, count );
        values[ start ] = null;
        page._first++;
    }


    /**
     * Remove child at given position.
     */
/*    
    private static void removeChild( BPage page, int index )
    {
        Object[] keys = page._keys;
        long[] children = page._children;
        int start = page._first;
        int count = index-page._first;

        System.arraycopy( keys, start, keys, start+1, count );
        keys[ start ] = null;
        System.arraycopy( children, start, children, start+1, count );
        children[ start ] = (long) -1;
        page._first++;
    }
*/
    
    /**
     * Set the entry at the given index.
     */
    private static <K,V> void setEntry( BPage<K,V> page, int index, K key, V value )
    {
        page._keys[ index ] = key;
        page._values[ index ] = value;
    }


    /**
     * Set the child BPage recid at the given index.
     */
    private static <K,V> void setChild( BPage<K,V> page, int index, K key, long recid )
    {
        page._keys[ index ] = key;
        page._children[ index ] = recid;
    }
    
    
    /**
     * Copy entries between two BPages
     */
    private static <K,V> void copyEntries( BPage<K,V> source, int indexSource,
                                     BPage<K,V> dest, int indexDest, int count )
    {
        System.arraycopy( source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy( source._values, indexSource, dest._values, indexDest, count);
    }


    /**
     * Copy child BPage recids between two BPages
     */
    private static <K,V> void copyChildren( BPage<K,V> source, int indexSource,
                                      BPage<K,V> dest, int indexDest, int count )
    {
        System.arraycopy( source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy( source._children, indexSource, dest._children, indexDest, count);
    }

    
    /**
     * Return the child BPage at given index.
     */
    BPage<K,V> childBPage( int index )
        throws IOException
    {
        return loadBPage( _children[ index ] );
    }


    /**
     * Load the BPage at the given recid.
     */
	private BPage<K,V> loadBPage( long recid )
        throws IOException
    {
        BPage<K,V> child = (BPage<K,V>) _btree._recman.fetch( recid, this );
        child._recid = recid;
        child._btree = _btree;
        return child;
    }

    
    private final int compare( K value1, K value2 )
    {
        if ( value1 == null ) {
            return 1;
        }
        if ( value2 == null ) {
            return -1;
        }
        return _btree._comparator.compare( value1, value2 );
    }

    public static byte[] readByteArray( DataInputStream in )
        throws IOException
    {
        int len = LongPacker.unpackInt(in);
        if ( len == 0 ) {
            return null;
        }
        byte[] buf = new byte[ len-1 ];
        in.readFully( buf );
        return buf;
    }


    public static void writeByteArray(SerializerOutput out, byte[] buf)
        throws IOException
    {
        if ( buf == null ) {
            out.write( 0 );
        } else {
        	LongPacker.packInt( out, buf.length+1);
            out.write( buf );
        }
    }

    /**
     * Dump the structure of the tree on the screen.  This is used for debugging
     * purposes only.
     */
    private void dump( int height )
    {
        String prefix = "";
        for ( int i=0; i<height; i++ ) {
           prefix += "    ";
        }
        System.out.println( prefix + "-------------------------------------- BPage recid=" + _recid);
        System.out.println( prefix + "first=" + _first );
        for ( int i=0; i< _btree._pageSize; i++ ) {
            if ( _isLeaf ) {
                System.out.println( prefix + "BPage [" + i + "] " + _keys[ i ] + " " + _values[ i ] );
            } else {
                System.out.println( prefix + "BPage [" + i + "] " + _keys[ i ] + " " + _children[ i ] );
            }
        }
        System.out.println( prefix + "--------------------------------------" );
    }


    /**
     * Recursively dump the state of the BTree on screen.  This is used for
     * debugging purposes only.
     */
    void dumpRecursive( int height, int level )
        throws IOException
    {
        height -= 1;
        level += 1;
        if ( height > 0 ) {
            for ( int i=_first; i<_btree._pageSize; i++ ) {
                if ( _keys[ i ] == null ) break;
                BPage<K,V> child = childBPage( i );
                child.dump( level );
                child.dumpRecursive( height, level );
            }
        }
    }

// 
//   JAN KOTEK: assertConsistency was commented out, as it was not referenced from anywhere    
//    /**
//     * Assert the ordering of the keys on the BPage.  This is used for testing
//     * purposes only.
//     */
//    private void assertConsistency()
//    {
//        for ( int i=_first; i<_btree._pageSize-1; i++ ) {
//            if ( compare( (byte[]) _keys[ i ], (byte[]) _keys[ i+1 ] ) >= 0 ) {
//                dump( 0 );
//                throw new Error( "BPage not ordered" );
//            }
//        }
//    }
//
//
//    /**
//     * Recursively assert the ordering of the BPage entries on this page
//     * and sub-pages.  This is used for testing purposes only.
//     */
//    void assertConsistencyRecursive( int height ) 
//        throws IOException 
//    {
//        assertConsistency();
//        if ( --height > 0 ) {
//            for ( int i=_first; i<_btree._pageSize; i++ ) {
//                if ( _keys[ i ] == null ) break;
//                BPage child = childBPage( i );
//                if ( compare( (byte[]) _keys[ i ], child.getLargestKey() ) != 0 ) {
//                    dump( 0 );
//                    child.dump( 0 );
//                    throw new Error( "Invalid child subordinate key" );
//                }
//                child.assertConsistencyRecursive( height );
//            }
//        }
//    }


    private static final int LEAF = Serialization.BPAGE_LEAF;
    private static final int NOT_LEAF = Serialization.BPAGE_NONLEAF;
    
    /**
     * Deserialize the content of an object from a byte array.
     *
     * @param serialized Byte array representation of the object
     * @return deserialized object
     *
     */
    @SuppressWarnings("unchecked")
	public BPage<K,V> deserialize( SerializerInput ois ) 
        throws IOException
    {
    	

      BPage<K,V> bpage = new BPage<K,V>();

  	  switch(ois.read()){
  		case LEAF:bpage._isLeaf = true;break;
  		case NOT_LEAF:bpage._isLeaf = false;break;
  		default: throw new InternalError("wrong BPage header");
  	  }
            
      if ( bpage._isLeaf ) {
          bpage._previous = LongPacker.unpackLong(ois);
          bpage._next = LongPacker.unpackLong(ois);
      }

      
      bpage._first = LongPacker.unpackInt(ois);

      try {

           bpage._keys = readKeys(ois,bpage._first);
           
      } catch ( ClassNotFoundException except ) {
          throw new IOException( except.getMessage() );
      }
      
      if ( bpage._isLeaf ) {
          
          try {
              readValues(ois, bpage);
          } catch ( ClassNotFoundException except ) {
              throw new IOException( except);
          }
      } else {
          bpage._children = new long[ _btree._pageSize ];
          for ( int i=bpage._first; i<_btree._pageSize; i++ ) {
              bpage._children[ i ] = LongPacker.unpackLong(ois);
          }
      }
      
      return bpage;

    }

    
    /** 
     * Serialize the content of an object into a byte array.
     *
     * @param obj Object to serialize
     * @return a byte array representing the object's state
     *
     */
    public void serialize(SerializerOutput oos, BPage<K,V> obj ) 
        throws IOException
    {

        
        // note:  It is assumed that BPage instance doing the serialization is the parent
        // of the BPage object being serialized.
        
        BPage<K,V> bpage =  obj;
        
        oos.writeByte( bpage._isLeaf?LEAF:NOT_LEAF );
        if ( bpage._isLeaf ) {
            LongPacker.packLong(oos, bpage._previous );
            LongPacker.packLong(oos, bpage._next );
        }
                
        LongPacker.packInt(oos, bpage._first );

       	writeKeys(oos, bpage._keys,bpage._first);        	

        if ( bpage._isLeaf ) {
        	writeValues(oos, bpage);
        } else {
            for ( int i=bpage._first; i<_btree._pageSize; i++ ) {
            	LongPacker.packLong(oos,  bpage._children[ i ] );
            }
        }
        
    }
    

	private void readValues(DataInputStream ois, BPage<K, V> bpage) throws IOException, ClassNotFoundException {
		  bpage._values = (V[]) new Object[ _btree._pageSize ];
	
		  if ( _btree.valueSerializer == null||   _btree.valueSerializer == DefaultSerializer.INSTANCE) {
			  V[] vals= (V[]) Serialization.readObject(ois);
			  for ( int i=bpage._first; i<_btree._pageSize; i++ ) {
				  bpage._values[i] = vals[i - bpage._first];	  
			  }
		  }else{
	          
			  for ( int i=bpage._first; i<_btree._pageSize; i++ ) {                  
		          byte[] serialized = readByteArray( ois );
		          if ( serialized != null ) {
		              bpage._values[ i ] = _btree.valueSerializer.deserialize( new SerializerInput( new ByteArrayInputStream(serialized)) );
		          }
		      }
		  }
	}


	private void writeValues(SerializerOutput oos, BPage<K, V> bpage) throws IOException {
		if ( _btree.valueSerializer == null || _btree.valueSerializer == DefaultSerializer.INSTANCE ) {
			Object[] vals2 = Arrays.copyOfRange(bpage._values, bpage._first, bpage._values.length);
			Serialization.writeObject(oos, vals2);
		}else{
			for ( int i=bpage._first; i<_btree._pageSize; i++ ) {                                                
		        if ( bpage._values[ i ] != null ) {
		        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
		            _btree.valueSerializer.serialize(new SerializerOutput(baos), bpage._values[ i ] );
		            writeByteArray( oos, baos.toByteArray() );
		        } else {
		            writeByteArray( oos, null );
		        }
		    }
		}
	}


	private static final int ALL_NULL = 0;
	private static final int ALL_INTEGERS = 1 << 5;
	private static final int ALL_INTEGERS_NEGATIVE = 2 <<5;
	private static final int ALL_LONGS = 3 <<5;
	private static final int ALL_LONGS_NEGATIVE = 4 <<5;
	private static final int ALL_STRINGS = 5 <<5;
	private static final int ALL_OTHER = 6 <<5;

	
	private K[] readKeys(SerializerInput ois, final int firstUse) throws IOException, ClassNotFoundException {
		Object[] ret = new Object[_btree._pageSize];
		final int type = ois.read();
		if(type == ALL_NULL){
			return (K[])ret;
		}else if(type == ALL_INTEGERS || type == ALL_INTEGERS_NEGATIVE){
			long first = LongPacker.unpackLong(ois);
			if(type == ALL_INTEGERS_NEGATIVE)
				first = -first;
			ret[firstUse] = Integer.valueOf((int)first);
			for(int i = firstUse+1;i<_btree._pageSize;i++){
//				ret[i] = Serialization.readObject(ois);
				long v = LongPacker.unpackLong(ois);
				if(v == 0) continue; //null
				v = v +first ;
				ret[i] = Integer.valueOf((int)v);
				first =v;
			}
			return (K[]) ret;
		}else if(type == ALL_LONGS || type == ALL_LONGS_NEGATIVE){
			long first = LongPacker.unpackLong(ois);
			if(type == ALL_LONGS_NEGATIVE)
				first = -first;

			ret[firstUse] = Long.valueOf(first);
			for(int i = firstUse+1;i<_btree._pageSize;i++){
				//ret[i] = Serialization.readObject(ois);
				long v = LongPacker.unpackLong(ois);
				if(v == 0) continue; //null
				v = v +first ;
				ret[i] = Long.valueOf(v);
				first = v;
			}
			return (K[]) ret;
		}else if(type == ALL_STRINGS){			
			byte[] previous = null;
			for(int i = firstUse;i<_btree._pageSize;i++){
				byte[] b = LeadingValueCompressionProvider.readByteArray(ois, previous, 0);
				if(b == null ) continue;
				ret[i] = new String(b);
				previous = b;
			}
			return (K[]) ret;			
			
		}else if(type == ALL_OTHER){
			if(_btree.keySerializer == null || _btree.keySerializer == DefaultSerializer.INSTANCE){
				for (int i = firstUse ; i < _btree._pageSize; i++) {
					ret[i] = DefaultSerializer.INSTANCE.deserialize(ois);
				}			
				return (K[]) ret;
			}

			
			Serializer ser = _btree.keySerializer!=null? _btree.keySerializer : DefaultSerializer.INSTANCE;
			OpenByteArrayInputStream in1 = null;
			SerializerInput in2 = null;
			byte[] previous = null;
			for(int i = firstUse;i<_btree._pageSize;i++){
				byte[] b = LeadingValueCompressionProvider.readByteArray(ois, previous, 0);
				if(b == null ) continue;
				if(in1 == null){
					in1 = new OpenByteArrayInputStream(b);
					in2 = new SerializerInput(in1);
				}
				in1.reset(b, b.length);
				ret[i] = ser.deserialize(in2);
				previous = b;
			}
			return (K[]) ret;			

		}else{ 
			throw new InternalError("unknown bpage header type: "+type);
		}
		
	}
    
	
	
	@SuppressWarnings("unchecked")
	private void writeKeys(SerializerOutput oos, K[] keys, final int firstUse) throws IOException {		
		if(keys.length!=_btree._pageSize)
			throw new IllegalArgumentException("wrong keys size");
				
		//check if all items on key are null
		boolean allNull = true;
		for (int i = firstUse ; i < _btree._pageSize; i++) {
			if(keys[i]!=null){
				allNull = false;
				break;
			}
		}
		if(allNull){
			oos.write(ALL_NULL);
			return;
		}

		/**
		 * Special compression to compress Long and Integer
		 */
		if (_btree._comparator == ComparableComparator.INSTANCE && 
				(_btree.keySerializer == null || _btree.keySerializer == DefaultSerializer.INSTANCE)) {
			boolean allInteger = true;
			for (int i = firstUse ; i <_btree._pageSize; i++) {
				if (keys[i]!=null && keys[i].getClass() != Integer.class) {
					allInteger = false;
					break;
				}
			}
			boolean allLong = true;
			for (int i = firstUse ; i < _btree._pageSize; i++) {
				if (keys[i]!=null &&  (keys[i].getClass() != Long.class ||
						//special case to exclude Long.MIN_VALUE from conversion, causes problems to LongPacker
					((Long)keys[i]).longValue() == Long.MIN_VALUE)
				) {
					allLong = false;
					break;
				}												
			}
			
			if(allLong){
				//check that diff between MIN and MAX fits into PACKED_LONG
				long max = Long.MIN_VALUE;
				long min = Long.MAX_VALUE;
				for(int i = firstUse;i <_btree._pageSize;i++){
					if(keys[i] == null) continue;
					long v = (Long)keys[i];
					if(v>max) max = v;
					if(v<min) min = v;
				}
				//now convert to Double to prevent overflow errors
				double max2 = max;
				double min2 = min;
				double maxDiff = Long.MAX_VALUE;
				if(max2 - min2 >maxDiff/2) // divide by two just to by sure
					allLong = false;
				
			}
			
			if(allLong && allInteger)
				throw new InternalError();

			if ( allLong || allInteger) {
				long first = ((Number) keys[firstUse ]).longValue();
				//write header
				if(allInteger){ 
					if(first>0)oos.write(ALL_INTEGERS );
					else oos.write(ALL_INTEGERS_NEGATIVE );
				}else if(allLong){
					if(first>0)oos.write(ALL_LONGS );
					else oos.write(ALL_LONGS_NEGATIVE );
				}else{
					throw new InternalError();
				}
				
				//write first
				LongPacker.packLong(oos,Math.abs(first));
				//write others
				for(int i = firstUse+1;i<_btree._pageSize;i++){
//					Serialization.writeObject(oos, keys[i]);
					if(keys[i] == null)
						LongPacker.packLong(oos,0);
					else{
						long v = ((Number) keys[i]).longValue();
						if(v<=first) throw new InternalError("not ordered");
						LongPacker.packLong(oos, v-first);
						first=  v;
					}
				}
				return;
			}else{
				//another special case for Strings
				boolean allString = true;
				for (int i = firstUse ; i < _btree._pageSize; i++) {
					if (keys[i]!=null &&  (keys[i].getClass() != String.class)
					) {
						allString = false;
						break;
					}												
				}
				if(allString){
					oos.write(ALL_STRINGS );
					byte[] previous = null;
					for (int i = firstUse ; i < _btree._pageSize; i++) {
						if(keys[i] == null){
							LeadingValueCompressionProvider.writeByteArray(oos, null, previous, 0);
						}else{
							byte[] b = ((String)keys[i]).getBytes();
							LeadingValueCompressionProvider.writeByteArray(oos, b, previous, 0);
							previous = b;
						}
					}
					return;
				}
			}
		}
		
		/**
		 * other case, serializer is provided or other stuff
		 */
		oos.write(ALL_OTHER );
		if(_btree.keySerializer == null || _btree.keySerializer == DefaultSerializer.INSTANCE){
			for (int i = firstUse ; i < _btree._pageSize; i++) {
				DefaultSerializer.INSTANCE.serialize(oos, keys[i]);
			}		
			return;
		}
		
		//custom serializer is provided, use it
		
		Serializer ser = _btree.keySerializer;
		byte[] previous = null;
		byte[] buffer = new byte[1024];
		OpenByteArrayOutputStream out2 = new OpenByteArrayOutputStream(buffer);
		SerializerOutput out3 = new SerializerOutput(out2);
		for (int i = firstUse ; i < _btree._pageSize; i++) {
			if(keys[i] == null){
				LeadingValueCompressionProvider.writeByteArray(oos, null, previous, 0);
			}else{
				out2.reset();
				ser.serialize(out3,keys[i]);
				byte[] b = out2.toByteArray();
				LeadingValueCompressionProvider.writeByteArray(oos, b, previous, 0);
				previous = b;
			}			
		}
			
		return;
		
		
	}



	/** STATIC INNER CLASS
     *  Result from insert() method call
     */
    static class InsertResult<K,V> {

        /**
         * Overflow page.
         */
        BPage<K,V> _overflow;

        /**
         * Existing value for the insertion key.
         */
        V _existing;

    }

    /** STATIC INNER CLASS
     *  Result from remove() method call
     */
    static class RemoveResult<K,V> {

        /**
         * Set to true if underlying pages underflowed
         */
        boolean _underflow;

        /**
         * Removed entry value
         */
        V _value;
    }


    /** PRIVATE INNER CLASS
     * Browser to traverse leaf BPages.
     */
    static class Browser<K,V>
        implements TupleBrowser<K,V>
    {

        /**
         * Current page.
         */
        private BPage<K,V> _page;


        /**
         * Current index in the page.  The index positionned on the next
         * tuple to return.
         */
        private int _index;


        /**
         * Create a browser.
         *
         * @param page Current page
         * @param index Position of the next tuple to return.
         */
        Browser( BPage<K,V> page, int index )
        {
            _page = page;
            _index = index;
        }

        public boolean getNext( Tuple<K,V> tuple )
            throws IOException
        {
            if ( _index < _page._btree._pageSize ) {
                if ( _page._keys[ _index ] == null ) {
                    // reached end of the tree.
                    return false;
                }
            } else if ( _page._next != 0 ) {
                // move to next page
                _page = _page.loadBPage( _page._next );
                _index = _page._first;
            }
            tuple.setKey( _page._keys[ _index ] );
            tuple.setValue(  _page._values[ _index ] );
            _index++;
            return true;
        }

        public boolean getPrevious( Tuple<K,V> tuple )
            throws IOException
        {
            if ( _index == _page._first ) {

                if ( _page._previous != 0 ) {
                    _page = _page.loadBPage( _page._previous );
                    _index = _page._btree._pageSize;
                } else {
                    // reached beginning of the tree
                    return false;
                }
            }
            _index--;
            tuple.setKey( _page._keys[ _index ] );
            tuple.setValue(  _page._values[ _index ] );
            return true;

        }
    }    

    /**
     * Used for debugging and testing only.  Recursively obtains the recids of
     * all child BPages and adds them to the 'out' list.
     * @param out
     * @param height
     * @throws IOException
     */
    void dumpChildPageRecIDs(List out, int height)
    throws IOException
    {
        height -= 1;
        if ( height > 0 ) {
            for ( int i=_first; i<_btree._pageSize; i++ ) {
                if ( _children[ i ] == RecordManager.NULL_RECID ) continue;
                
                BPage child = childBPage( i );
                out.add(new Long(child._recid));
                child.dumpChildPageRecIDs( out, height );
            }
        }
    }
    
}
