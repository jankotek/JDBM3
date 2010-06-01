/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot. 
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: Location.java,v 1.3 2006/05/31 22:29:44 thompsonbry Exp $
 */

package jdbm.recman;

/**
 * This class represents a location within a file. Both physical and
 * logical rowids are based on locations internally - this version is
 * used when there is no file block to back the location's data.
 */
final class Location {
    final private long block;
    final private short offset;
    private int _hashCode = 0;

    /**
     * Creates a location from a (block, offset) tuple.
     */
    Location(long block, short offset) {
        this.block = block;
        this.offset = offset;
    }

    /**
     * Creates a location from a combined block/offset long, as
     * used in the external representation of logical rowids.
     * 
     * @see #toLong()
     */
    Location(long blockOffset) {
        this.offset = (short) (blockOffset & 0xffff);
        this.block = blockOffset >> 16;
    }

    /**
     * Creates a location based on the data of the physical rowid.
     */
    Location(PhysicalRowId src) {
        block = src.getBlock();
        offset = src.getOffset();
    }

    /**
     * Returns the file block of the location
     */
    long getBlock() {
        return block;
    }

    /**
     * Returns the offset within the block of the location
     */
    short getOffset() {
        return offset;
    }

    /**
     * Returns the external representation of a location when used
     * as a logical rowid, which combines the block and the offset
     * in a single long.
     */
    long toLong() {
        return (block << 16) + (long) offset;
    }

    // overrides of java.lang.Object

    /**
         * Hash code based on the record identifier. This is computed lazily and
         * cached. It supports the buffered record installer, which creates hash
         * collections based on locations.
         * 
         * @see BufferedRecordInstallManager
         */
    public int hashCode() {
        if( _hashCode == 0 ) {
                _hashCode = new Long(toLong()).hashCode();
        }
        return _hashCode;
    }
    
    public boolean equals(Object o) {
        if( this == o ) return true;
//        if (o == null || !(o instanceof Location))
//            return false;
        Location ol = (Location) o;
        return ol.block == block && ol.offset == offset;
    }

    /**
     * True iff the block and offset are both zero.
     */
    public boolean isZero() {
        return block == 0L && offset == (short)0;
    }
    
    public String toString() {
        return "PL(" + block + ":" + offset + ")";
    }
}
