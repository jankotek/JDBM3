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
 * $Id: FreePhysicalRowId.java,v 1.1 2000/05/06 00:00:31 boisvert Exp $
 */

package jdbm.recman;

/**
 * This class extends the physical rowid with a size value to indicated the size 
 * of a free rowid on the free rowid list.
 */
final class FreePhysicalRowId extends PhysicalRowId {
	// offsets
	private static final short O_SIZE = PhysicalRowId.SIZE; // int size
	static final short SIZE = O_SIZE + Magic.SZ_INT;
	
	/**
	 * Used to cache size value. So no read is necessary when looking for free block 
	 */
	private int size = -1; //FIXME need to check if this RowId can be accessed from somewhere else

	/**
	 * Constructs a physical rowid from the indicated data starting 
	 * at the indicated position.
	 */
	FreePhysicalRowId(BlockIo block, short pos) {
		super(block, pos);
	}

	/** Returns the size */
	int getSize() {
		if(size==-1)			
			size = block.readInt(pos + O_SIZE);
		return size;
	}

	/** Sets the size */
	void setSize(int value) {
		size = value;
		block.writeInt(pos + O_SIZE, value);
	}

}
