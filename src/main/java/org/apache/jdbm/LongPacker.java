/*
Copyright (c) 2008, Nathan Sweet
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    * Neither the name of Esoteric Software nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package org.apache.jdbm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Packing utility for non-negative <code>long</code> and values.
 * <p/>
 * Originally developed for Kryo by Nathan Sweet.
 * Modified for JDBM by Jan Kotek
 */
public final class LongPacker {


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param os
     * @param value
     * @throws IOException
     */
    static public void packLong(DataOutput os, long value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7FL) != 0) {
            os.write((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        os.write((byte) value);
    }


    /**
     * Unpack positive long value from the input stream.
     *
     * @param is The input stream.
     * @return The long value.
     * @throws java.io.IOException
     */
    static public long unpackLong(DataInput is) throws IOException {

        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed long.");
    }


    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * @param os
     * @param value
     * @throws IOException
     */

    static public void packInt(DataOutput os, int value) throws IOException {

        if (value < 0) {
            throw new IllegalArgumentException("negative value: v=" + value);
        }

        while ((value & ~0x7F) != 0) {
            os.write(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        os.write((byte) value);
    }

    static public int unpackInt(DataInput is) throws IOException {
        for (int offset = 0, result = 0; offset < 32; offset += 7) {
            int b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new Error("Malformed integer.");

    }

}
