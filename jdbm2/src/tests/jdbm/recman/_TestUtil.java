/*
 *  $Id: TestUtil.java,v 1.1 2000/05/06 00:00:53 boisvert Exp $
 *
 *  Unit test utilities.
 *
 *  Simple db toolkit.
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License 
 *  as published by the Free Software Foundation; either version 2 
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License 
 *  along with this library; if not, write to the Free Software Foundation, 
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

/**
 * This class contains some test utilities.
 */
public class _TestUtil {
	/**
	 * Creates a "record" containing "length" repetitions of the indicated byte.
	 */
	public static byte[] makeRecord(int length, byte b) {
		byte[] retval = new byte[length];
		for (int i = 0; i < length; i++)
			retval[i] = b;
		return retval;
	}

	/**
	 * Checks whether the record has the indicated length and data
	 */
	public static boolean checkRecord(byte[] data, int length, byte b) {
		if (data.length != length) {
			System.err.println("length doesn't match: expected " + length + ", got " + data.length);
			return false;
		}

		for (int i = 0; i < length; i++)
			if (data[i] != b) {
				System.err.println("byte " + i + " wrong: expected " + b + ", got " + data[i]);
				return false;
			}

		return true;
	}

}
