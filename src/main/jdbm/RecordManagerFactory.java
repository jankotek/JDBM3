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


package jdbm;

import java.io.IOException;
import java.util.Properties;

/**
 * This is the factory class to use for instantiating {@link RecordManager}
 * instances.
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: RecordManagerFactory.java,v 1.2 2005/06/25 23:12:31 doomdark
 *          Exp $
 */
public final class RecordManagerFactory {

	/**
	 * Create a record manager.
	 * 
	 * @param name
	 *            Name of the record file.
	 * @throws IOException
	 *             if an I/O related exception occurs while creating or opening
	 *             the record manager.
	 * @throws UnsupportedOperationException
	 *             if some options are not supported by the implementation.
	 * @throws IllegalArgumentException
	 *             if some options are invalid.
	 */
	public static RecordManager createRecordManager(String name)
			throws IOException {
		return createRecordManager(name, new Properties());
	}

	/**
	 * Create a record manager.
	 * 
	 * @param name
	 *            Name of the record file.
	 * @param options
	 *            Record manager options.
	 * @throws IOException
	 *             if an I/O related exception occurs while creating or opening
	 *             the record manager.
	 * @throws UnsupportedOperationException
	 *             if some options are not supported by the implementation.
	 * @throws IllegalArgumentException
	 *             if some options are invalid.
	 */
	@SuppressWarnings("unchecked")
	public static RecordManager createRecordManager(String name,
			Properties options) throws IOException {

		String provider = options.getProperty(
				RecordManagerOptions.PROVIDER_FACTORY, "jdbm.recman.Provider");
		Throwable e = null;
		try {
			Class clazz = Class.forName(provider);
			RecordManagerProvider factory = (RecordManagerProvider) clazz.newInstance();
			return factory.createRecordManager(name, options);
		} catch (InstantiationException except) {
			e = except;
		} catch (IllegalAccessException except) {
			e = except;
		} catch (ClassNotFoundException except) {
			e = except;
		}
				
		throw new IllegalArgumentException(
			"Invalid record manager provider: " + provider,e);

	}

}
