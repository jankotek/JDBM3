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
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: RecordManagerOptions.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */

package jdbm;

/**
 * Standard options for RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: RecordManagerOptions.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */
public class RecordManagerOptions
{

    /**
     * Option to create a thread-safe record manager.
     */
    public static final String PROVIDER_FACTORY = "jdbm.provider";


    /**
     * Option to create a thread-safe record manager.
     */
    public static final String THREAD_SAFE = "jdbm.threadSafe";


    /**
     * Option to automatically commit data after each operation.
     */
    public static final String AUTO_COMMIT = "jdbm.autoCommit";


    /**
     * Option to disable transaction (to increase performance at the cost of
     * potential data loss).
     */
    public static final String DISABLE_TRANSACTIONS = "jdbm.disableTransactions";

    /**
     * Type of cache to use. Possible values: auto|none|mru|soft     * 
     * Default auto.
     * <p> 
     * With auto soft cache is used if VM have more then 1000 MB available.
     * Bellow that mru cache is used.
     * 
     */
    public static final String CACHE_TYPE = "jdbm.cache.type";

    
    /**
     * Cache size (when applicable)
     */
    public static final String CACHE_SIZE = "jdbm.cache.size";


    /**
     * Compress pages in RecordManager with ZLIB. 
     * This may result in better space usage, but lower performance.
     * Possible values: true|false (default false) 
     */
	public static final String COMPRESS = "jdbm.compress";

}
