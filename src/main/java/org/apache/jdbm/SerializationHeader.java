package org.apache.jdbm;

/**
 * Header byte, is used at start of each record to indicate data type
 * WARNING !!! values bellow must be unique !!!!!
 */
final class SerializationHeader {

    final static int NULL = 0;
    final static int NORMAL = 1;
    final static int BOOLEAN_TRUE = 2;
    final static int BOOLEAN_FALSE = 3;
    final static int INTEGER_MINUS_1 = 4;
    final static int INTEGER_0 = 5;
    final static int INTEGER_1 = 6;
    final static int INTEGER_2 = 7;
    final static int INTEGER_3 = 8;
    final static int INTEGER_4 = 9;
    final static int INTEGER_5 = 10;
    final static int INTEGER_6 = 11;
    final static int INTEGER_7 = 12;
    final static int INTEGER_8 = 13;
    final static int INTEGER_255 = 14;
    final static int INTEGER_PACK_NEG = 15;
    final static int INTEGER_PACK = 16;
    final static int LONG_MINUS_1 = 17;
    final static int LONG_0 = 18;
    final static int LONG_1 = 19;
    final static int LONG_2 = 20;
    final static int LONG_3 = 21;
    final static int LONG_4 = 22;
    final static int LONG_5 = 23;
    final static int LONG_6 = 24;
    final static int LONG_7 = 25;
    final static int LONG_8 = 26;
    final static int LONG_PACK_NEG = 27;
    final static int LONG_PACK = 28;
    final static int LONG_255 = 29;
    final static int LONG_MINUS_MAX = 30;
    final static int SHORT_MINUS_1 = 31;
    final static int SHORT_0 = 32;
    final static int SHORT_1 = 33;
    final static int SHORT_255 = 34;
    final static int SHORT_FULL = 35;
    final static int BYTE_MINUS_1 = 36;
    final static int BYTE_0 = 37;
    final static int BYTE_1 = 38;
    final static int BYTE_FULL = 39;
    final static int CHAR = 40;
    final static int FLOAT_MINUS_1 = 41;
    final static int FLOAT_0 = 42;
    final static int FLOAT_1 = 43;
    final static int FLOAT_255 = 44;
    final static int FLOAT_SHORT = 45;
    final static int FLOAT_FULL = 46;
    final static int DOUBLE_MINUS_1 = 47;
    final static int DOUBLE_0 = 48;
    final static int DOUBLE_1 = 49;
    final static int DOUBLE_255 = 50;
    final static int DOUBLE_SHORT = 51;
    final static int DOUBLE_FULL = 52;
    final static int DOUBLE_ARRAY = 53;
    final static int BIGDECIMAL = 54;
    final static int BIGINTEGER = 55;
    final static int FLOAT_ARRAY = 56;
    final static int INTEGER_MINUS_MAX = 57;
    final static int SHORT_ARRAY = 58;
    final static int BOOLEAN_ARRAY = 59;

    final static int ARRAY_INT_B_255 = 60;
    final static int ARRAY_INT_B_INT = 61;
    final static int ARRAY_INT_S = 62;
    final static int ARRAY_INT_I = 63;
    final static int ARRAY_INT_PACKED = 64;

    final static int ARRAY_LONG_B = 65;
    final static int ARRAY_LONG_S = 66;
    final static int ARRAY_LONG_I = 67;
    final static int ARRAY_LONG_L = 68;
    final static int ARRAY_LONG_PACKED = 69;

    final static int CHAR_ARRAY = 70;
    final static int ARRAY_BYTE_INT = 71;

    final static int NOTUSED_ARRAY_OBJECT_255 = 72;
    final static int ARRAY_OBJECT = 73;
    //special cases for BTree values which stores references
    final static int ARRAY_OBJECT_PACKED_LONG = 74;
    final static int ARRAYLIST_PACKED_LONG = 75;

    final static int STRING_EMPTY = 101;
    final static int NOTUSED_STRING_255 = 102;
    final static int STRING = 103;
    final static int NOTUSED_ARRAYLIST_255 = 104;
    final static int ARRAYLIST = 105;

    final static int NOTUSED_TREEMAP_255 = 106;
    final static int TREEMAP = 107;
    final static int NOTUSED_HASHMAP_255 = 108;
    final static int HASHMAP = 109;
    final static int NOTUSED_LINKEDHASHMAP_255 = 110;
    final static int LINKEDHASHMAP = 111;

    final static int NOTUSED_TREESET_255 = 112;
    final static int TREESET = 113;
    final static int NOTUSED_HASHSET_255 = 114;
    final static int HASHSET = 115;
    final static int NOTUSED_LINKEDHASHSET_255 = 116;
    final static int LINKEDHASHSET = 117;
    final static int NOTUSED_LINKEDLIST_255 = 118;
    final static int LINKEDLIST = 119;


    final static int NOTUSED_VECTOR_255 = 120;
    final static int VECTOR = 121;
    final static int IDENTITYHASHMAP = 122;
    final static int HASHTABLE = 123;
    final static int LOCALE = 124;
    final static int PROPERTIES = 125;

    final static int CLASS = 126;
    final static int DATE = 127;


    static final int JDBMLINKEDLIST = 159;
    static final int HTREE = 160;

    final static int BTREE = 161;

    static final int BTREE_NODE_LEAF = 162;
    static final int BTREE_NODE_NONLEAF = 163;
    static final int HTREE_BUCKET = 164;
    static final int HTREE_DIRECTORY = 165;
    /**
     * used for reference to already serialized object in object graph
     */
    static final int OBJECT_STACK = 166;
    static final int JAVA_SERIALIZATION = 172;


}
