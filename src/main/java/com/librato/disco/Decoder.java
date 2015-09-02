package com.librato.disco;

/**
 * Interface to decode bytes stored in Zookeeper to type T
 * @param <T> type of object excepted from decoding bytes
 */
public interface Decoder<T> {
    /**
     * Decode a byte array into an object of type T
     * @param bytes the byte array payload stored in Zookeeper
     * @return returns the object as decoded from bytes
     */
    T decode(byte[] bytes);

    /**
     * Handle decoding exception
     * @param ex the exception the decoder raised
     */
    void handleException(Exception ex);
}
