/*
This is the implementation of the paper "DHiX: Dynamic and Hybrid Real-time Window Aggregations with Constant Time Complexity".
By Savong Bou, Toshiyuki Amagasa, Hiroyuki Kitagawa
*/

// Utility Pair class if not using external libraries
class Pair<K, V> {
    private K key;
    private V value;
    public Pair(K k, V v) { key = k; value = v; }
    public K getKey() { return key; }
    public V getValue() { return value; }
}
