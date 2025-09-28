# DHiX
Dynamic and Hybrid Real-time Window Aggregations with Constant Time Complexity

This is the implementation of the paper "DHiX: Dynamic and Hybrid Real-time Window Aggregations with Constant Time Complexity".

# Contact
savong-hashimoto@cs.tsukuba.ac.jp

# Source code and programming structure:

- Main Class
   - DHiX.java

- DHiX Engine Class
   - DHiXEngine.java

- Main Methods in DHiX Engine Class
   - updateAggregationPath(int tail, int h) : Definition 4.6: Update Aggregation from  Head
to Tail
   - computeTail(int h, int w_c, int size) : Definition 4.4: Find Tail index for Count Window
   - initializeUnitAndLevels(): Initialize time unit and levels
   - insert(): Insert records into either the time interval or hashmap
   - query(): Find Tail index for Time Window
# How to run the code in brief:

- java DHiX.java


