/*
This is the implementation of the paper "DHiX: Dynamic and Hybrid Real-time Window Aggregations with Constant Time Complexity".
By Savong Bou, Toshiyuki Amagasa, Hiroyuki Kitagawa
*/
import java.util.*;

public class DHiXEngine {
    static class Record {
        long timestamp;
        int a;
        Integer aref;

        Record(long timestamp, int a) {
            this.timestamp = timestamp;
            this.a = a;
            this.aref = null;
        }
    }

    // Configuration
    boolean unitInitialized = false;
    long C;
    int h = 0;
    int cSlide = 0;
    long tSlide = 0;

    // Core structures
    List<Record> DHiX = new ArrayList<>();
    Map<Long, Integer> H = new HashMap<>();
    List<Pair<Long, Map<Integer, List<Integer>>>> intervalLevels = new ArrayList<>();
    List<Integer> verySparseInterval = new ArrayList<>();

    // Aggregation operator (can be replaced by any custom logic)
    private int aggregate(int a, int b) {
        return a + b; // Example: SUM
    }

    // Definition 4.4: Compute Tail for Count Window
    private int computeTail(int h, int w_c, int size) {
        if (h >= w_c - 1) {
            return h - w_c + 1;
        } else {
            return h + size - w_c + 1;
        }
    }

    // Definition 4.6: Update Aggregation from Tail to Head
    private void updateAggregationPath(int tail, int h) {
        Stack<Integer> P = new Stack<>();
        int i = tail;

        while (i <= h + 1 && i < DHiX.size()) {
            P.push(i);
            int j = i;
            Integer aref = DHiX.get(i).aref;
            if (aref == null) {
                i = j + 1;
            } else {
                i = aref + 1;
            }
        }

        // Phase 1
        int k = h;
        i = P.pop();
        int x = k;

        while (--k >= 0) {
            DHiX.get(k).a = aggregate(DHiX.get(k).a, DHiX.get(x).a);
            DHiX.get(k).aref = h;
            x = k;
            if (k == i) break;
        }

        // Phase 2
        while (!P.isEmpty()) {
            int j = P.pop();
            DHiX.get(j).a = aggregate(DHiX.get(j).a, DHiX.get(i).a);
            DHiX.get(j).aref = h;
            i = j;
        }
    }

    int capacity = 1000; // set this to your buffer size limit
    // Main DHiX Execution Algorithm (Algorithm 4)
    public void execute(List<Record> stream, String windowType, long W, long S) {
        for (Record r : stream) {
            long ti = r.timestamp;
            int value = r.a;

            // Append to DHiX
            if (h < DHiX.size()) {
                DHiX.set(h, new Record(ti, value));
            } else {
                DHiX.add(new Record(ti, value));
            }

            insert(ti, h, DHiX, H, intervalLevels, C, verySparseInterval);
            cSlide++;

            if (windowType.equals("count")) {
                if (cSlide == S) {
                    int tail = computeTail(h, (int) W, DHiX.size());
                    updateAggregationPath(tail, h);
                    System.out.println("Result = " + DHiX.get(tail).a);
                    cSlide = 0;
                }
            } else { // time-based window
                if (ti - tSlide >= S) {
                    int tail = query(ti, W, h, DHiX, H, intervalLevels, verySparseInterval);
                    updateAggregationPath(tail, h);
                    System.out.println("Result = " + DHiX.get(tail).a);
                    tSlide = ti;
                }
            }

            //h = (h + 1) % DHiX.size(); // or simply: h++;
            // ✅ Automatically reset h = 0 if about to be full
            if (h == capacity - 1) {
                h = 0;
            } else {
                h++;
            }

        }
    }

    // Reuse INSERT() and QUERY() functions from previous responses (Algorithm 1 & 2)
    // You can call:
    // insert(ti, h, DHiX, H, intervalLevels, C, verySparseInterval);
    // int tail = query(ti, W, h, DHiX, H, intervalLevels, verySparseInterval);
    
    void insert(long ti, int h, List<Record> DHiX, Map<Long, Integer> H,
            List<Pair<Long, Map<Integer, List<Integer>>>> intervalLevels,
            long C, List<Integer> verySparseInterval) {

    if (!unitInitialized) {
        initializeUnitAndLevels(DHiX, intervalLevels);
    }

    long gap = 0;
    long tPrev = 0;
    if (DHiX.size() > 1)
    {
        tPrev = DHiX.get(h - 1).timestamp;
        gap = ti - tPrev;
    }
    else
    {
        gap = C+1;
    }
    

    if (gap <= C) {
        for (long tm = tPrev + 1; tm <= ti; tm++) {
            H.put(tm, h);
        }
        return;
    }

    for (Pair<Long, Map<Integer, List<Integer>>> level : intervalLevels) {
        long size = level.getKey();
        Map<Integer, List<Integer>> intervalDict = level.getValue();

        if (gap <= size) {
            int idx = (int) (ti / size);
            intervalDict.computeIfAbsent(idx, k -> new ArrayList<>()).add(h);

            long intervalStart = (ti / size) * size;

            if (tPrev < intervalStart) {
                int idxPrev = (int) (tPrev / size);
                intervalDict.computeIfAbsent(idxPrev, k -> new ArrayList<>()).add(h - 1);
            }
            return;
        }
    }

    verySparseInterval.add(h);
}
    
    Integer query(long ti, long wt, int h, List<Record> DHiX, Map<Long, Integer> H,
              List<Pair<Long, Map<Integer, List<Integer>>>> intervalLevels,
              List<Integer> verySparseInterval) {

    long targetTime = ti - wt;

    if (targetTime < 0) return null;
    if (H.containsKey(targetTime)) return H.get(targetTime);

    for (Pair<Long, Map<Integer, List<Integer>>> level : intervalLevels) {
        Integer res = scan(level.getValue(), level.getKey(), targetTime, DHiX, h);
        if (res != null) return res;
    }

    for (int i : verySparseInterval) {
        Integer res = scanSingle(i, targetTime, DHiX, h);
        if (res != null) return res;
    }

    return null;
}

private Integer scan(Map<Integer, List<Integer>> intervalDict, long size,
                     long targetTime, List<Record> DHiX, int h) {
    int idx = (int) (targetTime / size);
    List<Integer> candidates = intervalDict.getOrDefault(idx, new ArrayList<>());

    for (int i : candidates) {
        long tCur = DHiX.get(i).timestamp;
        long tPre = (i > 0) ? DHiX.get(i - 1).timestamp : Long.MIN_VALUE;

        if (tPre == targetTime) return i - 1;
        if (tPre < targetTime && targetTime <= tCur) return i;

        if (i == candidates.get(candidates.size() - 1)) {
            long tNext = (i < h - 1) ? DHiX.get(i + 1).timestamp : Long.MAX_VALUE;
            if (tCur < targetTime && targetTime <= tNext) return i + 1;
        }
    }
    return null;
}

private Integer scanSingle(int i, long targetTime, List<Record> DHiX, int h) {
    long tCur = DHiX.get(i).timestamp;
    long tPre = (i > 0) ? DHiX.get(i - 1).timestamp : Long.MIN_VALUE;

    if (tPre == targetTime) return i - 1;
    if (tPre < targetTime && targetTime <= tCur) return i;

    if (i < h - 1) {
        long tNext = DHiX.get(i + 1).timestamp;
        if (tCur < targetTime && targetTime <= tNext) return i + 1;
    }
    return null;
}


//boolean unitInitialized = false;
//long C;

void initializeUnitAndLevels(List<Record> DHiX,
                             List<Pair<Long, Map<Integer, List<Integer>>>> intervalLevels) {

    int sSize = Math.min(10, DHiX.size());
    List<Long> tStamps = new ArrayList<>();
    for (int i = 0; i < sSize; i++) {
        tStamps.add(DHiX.get(i).timestamp);
    }

    String unit = "nanoseconds";
    List<String> timeUnits = Arrays.asList("nanoseconds", "microseconds", "milliseconds", "seconds", "minutes", "hours");

    Map<String, Long> timeMultipliers = Map.of(
        "nanoseconds", 1L,
        "microseconds", 1_000L,
        "milliseconds", 1_000_000L,
        "seconds", 1_000_000_000L,
        "minutes", 60_000_000_000L,
        "hours", 3_600_000_000_000L
    );

    int baseIndex = timeUnits.indexOf(unit);
    C = timeMultipliers.get(unit);

    for (int i = baseIndex + 1; i < timeUnits.size(); i++) {
        long size = timeMultipliers.get(timeUnits.get(i));
        intervalLevels.add(new Pair<>(size, new HashMap<>()));
    }

    unitInitialized = true;
}


}
