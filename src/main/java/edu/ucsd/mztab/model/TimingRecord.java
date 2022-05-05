package edu.ucsd.mztab.model;

public class TimingRecord
{
    /*====================================================================
     * Properties
     *====================================================================*/
    private Integer count;
    private Long    total;
    private Long    max;
    private Long    min;

    /*====================================================================
     * Constructor
     *====================================================================*/
    public TimingRecord() {
        count = 0;
        total = 0L;
        max = null;
        min = null;
    }

    /*====================================================================
     * Property accessor methods
     *====================================================================*/
    public Integer getCount() {
        return count;
    }

    public Long getTotal() {
        return total;
    }

    public Long getMax() {
        return max;
    }

    public Long getMin() {
        return min;
    }

    /*====================================================================
     * Public interface methods
     *====================================================================*/
    public void add(long elapsed) {
        // add this timing to running total
        total += elapsed;
        // compare this timing to current max and min
        if (max == null || max < elapsed)
            max = elapsed;
        if (min == null || min > elapsed)
            min = elapsed;
        // increment count
        count++;
    }

    public Double getAverage() {
        if (count < 1)
            return null;
        else return (double)total / count;
    }

    @Override
    public String toString() {
        StringBuilder record = new StringBuilder();
        Integer count = getCount();
        record.append(count);
        record.append("\t").append(getTotal());
        if (count != null && count > 1) {
            record.append("\t").append(getMax());
            record.append("\t").append(getMin());
            record.append("\t").append(getAverage());
        } else record.append("\t--\t--\t--");
        return record.toString();
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
