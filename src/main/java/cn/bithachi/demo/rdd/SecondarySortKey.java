package cn.bithachi.demo.rdd;

import scala.math.Ordered;

import java.io.Serializable;
import java.util.Objects;

/**
 * 二次排序
 *
 * 比较器：
 * 1. 实现 Ordered<T> 和 Serializable 接口
 * 2. 重写 equals 和 hashCode 方法
 * 3. 重写 $less ，$greater ，$less$eq ，$greater$eq 方法
 * 4. 重写 compare ， compareTo 方法
 * 5. 构造函数
 */

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    int first;
    int second;

    public SecondarySortKey() {
    }

    /**
     * 构造方法
     *
     * @param first
     * @param second
     */
    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public boolean $less(SecondarySortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }
        return false;
    }

    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    public int compare(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return that.second - this.getSecond();
        }
    }

    public int compareTo(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return that.second - this.getSecond();
        }
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondarySortKey that = (SecondarySortKey) o;
        return first == that.first && second == that.second;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}

