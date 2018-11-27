package com.tool.dataSync;

import lombok.Data;

import java.io.Serializable;

@Data
public final class Pair<U, V> implements Serializable {

    public final U first;
    public final V second;

    public Pair(U fst, V scnd) {
        first = fst;
        second = scnd;
    }

    public static <U, V> Pair<U, V> create(U first, V second) {
        return new Pair<>(first, second);
    }

    public U getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?, ?> other = (Pair<?, ?>) obj;
        if (first == null) {
            if (other.first != null) {
                return false;
            }
        } else if (!first.equals(other.first)) {
            return false;
        }
        if (second == null) {
            if (other.second != null) {
                return false;
            }
        } else if (!second.equals(other.second)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "(" + first + "," + second + ")";
    }

}
