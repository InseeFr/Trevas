package fr.insee.vtl.model;

import static java.lang.String.*;

public abstract class VTLNumber<T extends Number> extends VTLObject<T> {

    VTLNumber() {
        // private.
    }

    @Override
    public abstract T get();

    @Override
    public int compareTo(Object o) {
        if (o instanceof VTLNumber) {
            VTLNumber that = ((VTLNumber) o);
            return Double.compare(this.get().doubleValue(), that.get().doubleValue());
        } else {
            return super.compareTo(o);
        }
    }

    public VTLNumber add(VTLNumber addend) {
        return add(addend.get());
    }
    
    public VTLNumber add(Number addend) {
        Number augend = get();
        if (augend instanceof Double || addend instanceof Double) {
                return VTLNumber.of(augend.doubleValue() + addend.doubleValue());
        } else if (augend instanceof Long || addend instanceof Long) {
                return VTLNumber.of(augend.longValue() + addend.longValue());
        }
    
        throw new RuntimeException(
                format("unsupported number types %s, %s", augend.getClass(), addend.getClass())
        );
        
    }
    
    public VTLNumber subtract(VTLNumber subtrahend) {
        return subtract((Number) subtrahend.get());
    }
    
    public VTLNumber subtract(Number subtrahend) {
        Number minuend = get();
        if (minuend instanceof Double || subtrahend instanceof Double) {
            return VTLNumber.of(minuend.doubleValue() - subtrahend.doubleValue());
        } else if (minuend instanceof Long || subtrahend instanceof Long) {
            return VTLNumber.of(minuend.longValue() - subtrahend.longValue());
        }
        
        throw new RuntimeException(
                format("unsupported number types %s, %s", minuend.getClass(), subtrahend.getClass())
        );
    }
    
    public VTLNumber multiply(VTLNumber multiplicand) {
        return multiply((Number) multiplicand.get());
    }
    
    public VTLNumber multiply(Number multiplicand) {
        Number multiplier = get();
        if (multiplier instanceof Double || multiplicand instanceof Double) {
            return VTLNumber.of(multiplier.doubleValue() * multiplicand.doubleValue());
        } else if (multiplier instanceof Long || multiplicand instanceof Long) {
            return VTLNumber.of(multiplier.longValue() * multiplicand.longValue());
        }
        throw new RuntimeException(format("unsupported number types %s, %s", multiplier.getClass(), multiplicand.getClass()));
    }
    
    public VTLNumber divide(VTLNumber divisor) {
        return divide((Number) divisor.get());
    }
    
    public VTLNumber divide(Number divisor) {
        Number dividend = get();
        return VTLNumber.of(dividend.doubleValue() / divisor.doubleValue());
    }
}
