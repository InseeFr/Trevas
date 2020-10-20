package fr.insee.vtl.model;

import java.util.concurrent.atomic.AtomicLong;

public class SumExpression implements AggregationExpression<AtomicLong, Number, Long> {

    private final ResolvableExpression expression;

    public SumExpression(ResolvableExpression expression) {
        this.expression = expression;
    }

    @Override
    public AtomicLong init() {
        return new AtomicLong(0);
    }

    @Override
    public void accept(AtomicLong sum, Number o) {
        sum.addAndGet(o.longValue());
    }

    @Override
    public AtomicLong combine(AtomicLong c1, AtomicLong c2) {
        c1.addAndGet(c2.get());
        return c1;
    }

    @Override
    public Long finish(AtomicLong sum) {
        return sum.get();
    }

    @Override
    public ResolvableExpression getExpression() {
        return expression;
    }

    @Override
    public Class<?> getType() {
        return Long.class;
    }
}
