package fr.insee.vtl.model;

public abstract class VTLInteger extends VTLNumber<Long>  implements VTLTyped<VTLInteger> {

    public static final VTLInteger NULL = VTLInteger.of((Integer) null);

    private VTLInteger() {
    }

    @Override
    public Class<VTLInteger> getVTLType() {
        return VTLInteger.class;
    }

    public static VTLInteger of(Integer value) {
        return VTLInteger.of(value != null ? value.longValue() : null);
    }

    public static VTLInteger of(Long value) {
        return new VTLInteger() {
            @Override
            public Long get() {
                return value;
            }
        };
    }
}
