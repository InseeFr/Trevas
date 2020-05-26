package fr.insee.vtl.model;

public abstract class VTLBoolean extends VTLObject<Boolean> implements VTLTyped<VTLBoolean> {

    private VTLBoolean() {
        // private.
    }

    private static final VTLBoolean TRUE = new VTLBoolean() {
        @Override
        public Boolean get() {
            return true;
        }
    };

    private static final VTLBoolean FALSE = new VTLBoolean() {
        @Override
        public Boolean get() {
            return false;
        }
    };

    @Override
    public Class<VTLBoolean> getVTLType() {
        return VTLBoolean.class;
    }

    public static VTLBoolean of(Boolean value) {
        return value == null ? new VTLBoolean() {
            @Override
            public Boolean get() {
                return null;
            }
        } : value ? TRUE : FALSE;
    }

}
