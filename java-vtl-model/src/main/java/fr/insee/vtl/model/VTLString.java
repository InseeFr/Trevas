package fr.insee.vtl.model;

public abstract class VTLString extends VTLObject<String> implements VTLTyped<VTLString> {

    @Override
    public Class<VTLString> getVTLType() {
        return VTLString.class;
    }

    private VTLString() {
        // private.
    }

    public static VTLString of(String string) {
        return new VTLString() {
            @Override
            public String get() {
                // TODO: This is hot code. We should rather check in the comparator.
                return "".equals(string) ? null : string;
            }
        };
    }

}
