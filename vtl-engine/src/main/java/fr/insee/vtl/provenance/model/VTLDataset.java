package fr.insee.vtl.provenance.model;

import java.util.List;

public class VTLDataset {

    private String name;
    private Activity wasGeneratedBy;
    private List<VTLDataset> wasDerivedFrom;

    public VTLDataset(String name) {
        this.name = name;
    }

    public VTLDataset(String name, Activity wasGeneratedBy) {
        this.name = name;
        this.wasGeneratedBy = wasGeneratedBy;
    }

    public VTLDataset(String name, Activity wasGeneratedBy, List<VTLDataset> wasDerivedFrom) {
        this.name = name;
        this.wasGeneratedBy = wasGeneratedBy;
        this.wasDerivedFrom = wasDerivedFrom;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Activity getWasGeneratedBy() {
        return wasGeneratedBy;
    }

    public void setWasGeneratedBy(Activity wasGeneratedBy) {
        this.wasGeneratedBy = wasGeneratedBy;
    }

    public List<VTLDataset> getWasDerivedFrom() {
        return wasDerivedFrom;
    }

    public void setWasDerivedFrom(List<VTLDataset> wasDerivedFrom) {
        this.wasDerivedFrom = wasDerivedFrom;
    }


}
