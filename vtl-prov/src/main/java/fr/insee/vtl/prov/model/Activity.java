package fr.insee.vtl.prov.model;

public class Activity {

    private String name;
    //TODO: to handle later
    //private List<VTLDataset> used;
    private String label;

    public Activity(String name, String label) {
        this.name = name;
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
