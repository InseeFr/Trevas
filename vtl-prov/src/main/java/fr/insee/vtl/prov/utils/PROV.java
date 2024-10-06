package fr.insee.vtl.prov.utils;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

/**
 * Vocabulary definition for the <a href="https://www.w3.org/TR/prov-o/">PROV Ontology</a>.
 * 
 * @see <a href="http://www.w3.org/ns/prov-o">Turtle specification</a>
 */
public class PROV {
	/**
	 * The RDF model that holds the PROV entities
	 */
	private static Model model = ModelFactory.createDefaultModel();
	/**
	 * The namespace of the PROV vocabulary as a string
	 */
	public static final String uri = "http://www.w3.org/ns/prov#";
	/**
	 * Returns the namespace of the PROV vocabulary as a string
	 * @return the namespace of the PROV vocabulary
	 */
	public static String getURI() {
		return uri;
	}
	/**
	 * The namespace of the PROV vocabulary
	 */
	public static final Resource NAMESPACE = model.createResource(uri);

	/* ##########################################################
	 * Defines PROV Classes
	   ########################################################## */

	public static final Resource Entity = model.createResource(uri + "Entity");
	public static final Resource Activity = model.createResource(uri + "Activity");
	public static final Resource Agent = model.createResource(uri + "Agent");
	public static final Resource Collection = model.createResource(uri + "Collection");
	public static final Resource EmptyCollection = model.createResource(uri + "EmptyCollection");
	public static final Resource Bundle = model.createResource(uri + "Bundle");
	public static final Resource Person = model.createResource(uri + "Person");
	public static final Resource SoftwareAgent = model.createResource(uri + "SoftwareAgent");
	public static final Resource Organization = model.createResource(uri + "Organization");
	public static final Resource Location = model.createResource(uri + "Location");
	public static final Resource Influence = model.createResource(uri + "Influence");
	public static final Resource EntityInfluence = model.createResource(uri + "EntityInfluence");
	public static final Resource Usage = model.createResource(uri + "Usage");
	public static final Resource Start = model.createResource(uri + "Start");
	public static final Resource End = model.createResource(uri + "End");
	public static final Resource Derivation = model.createResource(uri + "Derivation");
	public static final Resource PrimarySource = model.createResource(uri + "PrimarySource");
	public static final Resource Quotation = model.createResource(uri + "Quotation");
	public static final Resource Revision = model.createResource(uri + "Revision");
	public static final Resource ActivityInfluence = model.createResource(uri + "ActivityInfluence");
	public static final Resource Generation = model.createResource(uri + "Generation");
	public static final Resource Communication = model.createResource(uri + "Communication");
	public static final Resource Invalidation = model.createResource(uri + "Invalidation");
	public static final Resource AgentInfluence = model.createResource(uri + "AgentInfluence");
	public static final Resource Attribution = model.createResource(uri + "Attribution");
	public static final Resource Association = model.createResource(uri + "Association");
	public static final Resource Plan = model.createResource(uri + "Plan");
	public static final Resource Delegation = model.createResource(uri + "Delegation");
	public static final Resource InstantaneousEvent = model.createResource(uri + "InstantaneousEvent");
	public static final Resource Role = model.createResource(uri + "Role");

	/* ##########################################################
	 * Defines PROV Properties
	   ########################################################## */
	public static final Property wasGeneratedBy = model.createProperty(uri + "wasGeneratedBy");
	public static final Property wasDerivedFrom = model.createProperty(uri + "wasDerivedFrom");
	public static final Property wasAttributedTo = model.createProperty(uri + "wasAttributedTo");
	public static final Property startedAtTime = model.createProperty(uri + "startedAtTime");
	public static final Property used = model.createProperty(uri + "used");
	public static final Property wasInformedBy = model.createProperty(uri + "wasInformedBy");
	public static final Property endedAtTime = model.createProperty(uri + "endedAtTime");
	public static final Property wasAssociatedWith = model.createProperty(uri + "wasAssociatedWith");
	public static final Property actedOnBehalfOf = model.createProperty(uri + "actedOnBehalfOf");
	public static final Property alternateOf = model.createProperty(uri + "alternateOf");
	public static final Property specializationOf = model.createProperty(uri + "specializationOf");
	public static final Property generatedAtTime = model.createProperty(uri + "generatedAtTime");
	public static final Property hadPrimarySource = model.createProperty(uri + "hadPrimarySource");
	public static final Property value = model.createProperty(uri + "value");
	public static final Property wasQuotedFrom = model.createProperty(uri + "wasQuotedFrom");
	public static final Property wasRevisionOf = model.createProperty(uri + "wasRevisionOf");
	public static final Property invalidatedAtTime = model.createProperty(uri + "invalidatedAtTime");
	public static final Property wasInvalidatedBy = model.createProperty(uri + "wasInvalidatedBy");
	public static final Property hadMember = model.createProperty(uri + "hadMember");
	public static final Property wasStartedBy = model.createProperty(uri + "wasStartedBy");
	public static final Property wasEndedBy = model.createProperty(uri + "wasEndedBy");
	public static final Property invalidated = model.createProperty(uri + "invalidated");
	public static final Property influenced = model.createProperty(uri + "influenced");
	public static final Property atLocation = model.createProperty(uri + "atLocation");
	public static final Property generated = model.createProperty(uri + "generated");
	public static final Property wasInfluencedBy = model.createProperty(uri + "wasInfluencedBy");
	public static final Property qualifiedInfluence = model.createProperty(uri + "qualifiedInfluence");
	public static final Property qualifiedGeneration = model.createProperty(uri + "qualifiedGeneration");
	public static final Property qualifiedDerivation = model.createProperty(uri + "qualifiedDerivation");
	public static final Property qualifiedPrimarySource = model.createProperty(uri + "qualifiedPrimarySource");
	public static final Property qualifiedQuotation = model.createProperty(uri + "qualifiedQuotation");
	public static final Property qualifiedRevision = model.createProperty(uri + "qualifiedRevision");
	public static final Property qualifiedAttribution = model.createProperty(uri + "qualifiedAttribution");
	public static final Property qualifiedInvalidation = model.createProperty(uri + "qualifiedInvalidation");
	public static final Property qualifiedStart = model.createProperty(uri + "qualifiedStart");
	public static final Property qualifiedUsage = model.createProperty(uri + "qualifiedUsage");
	public static final Property qualifiedCommunication = model.createProperty(uri + "qualifiedCommunication");
	public static final Property qualifiedAssociation = model.createProperty(uri + "qualifiedAssociation");
	public static final Property qualifiedEnd = model.createProperty(uri + "qualifiedEnd");
	public static final Property qualifiedDelegation = model.createProperty(uri + "qualifiedDelegation");
	public static final Property influencer = model.createProperty(uri + "influencer");
	public static final Property entity = model.createProperty(uri + "entity");
	public static final Property hadUsage = model.createProperty(uri + "hadUsage");
	public static final Property hadGeneration = model.createProperty(uri + "hadGeneration");
	public static final Property activity = model.createProperty(uri + "activity");
	public static final Property agent = model.createProperty(uri + "agent");
	public static final Property hadPlan = model.createProperty(uri + "hadPlan");
	public static final Property hadActivity = model.createProperty(uri + "hadActivity");
	public static final Property atTime = model.createProperty(uri + "atTime");
	public static final Property hadRole = model.createProperty(uri + "hadRole");
}