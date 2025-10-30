import pandas as pd
from  rdflib import Graph, Namespace, RDF, Literal, RDFS

#read data from readings_normalized.csv
df = pd.read_csv("../data/readings_normalized.csv")
print("this is df", df.head())

#new rdf graph
g = Graph()

#defined namespace
ex = Namespace("https://example.com/project-4")
cco = Namespace ("https://www.commoncoreontologies.org")
obo = Namespace(("http://purl.obolibrary.org/obo/"))
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns")
rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema")


#looping through each row and adding triples
for index, row in df.iterrows():
    artifact_id = str(row["artifact_id"])
    sdc_kind = str(row["sdc_kind"])
    unit_label = str(row["unit_label"])
    value = row["value"]
    timestamp = str(row["timestamp"])

    #create uris/getting stuff from data
    artifact_id_uri= ex[artifact_id]
    sdc_kind_uri= ex[sdc_kind]
    unit_label_uri= ex[unit_label]
    value_uri= ex[value]


    #adding to graph/conforming to design model
    #subject, predicate,object(the class i am assingning to the subject))
    #
    g.add((artifact_id, RDF.type, cco.Artifact))
    g.add((artifact_id, cco.bearerof, sdc_kind_uri))

    g.add((sdc_kind_uri, RDF.type, obo.SpecificallyDepententContinuant))

    g.add((unit_label_uri,RDF.type, cco.MeasurementUnit))

    g.add((value_uri, RDF.type, cco.MeasurementInformationConentEntity))
    g.add((value_uri, cco.measureof, sdc_kind_uri))
    g.add((value_uri, cco.usesmeasurmentUnit, unit_label_uri))
    g.add((value_uri, cco.hasvalue, value))
    g.add((value_uri, cco.hasvalue,timestamp))

#Run script to turtle file
g.serialize(destination="measure_cco.ttl", format="turtle")
