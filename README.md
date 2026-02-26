# Test_Analysis_Email_Data
Work Assignment - Technical Test

Completed this assignment in 2 way - In the first approach Clasical Java Map Reduce has been used. Inthe second approach
Scala with Spark has been used.

All the codes that has been wriiten using Java Map Reduce is be available under EmailDataAnalysis_JavaMapReduce folder.
For given two problem statement two different java packages has been created and package names are given below
1. com.java.mapreduce.test.avgwordcount - containing the solution for Average Email Word Count.
2. com.java.mapreduce.test.toprecipient - containing the solution for Top 100 recipient email address.

Simliar thing has been done using scala and solution is available under EmailDataAnalysisusingScala folder in repository.

Assumtion::
Due to access issue could not be able to test this on AWS. Codes are tested in local environment keeping the constraints that all the mail file will be available in
some input directory. 
Edit View
from il • 47125 • httpsc •| Sectio • Intellig• Aclosi•linker.txt
H1v BIe
File
Objective of this pilot is to implement Ontologies & Knowledge Graph, for a high priority supply chain usecase ine - eBOM navigation to demonstrate business impact.
- Data Source Layer
Redshift (ODP Database) provides the core entity layer, extracted monthly as CSV files using AWS Glue.
Ontology (from Protégé) provides the Bill of Materials (BOM) semantic model, exported as RDF on an ad-hoc basis.
Data Processing Layer
AWS Processing
AWS Glue extracts data from Redshift and unloads it into Amazon S3.
Data undergoes transformations, dataset joins, and is converted into RDF Triples.
•
The final output is written in TT format.
Graph Processing
•
Neptune Bulk Loader ingests transformed TTL files from S3 into Amazon Neptune.
Neptune stores the integrated Knowledge Graph.
Consumption Layer
Graph Visualization
•
Neptune Workbench / Notebook enables graph-native exploration using a Jupyter based interface.
Relational Visualization
Neptune data can be converted into relational form and queried using:
Amazon Athena
Supports SQL-like querying and ad-hoc analytics over graph data.
