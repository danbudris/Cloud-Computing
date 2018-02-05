
##############################
### Running on Laptop     ####
##############################


Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.bu.metcs.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 



running:

	WordCount Book-Tiny.txt  OUTPUT.txt

###########################################
##### Create a JAR File from eclipse ######
###########################################


Create a single gar file with eclipse 

File export -> export  -> export as binary -> 

"Extract generated libraries into generated JAR"


##############################
### AWS Elastic MapReduce ####
##############################


Running on AWS Elastic MapReduce 

As argument give your input and output directories. 

    s3://metcs755/WikipediaPages_oneDocPerLine_1000Lines_small.txt   s3://metcs755/output


##############################
## Tutorial Link for Maven ###
##############################

Hadoop: Setup Maven project for MapReduce in 5mn
https://hadoopi.wordpress.com/2013/05/25/setup-maven-project-for-hadoop-in-5mn/







