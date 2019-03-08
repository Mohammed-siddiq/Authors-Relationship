
## Description

Created a map/reduce program for parallel processing of the [publically available DBLP dataset](https://dblp.uni-trier.de) that contains entries for various publications at many different venues (e.g., conferences and journals). Raw [XML-based DMBLP dataset](https://dblp.uni-trier.de/xml) is also publically available along with its schema and the documentation.

Each entry in the dataset describes a publication, which contains the list of authors, the title, and the publication venue and a few other attributes. The file is approximately **2.5Gb**.

Consider the following entry in the dataset.
```xml
<inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
<author>Mark Grechanik</author>
<author>B. M. Mainul Hossain</author>
<author>Ugo Buy</author>
<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
<pages>174-183</pages>
<year>2013</year>
<booktitle>ICST</booktitle>
<ee>https://doi.org/10.1109/ICST.2013.19</ee>
<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
<crossref>conf/icst/2013</crossref>
<url>db/conf/icst/icst2013.html#GrechanikHB13</url>
</inproceedings>
```

This entry lists a paper at the IEEE International Conference on Software Testing, Verification and Validation (ICST) published in 2013 whose authors are my former Ph.D. student at UIC, now tenured Associate Professor at the University of Dhaka, Dr. B.M. Mainul Hussain whose advisor Mark Grechanik is a co-author on this paper. The third co-author is Prof.Ugo Buy, a faculty member at our CS department. The presence of two authors, Mark Grechanik and Ugo Buy in a single publication like this one establishes a connection between these faculty members. Your job is to create a "friendship" connectivity graph between UIC CS faculty members using the information extracted from this dataset. Paritioning this dataset into shards is easy, since it requires to preserve the well-formedness of XML only. Most likely, you will write a simple program to partition the dataset into an approximately equal size shards.


After creating and testing this map/reduce program locally, THhe job was deploy and run on the Amazon Elastic MapReduce (EMR).

## Steps to RUN on VM :

- Run `sbt clean assembly` , which will run the test cases and run generate the JAR
- Copy the jar on the machine.(Machine needs to have java 1.8)
- Run the following command :

```
hadoop jar Mohammed_Siddiq_HW3-assembly-0.1.jar RelateAuthors.JobRunner path/to/input/file path/to/output/file
```

##Steps to RUN on Cloud :

- Please refer the video uploaded on youtube to run the Job in on th cloud
 
    [Link to Youtube Video](https://youtu.be/6OFhZpm7-6U)



## Solution Overview

- Implemented XmlInputFormat that implements the DataRecordReader to generated Key,values based on the multiple start tags and end tags. Start and end Tag considered 

START_TAGS 

```
<article ,<inproceedings ,<proceedings ,<book ,<incollection ,<phdthesis ,<mastersthesis "`
```
END_TAGS 

```
</article>, </inproceedings>,</proceedings>,</book>,</incollection>,</phdthesis>,</mastersthesis>
```

- **The _Mapper_ :**
  -  processes the individual papers/publications given as xml by InputFormat 
  - Extracts the authors and filters the CS authors of UIC.
  - Emits the Individual authors as keys and 1 as the value .
  - Also emits the co-authors as the key and 1 as the value.
  - Signifying authors work alone and with the co-authors from the CS faculty of UIC.
  
  _For example:_
  
  suppose `(a1,a2,a3)` are authors of a paper then the mapper emits:
    
        (a1)->1,(a2)->1,(a3)->1,(a1,a2)->1,(a1,a3)->1,(a2,a3)->1
    
- **_The Combiner and Reducer :_** 
    
    - The combiner and reducer adds all the corresponding values of the keys, thereby summing up all the number of publications of individual authors and the co-authors.
    
The sample output of the mapper would look like this :
          
      a. prasad sistla	130
      a. prasad sistla,isabel f. cruz	2
      a. prasad sistla,lenore d. zuck	7
      a. prasad sistla,ouri wolfson	22
      a. prasad sistla,robert h. sloan	1
      a. prasad sistla,v. n. venkatakrishnan	8
      ajay d. kshemkalyani	113
      ajay d. kshemkalyani,ugo buy	1
      anastasios sidiropoulos	101
      anastasios sidiropoulos,bhaskar dasgupta	1
      andrew e. johnson	102
      andrew e. johnson,barbara di eugenio	2
      andrew e. johnson,luc renambot	26
      andrew e. johnson,tanya y. bergerwolf	1
      balajee vamanan	15
      barbara di eugenio	116
      barbara di eugenio,bing liu	1
      barbara di eugenio,brian d. ziebart	1
      barbara di eugenio,isabel f. cruz	2
      barbara di eugenio,luc renambot	1
      barbara di eugenio,ouri wolfson	2
      barbara di eugenio,peter c. nelson	3
      bhaskar dasgupta	142
      bhaskar dasgupta,nasim mobasheri	9
      bhaskar dasgupta,ouri wolfson	5
      bhaskar dasgupta,robert h. sloan	2
      bhaskar dasgupta,tanya y. bergerwolf	12
  
          
This final output directory of the reducers is given to the Graph Visualization tool written in graphviz to generate an PNG image representing friendship graph between professors. Individual nodes represent the CS faculty and the Edges between them represent the friendship(co-publishers).

The weights associated with the edges represent the number of times the they published together. The weights of the individual nodes represent the total number of publication of the author.

 [Link to GraphViz implementation to generate the DOT file from the output and present the graph](https://bitbucket.org/Iam_MohammedSiddiq/mohammed_siddiq_hw2_graphviz/src/master/)
 
 
 
 ![Graph of UIC CS faculty Researchers](https://drive.google.com/file/d/10s2qEnf3xRBm78Q2qkmKgWktAP9KrC2K/view?usp=sharing "Logo Title Text 1")
 
 
 
 [Sample output PNG representing the individual publications and friendship graph of the UIC](https://drive.google.com/file/d/10s2qEnf3xRBm78Q2qkmKgWktAP9KrC2K/view?usp=sharing) 
 (Pro tip : Zoom in for the details.)
 

 
 [And Finally here's the link to the youtube video](https://youtu.be/6OFhZpm7-6U)
 
 

