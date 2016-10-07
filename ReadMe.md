## Synopsis

This is a Scala Project, based on Big Data Analysis using Apache Spark APIs. The project aims 
to retrieve information from email files, and make some simple analysis via ingestion of these files.


## Code Example

Note: Please read the comments in AvarageWordCalculator and TopRecipentsCalculator classes!

From the client's perspective the commands can be called as below:
'

	val trc = new TopRecipentsCalculator
    val result = trc.reduce(sc, pathForXML) 
    
    val awc = new AvarageWordCalculator
    awc.getAvarageWordsPerMail(sc, pathForMails) 
    

## Motivation

The Analysis answers 2 questions
1. What is the avarage length, in words of the emails
2. Which are the top 100 recipient email addresses

## Installation

After downloading the project, you should be running from the command line sbt.

## Tests

There are 2 main test cases. Each are per task given in the assignments
You can run them using the following command

cmd> sbt test

## Contributors

Mehmet Oguz Divilioglu, Email: mo.divilioglu@gmail.com