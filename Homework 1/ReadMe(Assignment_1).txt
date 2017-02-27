Name   : Ankita S. Patil
Subject: Big data management and analytics


===========================================================================================================================================
Question 1
============================================================================================================================================
 Package name: BigData
 Class name  : BusinessCategoryPA.java
 Jar name    : BusinessCategoryPA.jar
 

(1) Log into HDFS

(2) Run the following command
	
	hadoop jar BusinessCategoryPA.jar BigData.BusinessCategoryPA <location of business.csv> /outputBusinessCategoryPA_v5/
	
	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
        hadoop jar BusinessCategoryPA.jar BigData.BusinessCategoryPA /NameOfTheDirectory/business.csv  /NameOfTheDirectory/outputBusinessCategoryPA_v5/

(3)To display the contents of an output file

	hadoop fs -cat /outputBusinessCategoryPA_v5/*
	
	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
	hadoop fs -cat /NameOfTheDirectory/outputBusinessCategoryPA_v5/*

=============================================================================================================================================
Question 2
=============================================================================================================================================
 Package name: BigData
 Class name  : TopTenRatedBusiness.java
 Jar name    : TopTenRatedBusiness.jar

(1) Log into HDFS

(2) Run the following command

    hadoop jar TopTenRatedBusiness.jar BigData.TopTenRatedBusiness <location of review.csv> /outputTopTenRatedBusiness_v2/
    
	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
	hadoop jar TopTenRatedBusiness.jar BigData.TopTenRatedBusiness /NameOfTheDirectory/review.csv /NameOfTheDirectory/outputTopTenRatedBusiness_v2/

(3)To display the contents of an output file
	
	hadoop fs -cat /outputTopTenRatedBusiness_v2/*
	
	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
        hadoop fs -cat /NameOfTheDirectory/outputTopTenRatedBusiness_v2/*

=========================================================================================================================================================================
Question 3
========================================================================================================================================================================
 Package name: BigData
 Class name  : TopRatedBusinessData.java
 Jar name    : TopRatedBusinessData.jar

 (1) Log into HDFS

 (2) Run the following command
     
	hadoop jar TopRatedBusinessData.jar BigData.TopRatedBusinessData <location of review.csv> <location of business.csv> /outputTBD1_v9/ /outputTBD2_v9/

	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
        hadoop jar TopRatedBusinessData.jar BigData.TopRatedBusinessData /NameOfTheDirectory/review.csv /asp160730/business.csv /NameOfTheDirectory/outputTBD1_v9/ /NameOfTheDirectory/outputTBD2_v9/
  
  (3)To display the contents of an output file
    
	hadoop fs -cat /outputTBD2_v9/*
	
	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
        hadoop fs -cat /NameOfTheDirectory/outputTBD2_v9/*

==============================================================================================================================
Question 4
==============================================================================================================================
 Package name: BigData
 Class name  : UsersAndRating.java
 Jar name    : UsersAndRating.jar

 (1) Log into HDFS

 (2) Run the following command
	
	hadoop jar UsersAndRating.jar BigData.UsersAndRating <location of review.csv> <location of business.csv> /outputUsersAndRating_v4/

	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
	hadoop jar UsersAndRating.jar BigData.UsersAndRating /NameOfTheDirectory/review.csv /NameOfTheDirectory/business.csv /NameOfTheDirectory/outputUsersAndRating_v4/

 (3)To display the contents of an output file
    
	hadoop fs -cat /outputUsersAndRating_v4/*
	
	I ran the program on UTD's hadoop cluster using following command in the directory => /NameOfTheDirectory/
	hadoop fs -cat /NameOfTheDirectory/outputUsersAndRating_v4/*
