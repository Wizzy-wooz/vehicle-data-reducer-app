VehicleDateReducerApp starts a spark job that processes files of vehicle data. 
The data will include vehicle id, sample time and data from various sensors. 
The process will include multiple steps:

1. Read input CSV file.
2. Alter the data fields based on configuration.
3. The process will use a configuration file that map between the input file fields to the required output fields. Each original field will be assigned an output name. if no name is indicated, that means the field should not be part of the output. You can find an example configuration file attached.
Reduce the samples frequency the sample_time column is representing the time the sample was taken in epoch time. The process will make sure every vehicle have no more than one sample in every 10 seconds window. If there are more samples than that it will keep only the last one.
4. Write output as JSON file.
   
Application takes 5 arguments:
1. configPath - schema config path;
2. sourcePath - path to read input CSV file;
3. targetPath - path to output as JSON file;
4. windowDuration - default 10 seconds; 
5. parallelism - default value is 1 - result will be saved in single json file. 

Solution testing has been made on:
1. example files.
2. example snippet. 

Received results match with provided.