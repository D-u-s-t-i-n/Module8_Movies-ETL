# Module8_Movies-ETL (Challenge Notes)

## The challenge.py contains:
### LoadData initial main function that accepts 3 filenames (kaggle, wiki, and ratings). If all files are present, it will call the next function.
### CleanData function is called after confirming the 3 data files exist. Data cleaning sub functions are called from here:
- fill_missing_kaggle_data
- parse_dollars
- clean_movie
### UploadToDatabase is called after successful cleaning. A global boolean variable bool_success is used to track if any exceptions were caught during the cleaning process and prevent the data files from being marked processed.

## Assumptions
- The raw data format remains the same (kaggle csv, ratings csv, wiki json).
- The data column formatting messiness is not too deviated from the one given in the module lessons. Since we are not given any other data sources, it is difficult to test and account for all potential formats that may cause the cleanup function to fail. Try-except was added for dropping duplicate imdb ids.
- The program assumes the graders have their postgre login info ready in their own config.py file
- For grading, the same dataset will be used for test so I did not upload the large ~700 MB file. If generating the raw data was part of the exercise, I would have put effort to upload the large raw data files.
- Processed data should not be deleted, so they are moved to
    -- Separate from new incoming data
    -- Prevent the same data from being run over and over again. 
    

## Areas of Improvement
Due to my limited Python ability and time constraints, I see the following possible areas for improvement.
- In the `loaddata` function, I think the if-else file check portion can be replaced by try-exception.
- For database upload, it's standard practice to check for existing records and append rather than delete and re-upload. 
- Rather than running the python code periodically, the main program could be calling the loaddata function in a do-while loop to be continuously monitoring/polling for new data files (kaggle, wiki, and ratings).
