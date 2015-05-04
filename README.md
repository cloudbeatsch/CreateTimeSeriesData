# CreateTimeSeriesData
Transform a csv file which contains a time series of values into a format which can be used for pattern recognition and machine learning.

**CreateTimeSeriesData.py**  provides the following inputs to control the generation of the time series dataset:
    
    usage: CreateTimeSeriesData.py [-h] [-i ID] [-t TIMESTAMP] [-v VALUE]
                                   [-a [ADDITIONAL_COLS [ADDITIONAL_COLS ...]]]
                                   inputCSV outputCSV threshold
                                   trigger_window_size datapoints slots slot_size
    
    positional arguments:
      inputCSV              path to input csv file
      outputCSV             path to output csv file
      threshold             threshold of positive event
      trigger_window_size   trigger window size in seconds
      datapoints            nr of latest datapoints
      slots                 nr of slots in time series
      slot_size             slot size in seconds

The core concepts of the script are **trigger window**, **last data points** and **slots**:

•	**triggerwindowsize** defines how many seconds we search forward for finding values which are below the threshold value. If we found a value which is below the threshold, we set the value of the output column IsTriggered to 2, in all other cases we set it to 1. Note, *IsTriggered* will become our label for training the machine learning algorithm.

•	**datapoints** defines how many of the latest data points will be added to the output dataset. 

•	**slots** defines the number of slots we aggregate and add to the output dataset. The length of a slot is defined by the slote_size in seconds.

#Example
The idea is to use a series of session data to identify churn patterns. We define a customer at risk if the SessionDuration is below 5 minutes per session. 

    python CreateTimeSeriesData.py SessionData.csv TSDataset.csv 40 86400 3 7 86400 --id=CustomerId --value=SessionDuration  -a SubscriptionType Age

•	SessionData.csv and TSDataset.csv filenames  

•	a threshold of 5 with a trigger window size of 24 hours (86400 seconds)

•	adding the 3 last data points

•	adding 7 slots of 24 hours each

•	using the column called CustomerId as the entity key 

•	use the column called SessionDuration as the value

•	add two additional columns (SubscriptionType and Age) from the input dataset to the output dataset

