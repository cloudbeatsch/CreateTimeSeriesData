import csv
import time
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('inputCSV', help ='path to input csv file')
parser.add_argument('outputCSV', help= 'path to output csv file')
parser.add_argument('threshold', help='threshold of positive event', type=float)
parser.add_argument('trigger_window_size', \
	help='trigger window size in seconds', type=int)
parser.add_argument('datapoints', help='nr of latest datapoints', type=int)
parser.add_argument('slots', help='nr of slots in time series', type=int)
parser.add_argument('slot_size', help='slot size in seconds', type=int)

parser.add_argument('-i', '--id',  \
	help='column name for entity id', default='ID')
parser.add_argument('-t', '--timestamp', \
	help='column name for timestamp', default='TimeStamp')
parser.add_argument('-v', '--value',  \
	help='column name for value', default='Value')
parser.add_argument('-a', '--additional_cols', nargs='*',
	help='additional columns to be added to the output dataset')
args = parser.parse_args()

# first we read all entries from the input file and create a dictionary
# with ID as the key
entries = {}
id_col_name = args.id
timestamp_col_name = args.timestamp
value_col_name = args.value
additional_cols = args.additional_cols
if additional_cols is None :
	additional_cols = {}
with open(args.inputCSV, 'rb') as csvfile:
	data_reader = csv.DictReader(csvfile, delimiter =';')
	errors = 0
	for row in data_reader :
		timestamp = row[timestamp_col_name]
		if not entries.has_key(row[id_col_name]) :
			entries[row[id_col_name]] = []
		try :
			if timestamp.find('.') :
				timestamp = timestamp.split('.')[0]
			timeTuple = time.strptime(timestamp, '%Y-%m-%d  %H:%M:%S')
		except:
			print 'can not parse TimeStamp: ', timestamp
			errors += 1
		else:
			try:
				# the value of the dictionary is a list containing of 
				# time, ID, the measurement value, 
				# and optionally additional columns 
				entry = [timeTuple, row[id_col_name], \
					float(row[value_col_name].replace(',', '.'))]
				for additional_col in additional_cols :
					entry.append(row[additional_col])
				entries[row[id_col_name]].append(entry)

			except ValueError,e:
				print "error",e,"data: ",row[value_col_name]
print 'Nr of parsing errors: ', errors

with open(args.outputCSV, 'wb') as csvfile:
	fieldnames = [id_col_name, 'Seq', 'IsTriggered', value_col_name, \
		'NrOfTotalDatapoints']

	nr_of_slots = args.slots
	slot_size = args.slot_size
	nr_of_last_datapoints = args.datapoints
	trigger_threshold = args.threshold
	trigger_window_size = args.trigger_window_size

	# construct the fieldnames for the last datapoints
	last_time_span_name = 'TimeSpan-Last'+ str(nr_of_last_datapoints)
	fieldnames.append(last_time_span_name)
	last_high_value_name = 'HighValue-Last' + str(nr_of_last_datapoints)
	fieldnames.append(last_high_value_name)
	last_avarage_value_name = 'AvarageValue-Last' + str(nr_of_last_datapoints)
	fieldnames.append(last_avarage_value_name)
	last_low_value_name = 'LowValue-Last' + str(nr_of_last_datapoints)
	fieldnames.append(last_low_value_name)

	# construct the fieldnames for each day in the timeseries
	for s in range(1, nr_of_slots+1) :
		fieldnames.append('HighValue-' + str(s))
		fieldnames.append('NrOfDatapoints-' + str(s))
		fieldnames.append('AvarageValue-' + str(s))
		fieldnames.append('LowValue-' + str(s))
	
	#construct the filenames for the additional columns
	for additional_col in additional_cols :
		fieldnames.append(additional_col)
	
	time_series_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, \
		delimiter=',')
	time_series_writer.writeheader()
	# interating through each ID
	for entry in entries.values() :
		# reset the sequence number
		sequence = 1
		# sort the entries based on time (each value is a tuple of time and
		# list)
		entry.sort()
		# iterate over all entries starting with nr_of_last_datapoints to 
		# ensure we can provide the last datapoints
		for entry_index in range(nr_of_last_datapoints, len(entry)) :
			entries = { }
			entries[id_col_name] = entry[entry_index][1]
			entries['Seq'] = sequence
			entries[value_col_name] = entry[entry_index][2]
			list_index = 3
			for additional_col in additional_cols :
				entries[additional_col] = entry[entry_index][list_index] 
				list_index += 1
			is_triggered = 1

			cursor = entry_index
			# loop over all entries within the next hours (specified by  
			# trigger_window) and check if a value is below the treshold
			while cursor < len(entry) : 
				elapsed_time = time.mktime(entry[cursor][0]) - \
					time.mktime(entry[entry_index][0])
				if (elapsed_time < trigger_window_size) :
					if (entry[cursor][2]) < trigger_threshold :
						is_triggered = 2
						break
					cursor += 1
				else :
					break
			entries['IsTriggered'] = is_triggered

			# write the time span of the last datapoints in minutes
			entries[last_time_span_name] = \
				int((time.mktime(entry[entry_index][0]) - \
					time.mktime(entry[entry_index-nr_of_last_datapoints][0])) \
						// 60)
			# reset high value to its lowest possible value
			last_high_value = sys.float_info.min
			# reset low value to its highest possible value
			last_low_value = sys.float_info.max
			last_value_total = 0
			for l in range(1, nr_of_last_datapoints+1) :
				last_value_total += entry[entry_index-l][2]
				if (entry[entry_index-l][2]) < last_low_value :
					last_low_value = entry[entry_index-l][2]
				if (entry[entry_index-l][2]) > last_high_value :
					last_high_value = entry[entry_index-l][2]	
			# write the most recent datapoints 
			entries[last_high_value_name] = last_high_value
			entries[last_avarage_value_name] = \
				last_value_total / nr_of_last_datapoints
			entries[last_low_value_name] = last_low_value
			
			# now creating the daily entries
			valid_entries = 0
			# set the iteration cursor and start with the previous measurment
			cursor = entry_index - 1
			total_datapoints = 0
			for slot in range(1, (nr_of_slots+1)) :
				# reset high value to its lowest possible value
				high_value = sys.float_info.min
				# reset low value to its highest possible value
				low_value = sys.float_info.max
				nr_of_values = 0
				value_total = 0
				# get the highest and lowest value in each 24h slot
				while cursor >= 0 :
					elapsed_time = time.mktime(entry[entry_index][0]) - \
						time.mktime(entry[cursor][0])
					# check if we're still within the current slot
					if (elapsed_time < (slot_size * slot)) \
						and (elapsed_time > (slot_size * (slot - 1))) :
						if (entry[cursor][2]) < low_value :
							low_value = entry[cursor][2]
						if (entry[cursor][2]) > high_value :
							high_value = entry[cursor][2]
						value_total += entry[cursor][2]
						nr_of_values += 1
						# move one entry back in time
						cursor -= 1
					else :
						break
				slot_string = str(slot)
				# only add the entry if we got high and low values
				if (high_value > sys.float_info.min) and \
					(low_value < sys.float_info.max) :
						entries['HighValue-'+slot_string] = high_value
						entries['LowValue-'+slot_string] = low_value
						entries['NrOfDatapoints-'+slot_string] = nr_of_values
						entries['AvarageValue-'+slot_string] = \
							value_total / nr_of_values
						total_datapoints += nr_of_values
						valid_entries += 1
				else :
					break
			# only write the datapoints if all slot entries have values
			if valid_entries ==  nr_of_slots :
				entries['NrOfTotalDatapoints'] = total_datapoints
				time_series_writer.writerow(entries)
				sequence += 1
