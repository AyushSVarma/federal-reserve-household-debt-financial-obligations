import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool
from datetime import date, datetime

def data_to_s3(data):

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	today = date.today().strftime('%m/%d/%Y')

	source_url_start = 'https://www.federalreserve.gov/datadownload/Output.aspx?rel=FOR&series=5c8df3fd05e5b5ad4297328218040855&lastobs=&from=01/01/1980&to={}&filetype='.format(
		today)
	source_url_end = '&label=include&layout=seriescolumn'

	try:
		response = urlopen(source_url_start + data['url_middle'] + source_url_end)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, data)

	except URLError as e:
		raise Exception('URLError: ', e.reason, data)

	else:
		data_set_name = os.environ['DATA_SET_NAME']
		filename = data_set_name + data['format']
		file_location = '/tmp/' + data_set_name + data['format']

		with open(file_location, 'wb') as f:
			f.write(response.read())

		# variables/resources used to upload to s3
		s3_bucket = os.environ['S3_BUCKET']
		new_s3_key = data_set_name + '/dataset/' + filename
		s3 = boto3.client('s3')

		s3.upload_file(file_location, s3_bucket, new_s3_key)

		print('Uploaded: ' + filename)

		# deletes to preserve limited space in aws lamdba
		os.remove(file_location)

		# dicts to be used to add assets to the dataset revision
		return {'Bucket': s3_bucket, 'Key': new_s3_key}

def source_dataset():

	# list of enpoints to be used to access data included with product
	endpoints = [
		{'url_middle': 'csv', 'format': '.csv'},
		{'url_middle': 'spreadsheetml', 'format': '.xls'},
		{'url_middle': 'sdmx', 'format': '.xml'},
	]

	# multithreading speed up accessing data, making lambda run quicker
	with (Pool(3)) as p:
		asset_list = p.map(data_to_s3, endpoints)

	# asset_list is returned to be used in lamdba_handler function
	return asset_list