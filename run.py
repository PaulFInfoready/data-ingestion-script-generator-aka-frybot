import sys, os, csv, glob, shutil, boto, boto.s3.connection, boto3, configparser, ssl
from boto.s3.key import Key
##########################################################
# File name: run.py
# Author: Paul Fry
# Date created: 08/08/2017
# Date last modified: N/A
# Python Version: 3.6
##########################################################
# Global Variables
##########################################################
# Read input from config file
config = configparser.ConfigParser()
config.read("config.ini")

crds = config['DEFAULT']['cfg_crds']
AWS_ACCESS_KEY_ID = config['DEFAULT']['cfg_AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY= config['DEFAULT']['cfg_AWS_SECRET_ACCESS_KEY']
bucket_name = config['DEFAULT']['cfg_bucket_name']

##########################################################
# 1. Create initial folder structure
##########################################################
if not os.path.exists('ip'):
	os.makedirs('ip')
	os.makedirs('ip/csv')
	os.makedirs('ip/parquet')
	os.makedirs('ip/txt')

if not os.path.exists('op'):
	os.makedirs('op')
	os.makedirs('op/schema')
	os.makedirs('op/sql')
	os.makedirs('op/staging')
	
if not os.path.exists('op/staging'):	
	os.makedirs('op/staging')

##########################################################
# 2. Remove existing input & output files, if they exist
##########################################################
for root, dirs, files in os.walk('ip/csv'):
	for f in files:
		os.unlink(os.path.join(root, f))
	for d in dirs:
		shutil.rmtree(os.path.join(root, d))	

for root, dirs, files in os.walk('ip/txt'):
	for f in files:
		os.unlink(os.path.join(root, f))
	for d in dirs:
		shutil.rmtree(os.path.join(root, d))		
		
for root, dirs, files in os.walk('ip/parquet'):
	for f in files:
		os.unlink(os.path.join(root, f))
	for d in dirs:
		shutil.rmtree(os.path.join(root, d))

# remove existing output csv files, if they exist
for root, dirs, files in os.walk('op/schema'):
	for f in files:
		os.unlink(os.path.join(root, f))
	for d in dirs:
		shutil.rmtree(os.path.join(root, d))		
		
# remove existing output sql files, if they exist
for root, dirs, files in os.walk('op/sql'):
	for f in files:
		os.unlink(os.path.join(root, f))
	for d in dirs:
		shutil.rmtree(os.path.join(root, d))
		
# remove existing files in the staging folder, if they exist
for root, dirs, files in os.walk('op/staging'):
	for f in files:
		os.unlink(os.path.join(root, f))
	for d in dirs:
		shutil.rmtree(os.path.join(root, d))

##########################################################
# 3. Load the files from the S3 bucket
##########################################################
# resolve issue accessing S3 buckets containing dots
if hasattr(ssl, '_create_unverified_context'):
	ssl._create_default_https_context = ssl._create_unverified_context
   
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)		
bucket = conn.get_bucket(bucket_name)

#keep the commented code below, good odds it may be needed at some point

#firstly, detect any folders that exist in the S3 bucket...and create these locally, to resolve issues trying to download this data locally
#client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#paginator = client.get_paginator('list_objects')
#s3_directories=[]
#for result in paginator.paginate(Bucket=bucket_name, Delimiter='/'):
#	for prefix in result.get('CommonPrefixes'):
#		s3_directories.append(prefix.get('Prefix')[:-1])

#os.chdir('ip')
#for x in s3_directories:
#	if not os.path.exists(x):
#		os.makedirs(x)
#os.chdir('..')

# go through the list of files
bucket_list = bucket.list()
for l in bucket_list:
	keyString = str(l.key)
	
	#don't download files contained within directories
	if keyString.find('/')==-1:
		# check if file exists locally, if not: download it
		if not os.path.exists('ip/'+keyString):
			l.get_contents_to_filename('ip/'+keyString)

#convert any .txt files to .csv
os.chdir('ip')
for file in os.listdir('.'):
	if file.endswith('.txt'):
		#take all .txt filenames & create target .csv equivalents
		op_csv_file = '%s.csv' % os.path.splitext(file)[0]
		#open .txt file, using csv reader (with tab delimiter)
		in_file=open(file, "rt", encoding='utf8')
		in_txt = csv.reader(in_file, delimiter = '\t', quoting=csv.QUOTE_NONE)
		#write the contents of the .txt file, to a new/equivalent .csv file
		out_file=open(op_csv_file, 'w', encoding='utf8', newline='')
		out_csv = csv.writer(out_file)
		out_csv.writerows(in_txt)
		out_file.close()
		in_file.close()
os.chdir('..')

# move the input files into appropriate folders
ip_txt_files=os.path.join(os.getcwd(),'ip\*.txt')
ip_txt_folder=os.path.join(os.getcwd(),'ip\\txt')
ip_csv_files=os.path.join(os.getcwd(),'ip\*.csv')
ip_csv_folder=os.path.join(os.getcwd(),'ip\csv')
ip_parquet_files=os.path.join(os.getcwd(),'ip\*.parquet')
ip_parquet_folder=os.path.join(os.getcwd(),'ip\parquet')

for single_ip_txt_file in glob.glob(ip_txt_files):
    shutil.move(single_ip_txt_file,ip_txt_folder)
for single_ip_csv_file in glob.glob(ip_csv_files):
    shutil.move(single_ip_csv_file,ip_csv_folder)
for single_parquet_file in glob.glob(ip_parquet_files):
    shutil.move(single_parquet_file,ip_parquet_folder)

#############################################################
# 4. Write the .csv files to the op/staging folder
#############################################################
# list all of the files in the input dir & put into a list
iplist = os.listdir('ip/csv')

# get the first line (schema) of all files in the input dir
os.chdir('ip/csv')
for item in iplist:
	infile = open(item, "rb")
	ip_first_line = infile.readline()
	infile.close()
	
	# output the schema to a separate file in the output dir
	os.chdir('..\..\op\staging')
	outfile = open(item,"wb")
	outfile.write(ip_first_line)
	outfile.close()
	
	os.chdir('..\..\ip\csv')
	
os.chdir('..\..')

#############################################################
# 5. Write the .sql files
#############################################################
schemalist = os.listdir('op\staging')
os.chdir('op\staging')
for csv_table in schemalist:
	# for every sql file in the dir
	input_tbl_list = '%s.sql' % os.path.splitext(csv_table)[0]
	#remove the .csv extn
	input_sql_tbl = os.path.splitext(csv_table)[0]
	# replace '-'s from input_sqp_tbl
	input_sql_tbl = input_sql_tbl.replace('-','_')
	# remove #'s from input_sqp_tbl
	input_sql_tbl = ''.join([i for i in input_sql_tbl if not i.isdigit()])
	# remove the final character from the var (it's always an underscore)
	input_sql_tbl = input_sql_tbl[:-1]
	with open(input_tbl_list, 'w+') as sql_output:
		sql_output.write("--DROP TABLE IF EXISTS landing." + input_sql_tbl + ";" + "\nCREATE TABLE IF NOT EXISTS landing." + input_sql_tbl + " (\n")
		
		#get the column names from the schema file
		schema = open(csv_table, "r")
		col_names = schema.readline()
		#remove newline character at end of string
		col_names = col_names[:-1]
		schema.close()

		with open(csv_table) as csvfile:
			
			for row in csv.reader(csvfile, delimiter=','):
				#capture the last column of the CSV file
				#I'd previously needed this - no harm in capturing it
				last_col = int(sum(1 for col in row))-1
				i=0
				for col in row:
					i=i+1
					sql_output.write(col + "\tVARCHAR(100),\n")
		sql_output.write("CREATED_AT_DATE DATETIME NOT NULL DEFAULT SYSDATE\n")
		sql_output.write(");\n\n")
		
		
		# add the COPY statement
		sql_output.write("COPY landing." + input_sql_tbl + " (" + col_names + ") \nFROM 's3://" + bucket_name + "/" + csv_table + "'\n")
		sql_output.write("credentials 'aws_iam_role=" + crds + "'\n")
		sql_output.write("delimiter ',' CSV IGNOREHEADER 1 timeformat 'YYYY-MM-DD HH:MI:SS' region 'ap-southeast-2' trimblanks ACCEPTINVCHARS IGNOREBLANKLINES;")
		
		
schema_files=os.path.join(os.getcwd(),'*.csv')
sql_files=os.path.join(os.getcwd(),'*.sql')
os.chdir('..')
schema_folder=os.path.join(os.getcwd(),'schema')
sql_folder=os.path.join(os.getcwd(),'sql')

# move the sql & csv (schema) files into appropriate folders 
for single_schema_file in glob.glob(schema_files):
    shutil.move(single_schema_file,schema_folder)

for single_sql_file in glob.glob(sql_files):
    shutil.move(single_sql_file,sql_folder)

os.chdir('..')
os.rmdir('op/staging')