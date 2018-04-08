import boto3
import csv
import os

def lambda_handler(event, context):
    resource = boto3.resource('s3')
    client = boto3.client('s3')
    path_to_source = 'data/'
    bucket_name = 'bucket_name'
    response = client.list_objects(Bucket=bucket_name, Prefix=path_to_source)
    count = 0 
    tmp_path = '/tmp/'
    local_src_path = tmp_path + 'local_src'
    combined_file = 'combined_file.csv'
    
    ## Remove the combined file if exists
    try:
        os.remove(tmp_path + combined_file)
    except OSError:
        pass
    

    ## open file to write
    with open (tmp_path+combined_file, 'a') as file_writer: 
        combined_file_writer = csv.writer(file_writer)
        print(response)

        for res in response['Contents']:
            name =  res['Key'].rsplit('/',1)
            filename = name[1]
            
            ## s3 source path
            s3_src = path_to_source + filename
            
            ## Create local storage of s3 files
            local_src = tmp_path + 's3-file-' +  filename
            
            ## Download dat from s3 to tmp
            resource.Bucket(bucket_name).download_file(s3_src,local_src)
            
            ## iterate through local source
            with open(local_src, 'r') as file_reader :
                local_file_reader = csv.reader(file_reader)
                for row in local_file_reader:
                        
                    count += 1
                    row.append((count))
                    combined_file_writer.writerow(row)
                    print(row)

                            
    # Upload to s3
    data = open(tmp_path+combined_file, 'rb')
    resource.Bucket(bucket_name).put_object(Key=path_to_source + combined_file, Body = data)
    
    ## Iterate through final file
    with open (tmp_path+combined_file , 'r') as file_reader:
        combined_file_reader = csv.reader(file_reader)
        for row in combined_file_reader:
            print(row)
            
    return 0
