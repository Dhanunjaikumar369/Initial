import pandas as pd
import tables 
import numpy as np
from azure.storage.blob import BlockBlobService
import io 


# ## Establishing the Connection options
STORAGEACCOUNTNAME= ''
STORAGEACCOUNTKEY= ''
CONTAINERNAME= 'refinedzone'
BLOBNAME= 'Master/Sites/sites-customer/'
DEST_CONTAINERNAME= 'purposedzone'
DEST_BLOBNAME = '/Sites/sites-customer/'


# ## Creating a function to split the data into equal proportions

def split_dataframe(df, size):
    # size of each row
    #print(df.memory_usage().sum(),"size of df")
    row_size = np.ceil(df.memory_usage().sum() / len(df)) + 1
    #print(row_size, "row-size")

    # maximum number of rows of each segment
    if int(size // row_size) == 0:
        row_limit =1
    else:   
        row_limit = int(size // row_size)
    
    #print(row_limit)
 
    # number of segments
    seg_num = int((len(df) + row_limit - 1) // row_limit)
    #print(seg_num)

    # split df
    segments = [df.iloc[i*row_limit : (i+1)*row_limit] for i in range(seg_num)]

    return segments


# ## Establishing the right Container and Blob location to read from and to dump into

def blob_file_splitting(CONTAINERNAME,BLOBNAME,DEST_CONTAINERNAME,DEST_BLOBNAME):
    
    # Creating a connection to a blob location to read / write data
    blobService = BlockBlobService(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY)

    # Reading a list of files in a bolb location of a container
    blobs = blobService.list_blobs(CONTAINERNAME,prefix=BLOBNAME, delimiter="")

    #print(blobs)

    # Reading all the files from the blobs
    for blob in blobs:
        print(blob.name.split("/")[-1])
        BLOBNAME=blob.name
        LOCALFILENAME=blob.name.split("/")[-1]
        blobService.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
        dataframe_blobdata = pd.read_csv(LOCALFILENAME)
        #print(dataframe_blobdata.head())

        # Conditionally splitting a file if its memory > 70 MB (~~45 MB in the location) for a df
        if(dataframe_blobdata.memory_usage().sum()>72000000):
            df=split_dataframe(dataframe_blobdata,72000000)
            for i in range(0,len(df)):
                output = io.StringIO() 
                output = df[i].to_csv (index_label = "idx", encoding = "utf-8")
                #print(i)
                blobService.create_blob_from_text(DEST_CONTAINERNAME, DEST_BLOBNAME + LOCALFILENAME.strip(".csv")+str(i+1)+'.csv', output)
                del output
        else :
            df=dataframe_blobdata
            output = io.StringIO() 
            output = df.to_csv (index_label = "idx", encoding = "utf-8")
            blobService.create_blob_from_text(DEST_CONTAINERNAME, DEST_BLOBNAME + LOCALFILENAME.strip(".csv")+'.csv', output)
            del output

#Executing the function
blob_file_splitting(CONTAINERNAME,BLOBNAME,DEST_CONTAINERNAME,DEST_BLOBNAME)

