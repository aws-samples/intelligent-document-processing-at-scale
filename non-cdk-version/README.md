# Processing Documents With Textract At Scale

## Getting Started
This repo contains a python script that comes in two flavors. One is in the Notebook format and packaged as a ipynb format and the other is in a simple py format.

The script has been designed to do raw OCR on a large cohort of documents that are contained in a S3 bucket, and do this at scale. By leveraging this script, we can unlock all the content contained in those documents, be it PDF's, TIFFs or other image types and use that information downstream for further analytics or make it available to an LLM for RAG or fine tuning.

In making this script, the goal was to make it simple and minimalistic as possible, using the least amount of infrastructure to achieve the end result. Basically, the script relies on a DynamoDB table to store information on the files that need to have OCR, with the script orchestrating calls to Textract's detect text API.

The easiest way to run this script is from a notebook by using the textractFeeder.ipynb . This has been successfully tested from a ml.t3.medium notebook instance. In addition, the notebook needs to have the appropriate IAM configuration so that code executing inside of EC2 can call Textract and DynamoDB.

The script is designed to read rows from a table that has been prepopulated with the bucket name and object name for documents that need to be sent to Textract. The table schema is as follows and is created from within the Notebook.
```
Table - s3ObjectNames
Partition Key	ObjectName (String)
		bucketName (String)
		createdDate (Decimal)
		outputbucketName (String)
		txJobId (String)
```

## Running the script
In order for the script to run, at the minimum the ObjectName, bucketName and createDate need to be populated with your list of files that need OCR. There are various ways this can be done. On a bucket that has a document count in the several thousand range, there is a notebook cell within the notebook script that can be executed on its own that will populate the DynamoDB. In the regular py script file this function is called fetchAllObjectsInBucketandStoreName. By providing the input bucket name, the function will enumerate over the object names and insert those object keys into the DynamoDB. For a bucket that contains hundreds of thousands of documents, it might be better to actually run an Inventory report from the S3 console. This will schedule a S3 job that will generate a CSV file with all the object names listed within. Once the CSV file is generated, the object names can be extracted and populated into the DynamoDB, not demonstrated here.

To start a run, load the python script into your notebook, and then starting at the top work you way down through the cells, waiting for each to complete. If you choose to run the py formated script, then each function gets called from our _main_ entry point. The first thing you will need to do is specify the following values for the _tracking_table, _input_bucket and _output_bucket. The tracking table name can be any name of your choice The final cell will kick off the process for calling Textract with the information it finds in the DynamoDB.

This project also includes an additional help file that can be ran after all the documents have been sent to Textract. This helper script called convertTextractOutIntoTextHelper will select rows from the dynamoDB and then convert the text found in the Textract JSON output files in the S3 bucket to a .txt file which will be written back to S3, and then will update the row in DynamoDB with the output name for that given file.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

