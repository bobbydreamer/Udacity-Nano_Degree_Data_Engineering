# Create new AWS user

Go to AWS IAM -> Click Add User and follow below steps to create new user

Type in user name & CHECK "Programmatic access" and Click NEXT
![alt text](./images/IAM-1.PNG "")

Select "Attach existing policies directly", find and SELECT "AmazonRedshiftFullAccess" 
![alt text](./images/IAM-2.PNG "")

then find and SELECT "AmazonS3ReadOnlyAccess" and Click NEXT
![alt text](./images/IAM-3.PNG "")

Nothing here. Click NEXT
![alt text](./images/IAM-4.PNG "")

Review the selections. Click NEXT
![alt text](./images/IAM-5.PNG "")

Save the access key ID & Secret Access key.
Recommendation : Better to copy key from here and save it in a file rather than download.
![alt text](./images/InkedIAM-6-ClickSHOW_LI.JPG "")
User is created
![alt text](./images/IAM-7.PNG "")
This error occured when creating a role
![alt text](./images/IAM-8.PNG "")
Above error is resolved by adding "Administrator Access" and removing other two permissions
![alt text](./images/IAM-9.PNG "")
![alt text](./images/IAM-10-DeattachedPermissions.PNG "")