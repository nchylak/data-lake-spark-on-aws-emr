# Song Play ETL Project

## Summary

This project consists in using Spark on AWS EMR for:

1. **Extracting** the below the two datasets from S3

   * song data from a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/)

   * user activity logs from a music streaming app

2. **Transforming** them to fit a star schema
3. **Loading** the tables of that star schema back on S3.



The desired star schema is as follows:

![ERD](sparkifydb_erd.png)

##### Fact Table

1. songplays
   * records in log data associated with song plays i.e. records with page `NextSong`
     * *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

##### Dimension Tables

1. times
   - timestamps of records in **songplays** broken down into specific units
     - *start_time, hour, day, week, month, year, weekday*
2. users
   * users in the app
     * *user_id, first_name, last_name, gender, level*
3. songs
   * songs in music database
     * *song_id, title, artist_id, year, duration*
4. artists
   * artists in music database
     * *artist_id, name, location, latitude, longitude*

## Files description

1. `etl.py` reads and processes the datasets from S3 to fit the desired star schema (see above), whose tables should be saved back into S3.
2. `test.ipynb` is for testing purposes.

## How to run this project

#### Create an EMR cluster

```bash
aws emr create-cluster \
--name "udacity_cluster" \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark  \
--ec2-attributes KeyName=udacity_kp,SubnetId=subnet-02cc9819c677085ef \
--instance-type m5.xlarge \
--profile default \
--region us-west-2
```

#### Modify the master node's security group

You need to modify the inbound rules of the master node's security group in order to allow for SSH traffic from your computer.

1. Find the security group of the master mode (`EmrManagedMasterSecurityGroup`)

   ```bash
   aws emr describe-cluster --region us-west-2 --cluster-id j-XXXXXXXX
   ```

2. Edit its inbound rules to authorize SSH traffic (port 22) from your computer

   ```bash
   aws ec2 authorize-security-group-ingress \
   --group-id sg-XXXXXXXXXXXXX \
   --protocol tcp \
   --port 22 \
   --cidr XX.XXX.XX.XX/32 \
   --region us-west-2
   ```

#### Find the cluster's public DNS (`PublicDnsName`)

```bash
aws emr list-instances --cluster-id j-XXXXXXXX --region us-west-2
```

#### Copy  `etl.py` and `dl.config` (see below) to the cluster

Replace `ec2-XX-XXX-XX-XXX.us-west-2.compute.amazonaws.com` with the public DNS.

```bash
sudo scp -i ~/.ssh/udacity_kp.pem path_to_script/etl.py hadoop@ec2-XX-XXX-XX-XXX.us-west-2.compute.amazonaws.com:/home/hadoop

sudo scp -i ~/.ssh/udacity_kp.pem path_to_script/dl.config hadoop@ec2-XX-XXX-XX-XXX.us-west-2.compute.amazonaws.com:/home/hadoop
```

#### Create a local tunnel to the cluster

Replace `ec2-XX-XXX-XX-XXX.us-west-2.compute.amazonaws.com` with the public DNS.

```bash
sudo ssh -i ~/.ssh/udacity_kp.pem hadoop@ec2-XX-XXX-XX-XXX.us-west-2.compute.amazonaws.com
```

#### Run the `etl.py` script

1. Make sure you have a configuration file `dl.config` ready with your AWS credentials and the desired configuration of your cluster. Mine looks like the below:

   ```
   [AWS]
   key = paste_your_key_here
   secret = paste_your_secret_key_here
   ```

2. Install configparser on the cluster (in my case the module could not be found)

   ```bash
   sudo pip install configparser
   ```

3. Run `etl.py`

   ```bash
   spark-submit /home/hadoop/etl.py
   ```

4. After the script has run (it takes a while), you can start a pyspark notebook from AWS EMR (from the console) and execute `test.ipynb` for testing purposes.

5. When you are done working on the cluster, terminate it in order to avoid unnecessary costs.