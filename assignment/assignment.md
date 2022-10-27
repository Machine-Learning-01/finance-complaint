1. Select a finance related project such as fraudulent transaction detection, or insurance premium prediction or any other relevant prroject

2. Build data pipeline to create feature store in aws s3 using spark

3. Develop the project  as this [finance complaint] project is build with additional modification as suggested.
Use petastorm to create tensor of spark dataframe for model training using ANN.

Note: This project is build completly in spark we want you to explore <b>petastorm library</b> to build model to convert your spark dataframe to tensor so that you can train ANN model in pytorch as pytorch will not work with spark data frame in <b>model trainer</b> step.

4. Use Travis CI to build CI/CD pipeline for Deployment in EC2
5. All artifact file should be saved in any cloud storage such S3 bukcet, Azure blob storage, Google cloud storage.


Optional: Use AWS cloud watch to log your application