1. Select a finance related project such as fraudulent transaction detection, or insurance premium prediction or any other relevant prroject

2. Build data pipeline to create feature store in aws s3 using spark

3. Develop the project  as this [finance complaint] project is build along with small modification as suggested below.
Use petastorm to create tensor of spark dataframe for model training using ANN.

Note: This project is build completly in spark we want you to explore <b>petastorm library</b> to build model to convert your spark dataframe to tensor so that you can train ANN model in pytorch as pytorch will not work with spark data frame in <b>model trainer</b> step.

1. Use Travis CI to build CI/CD pipeline for Deployment in EC2
2. All artifact file should be saved in any cloud storage such S3 bukcet, Azure blob storage, Google cloud storage.
3. Implement detection machenism for Model Drift such as Concept Drift and Target Drift.
4. Trigger mail along with failure reason if pipeline fails.

Optional: Use AWS cloud watch to maintain log of your application.