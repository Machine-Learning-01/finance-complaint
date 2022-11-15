FROM openjdk:20-slim-buster
#RUN yum -y update && yum -y install yum-utils && yum -y groupinstall development && yum -y install awscli  && yum list python3*  && yum -y install python3 python3-dev python3-pip python3-virtualenv
RUN apt update -y && apt-get install python3-pip python3-setuptools python3.7-venv -y
#RUN python -V
RUN python3 -V
ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3
RUN pip3 install --upgrade pip
RUN mkdir /app
COPY . /app/
# RUN mkdir /app/finance_complaint/
# COPY ./finance_complaint /app/finance_complaint/
# COPY ./requirements.txt /app
# COPY ./setup.py /app
# COPY ./main.py /app
# COPY ./start.sh /app
WORKDIR /app/

ENV AIRFLOW_HOME="airflow"
# RUN python3 -m venv venv
# RUN source  venv/bin/activate
RUN pip3 install apache-airflow
RUN airflow db init 
RUN airflow users create  -e avnish@ineuron.ai -f Avnish -l Yadav -p admin -r Admin  -u admin
RUN pip3 install -r requirements.txt
RUN chmod 777 /app/start.sh
CMD ["/app/start.sh"]