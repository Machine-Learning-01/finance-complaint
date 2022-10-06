FROM amazoncorretto:8

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv

RUN python -V
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
RUN python3 -m venv venv
RUN source  venv/bin/activate
RUN pip3 install -r requirements.txt
RUN chmod 777 ./start.sh
CMD ["./start.sh"]