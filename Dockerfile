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
RUN pip3 install numpy pandas
RUN mkdir app
COPY dist/ app/
COPY main.py /app
RUN pip3 install  /app/finance_complaint-0.0.4-py3-none-any.whl > temp.txt

CMD ["bash"]
