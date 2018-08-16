FROM python:2.7.12
ARG VERSION_TAG
RUN git clone -b $VERSION_TAG https://github.com/DuoSoftware/DVP-ETL.git /usr/local/src/dvpetl
RUN cd /usr/local/src/dvpetl
WORKDIR /usr/local/src/dvpetl
RUN pip install -r /usr/local/src/dvpetl/Requirement.txt
CMD [ "python", "ETL.py" ]