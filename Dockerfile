FROM python:3.8
RUN apt-get update

RUN mkdir /workdir
WORKDIR /workdir

COPY src/requirements.txt ./

RUN apt-get update
RUN pip install -r requirements.txt
#RUN pip3 install Pillow

COPY src/ ./

EXPOSE 8050
CMD ["python", "Dash/app3.py"]