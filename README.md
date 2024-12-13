# Apache Spark Environment with Docker

## Before you start using Pyspark:

```
Create a data folder within the pyspark folder.
Upload the txt you want to use into the pyspark folder.
```

## Pyspark

```
cd pyspark
docker build -t pyspark-app .
docker run pyspark-app
```

## Pyspark for Different Versions

```
cd pyspark-version
docker build -t pyspark-version-app .
docker run pyspark-version-app
```

## Jupyter Notebook

```
cd jupyter
docker-compose up --build
or
docker-compose up
```

When the application runs, it will be here [http://127.0.0.1:8000/](http://127.0.0.1:8888/lab). You can start working in the /work folder

## WEB APP

```
cd web-app
docker-compose up --build
or
docker-compose up
```

Once the application runs it will here http://127.0.0.1:8000/

## Web APP -> How to File Upload and View

```
cd web-app
docker-compose up
Go here http://127.0.0.1:8000/
Select file
Click File Upload and View
```

### Create New project

```
docker-compose run web django-admin startproject <project-name> .
docker-compose up
```

### Start App

```
docker-compose run web python manage.py startapp <app_name>
docker-compose up --build
```
