# **Implementación de un Lakehouse en AWS**

## **Integrantes**
- Andres Guerra
- Tomás Duque
- Juan Felipe Ortiz

## **Introducción**

Este proyecto se centra en la implementación de un **Lakehouse** en **Amazon Web Services (AWS)** utilizando datasets obtenidos de **Kaggle**, que contienen información sobre:

- Las emisiones de **CO2** de diferentes países a lo largo de los años.
- Las emisiones generadas por la producción de **alimentos**.

El objetivo principal es diseñar e implementar un ecosistema de datos que integre:

- Un **Data Lake** en **S3** para almacenamiento escalable.
- Un **Data Warehouse** en **Redshift** para consultas avanzadas.
- El procesamiento de grandes volúmenes de datos mediante **Hadoop** y **Spark** en **AWS EMR**.

Este ecosistema permitirá la ejecución de **consultas SQL** optimizadas tanto en **Amazon Athena** como en **Redshift Spectrum**, facilitando el análisis detallado y la obtención de insights a partir de los datos.

## **Estructura del Proyecto**

### **1. Notebook de PySpark**
Este repositorio incluye un notebook de **Jupyter** que contiene el código **PySpark** utilizado para procesar y transformar los datos en un clúster de **Amazon EMR**. El notebook permite cargar los datos desde **S3**, realizar análisis exploratorio y ejecutar transformaciones clave.

### **2. Consultas SQL**
El archivo `queries.sql` contiene las consultas SQL ejecutadas en **Amazon Athena** y **Amazon Redshift Spectrum**. Estas consultas permiten analizar los datos transformados y generar reportes sobre las emisiones de CO2 y su relación con la producción de alimentos.

## **Descripción de los Datos**

Los datasets utilizados en este proyecto provienen de **Kaggle** y contienen:

- Emisiones de **CO2** por país y año.
- Emisiones de **CO2** generadas por la producción de diferentes tipos de alimentos.

Estos datos permiten un análisis detallado de las emisiones a nivel global y sectorial, proporcionando una visión de cómo las actividades humanas impactan el medio ambiente.

## **Contenido del Repositorio**

1. **`notebookpyspark.ipynb`**: Notebook con el análisis y las transformaciones de datos utilizando **PySpark** en un clúster de **Amazon EMR**.
   
2. **`queries.sql`**: Archivo que contiene las consultas SQL ejecutadas en **Athena** y **Redshift Spectrum** para obtener insights clave sobre las emisiones de CO2.

## **Procesos Implementados**

### **1. Data Lake en Amazon S3**
- **Zona Raw**: Almacenamiento de los datasets originales en formato **CSV**.
- **Zona Trusted**: Almacenamiento de los datos transformados en formato **Parquet**, optimizados para consultas.

### **2. Transformación de Datos con PySpark**
El notebook incluye la carga de datos desde **S3**, la limpieza y transformación de los datos, y la conversión de **CSV** a **Parquet** para almacenamiento eficiente.

### **3. Consultas SQL en Amazon Athena y Redshift Spectrum**
- **Athena**: Consultas SQL sobre los datos almacenados en **S3** y catalogados en **AWS Glue**.
- **Redshift Spectrum**: Consultas avanzadas para realizar análisis multidimensionales sobre las emisiones de CO2.

## **Requisitos**

Para ejecutar este proyecto necesitarás:
- Un clúster de **Amazon EMR** con soporte para **Jupyter Notebooks**.
- Acceso a **Amazon S3**, **Glue**, **Athena**, y **Redshift** en **AWS**.

## **Conclusiones**

Este proyecto demuestra cómo implementar un **Lakehouse** en AWS para gestionar grandes volúmenes de datos de manera eficiente. Utilizando tecnologías como **S3**, **EMR**, **Athena**, y **Redshift**, es posible realizar un análisis detallado de los datos de emisiones de CO2 y su impacto a nivel mundial y sectorial.
