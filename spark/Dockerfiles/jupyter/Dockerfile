FROM bitnami/spark:3.4.2

USER root

# ✅ تحديث النظام وتثبيت Python و Jupyter والمكتبات المطلوبة لـ Spark
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    pip3 install --no-cache-dir \
        notebook \
        ipywidgets \
        findspark \
        traitlets \
        py4j      \
        delta-spark==2.4.0

# ✅ إنشاء المستخدم spark بـ UID 1000 إذا لم يكن موجود
RUN id -u 1000 || useradd -m -u 1000 spark

# ✅ إنشاء مجلدات العمل والصلاحيات اللازمة
RUN mkdir -p /opt/notebooks /opt/bitnami/spark/tmp /opt/bitnami/spark/conf && \
    chown -R spark:spark /opt/notebooks /opt/bitnami/spark/tmp /opt/bitnami/spark/conf && \
    chmod -R u+rwX /opt/notebooks /opt/bitnami/spark/tmp /opt/bitnami/spark/conf

# ✅ إعداد مجلدات Jupyter داخل /opt/notebooks
ENV JUPYTER_RUNTIME_DIR=/opt/notebooks/.jupyter/runtime
ENV JUPYTER_DATA_DIR=/opt/notebooks/.jupyter/data
ENV JUPYTER_CONFIG_DIR=/opt/notebooks/.jupyter/config

RUN mkdir -p $JUPYTER_RUNTIME_DIR $JUPYTER_DATA_DIR $JUPYTER_CONFIG_DIR && \
    chown -R spark:spark /opt/notebooks/.jupyter

# ✅ إعداد متغيرات البيئة الخاصة بـ PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# ✅ التحويل للمستخدم spark
USER spark

WORKDIR /opt/notebooks

# ✅ تشغيل Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", \
     "--NotebookApp.token=", "--NotebookApp.password=", \
     "--NotebookApp.allow_origin=*", \
     "--NotebookApp.disable_check_xsrf=True"]
