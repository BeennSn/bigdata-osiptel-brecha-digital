## 1. Requisitos del host

| Recurso | Recomendado | Mínimo |
|---|---|---|
| RAM | 16 GB (VM de 8 GB) | 8 GB (VM de 4 GB) |
| CPU | 4 núcleos o más | 4 núcleos |
| Disco libre | 100 GB | 100 GB |
| Virtualización | Soporte por hardware habilitado | Soporte por hardware habilitado |
| Software | VirtualBox 7.x | VirtualBox 7.x |

Perfiles de memoria según la RAM de la VM (se usan en los pasos 3.4 y 3.5):

| RAM de la VM | `YARN_MEM` (MB) | `DRIVER_MEM` | `EXEC_MEM` | `EXEC_INSTANCES` |
|---|---|---|---|---|
| 4 GB | 2560 | 512m | 768m | 1 |
| 8 GB | 5120 | 1g | 1g | 2 |
| 16 GB | 10240 | 2g | 2g | 3 |

---

## 2. Máquina virtual

### 2.1 Especificación

| Parámetro | Valor |
|---|---|
| Hipervisor | VirtualBox 7.x |
| Sistema operativo | Debian 12 (Bookworm), ISO `netinst`, misma arquitectura que el host (`arm64` o `amd64`) |
| RAM | 8192 MB (mínimo 4096 MB) |
| CPU | 4 vCPU |
| Firmware | EFI activado |
| Disco | VDI de 100 GB, asignación dinámica |
| Particionado | EFI + `/` (ext4) + swap |
| Swap | 2 a 4 GB en total |
| Red | NAT con reenvío de puertos (no usar adaptador puente) |
| Puertos | 9870 (HDFS), 8088 (YARN), 8888 (JupyterLab) |
| Software en la instalación | Solo "SSH server" y "utilidades estándar del sistema" |
| Usuario | No root, con `sudo` |
| Hostname | Minúsculas, números y guiones (sin `_`) |
| Zona horaria | America/Lima |

Topología: un solo nodo (pseudo-distribuido), replicación de HDFS igual a 1.

### 2.2 Creación

1. Descargar la ISO `netinst` de Debian 12 con la arquitectura del host.
2. En VirtualBox, crear la VM: tipo Linux, versión Debian (64-bit). Omitir la instalación desatendida. Asignar RAM y CPU, activar EFI y crear el disco VDI dinámico.
3. En Configuración → Almacenamiento, verificar que el disco `.vdi` y la ISO estén conectados al controlador.
4. Con la VM apagada, reenviar los puertos:

```bash
VBoxManage modifyvm "<nombre-vm>" \
  --natpf1 "hdfs,tcp,127.0.0.1,9870,,9870" \
  --natpf1 "yarn,tcp,127.0.0.1,8088,,8088" \
  --natpf1 "jupyter,tcp,127.0.0.1,8888,,8888"
```

Opcional, para entrar por SSH desde el host: `--natpf1 "ssh,tcp,127.0.0.1,2222,,22"` y `ssh -p 2222 <usuario>@127.0.0.1`.

### 2.3 Instalación de Debian

| Decisión | Valor |
|---|---|
| Modo | Instalación en texto |
| Particionado | Guiado, todo el disco, todos los ficheros en una partición |
| Réplica de red | `deb.debian.org` |
| Selección de software | Solo "SSH server" y "utilidades estándar del sistema" |
| Usuario | No root; el nombre de usuario debe coincidir con el definido en el instalador |

### 2.4 Configuración posterior

```bash
# Si sudo no existe (ocurre al definir contraseña de root)
su -
apt install -y sudo
usermod -aG sudo <usuario>
exit        # cerrar sesión y volver a entrar

# Zona horaria
sudo timedatectl set-timezone America/Lima

# Swap adicional de 2 GB
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# apt no debe usar el medio de instalación
sudo sed -i '/^deb cdrom:/s/^/# /' /etc/apt/sources.list
sudo apt update
```

---

## 3. Instalación del entorno

Ejecutar en orden, con el usuario no root.

### 3.1 Paquetes base

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y wget curl gnupg git rsync ssh python3-venv python3-pip \
  poppler-utils tesseract-ocr tesseract-ocr-spa
```

### 3.2 Java 11 (Temurin)

`default-jdk` instala Java 17 en Debian 12 y `openjdk-11-jdk` no está en sus repositorios. Hadoop 3.3.6 solo soporta Java 8 y 11, por eso se usa el repositorio de Adoptium.

```bash
wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public \
  | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null
echo "deb https://packages.adoptium.net/artifactory/deb $(. /etc/os-release && echo $VERSION_CODENAME) main" \
  | sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update && sudo apt install -y temurin-11-jdk
java -version
```

### 3.3 Directorios y SSH local

Los scripts de arranque de Hadoop se conectan por SSH a `localhost`, incluso en un solo nodo.

```bash
sudo mkdir -p /data/docs /data/hdfs/nn /data/hdfs/dn
sudo chown -R $USER: /data

[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost 0.0.0.0 127.0.0.1 >> ~/.ssh/known_hosts
ssh localhost exit      # no debe pedir contraseña
```

### 3.4 Hadoop 3.3.6 (HDFS + YARN)

```bash
HADOOP_VER=3.3.6
wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz
sudo tar -xzf hadoop-$HADOOP_VER.tar.gz -C /opt
sudo ln -sfn /opt/hadoop-$HADOOP_VER /opt/hadoop
sudo chown -R $USER: /opt/hadoop-$HADOOP_VER
```

Variables de entorno (`JAVA_HOME` se calcula, no depende de la arquitectura):

```bash
cat >> ~/.bashrc <<'EOF'

# Big Data
export JAVA_HOME=$(ls -d /usr/lib/jvm/temurin-11-jdk-* | head -n1)
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=$HOME/venv-bd/bin/python
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin
EOF
source ~/.bashrc
echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh
```

Configuración (fijar `YARN_MEM` según la tabla de perfiles de la sección 1):

```bash
YARN_MEM=5120

cat > $HADOOP_CONF_DIR/core-site.xml <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/hdfs-site.xml <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///data/hdfs/nn</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///data/hdfs/dn</value></property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/mapred-site.xml <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/yarn-site.xml <<EOF
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>$YARN_MEM</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>$YARN_MEM</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>4</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
</configuration>
EOF
```

Formatear el NameNode (solo la primera vez) y arrancar los servicios:

```bash
[ -d /data/hdfs/nn/current ] || hdfs namenode -format -nonInteractive
start-dfs.sh
start-yarn.sh
jps
```

### 3.5 Spark 3.5.x

Spark 4.x exige Java 17 o superior, incompatible con Hadoop 3.3.6. `pyspark` (paso 3.6) debe tener exactamente la misma versión que Spark.

```bash
SPARK_VER=3.5.1
wget https://archive.apache.org/dist/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3.tgz
sudo tar -xzf spark-$SPARK_VER-bin-hadoop3.tgz -C /opt
sudo ln -sfn /opt/spark-$SPARK_VER-bin-hadoop3 /opt/spark
sudo chown -R $USER: /opt/spark-$SPARK_VER-bin-hadoop3
```

Configuración (fijar los tres valores según la tabla de perfiles):

```bash
DRIVER_MEM=1g
EXEC_MEM=1g
EXEC_INSTANCES=2

cat > $SPARK_HOME/conf/spark-defaults.conf <<EOF
spark.master                yarn
spark.submit.deployMode     client
spark.driver.memory         $DRIVER_MEM
spark.executor.memory       $EXEC_MEM
spark.executor.instances    $EXEC_INSTANCES
spark.serializer            org.apache.spark.serializer.KryoSerializer
EOF
```

`spark.sql.shuffle.partitions` se deja en su valor por defecto (200) y se ajusta en la etapa de optimización.

### 3.6 Python y JupyterLab

Debian 12 no permite `pip install` global; se usa un entorno virtual.

```bash
python3 -m venv ~/venv-bd
source ~/venv-bd/bin/activate
pip install --upgrade pip
pip install jupyterlab pyspark==$SPARK_VER pandas pyarrow matplotlib \
  pdfplumber pypdf pymupdf pytesseract
```
# 4. Actualizar ~/.bashrc con las rutas definitivas de Spark y PySpark
cat >> ~/.bashrc <<'EOF'

# Spark & Python Environment
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=$HOME/venv-bd/bin/python
export PATH=$PATH:$SPARK_HOME/bin
EOF

source ~/.bashrc

## Iniciar JupyterLab (el token aparece en la consola):

```bash
source ~/venv-bd/bin/activate
jupyter lab --no-browser --ip=0.0.0.0 --port=8888
```

### 3.7 Operación de los servicios

```bash
start-dfs.sh && start-yarn.sh      # encender
stop-yarn.sh && stop-dfs.sh        # apagar
```

---

## 4. Verificación

| Prueba | Resultado esperado |
|---|---|
| `java -version` | Temurin 11 |
| `hadoop version` | 3.3.6 |
| `spark-submit --version` | La versión de `SPARK_VER` |
| `jps` | NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager |
| `hdfs dfs -mkdir -p /datalake/raw && hdfs dfs -ls /datalake` | Lista sin error |
| `spark-submit --class org.apache.spark.examples.SparkPi $SPARK_HOME/examples/jars/spark-examples_*.jar 100` | Termina e imprime `Pi is roughly 3.14...` |
| `localhost:9870`, `localhost:8088`, `localhost:8888` en el navegador del host | Cargan HDFS, YARN y JupyterLab |
| Celda en Jupyter: `from pyspark.sql import SparkSession; s=SparkSession.builder.getOrCreate(); print(s.version, s.range(10**6).count())` | Versión de Spark y `1000000`; la aplicación aparece en `localhost:8088` |

El aviso `Unable to load native-hadoop library` es esperable y no afecta al funcionamiento.

---

## 5. Ejecución del pipeline

Pendiente de completar: ingesta y manifest (`ingesta/`), procesamiento a Silver, características y modelo (`notebooks/`).

---

## 6. Estructura del repositorio

```
├── README.md
├── ingesta/      # scripts de carga y manifest
├── notebooks/    # procesamiento, exploratorio y modelo
└── conf/         # configuración de Hadoop y Spark
```