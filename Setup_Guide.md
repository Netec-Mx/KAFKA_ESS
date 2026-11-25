# Setup Guide General

## Apache Kafka para desarrolladores .NET

| Control documental | Información |
|---|---|
| Clave del curso | KAFKA_ESS |
| Versión del documento | 0.1 |
| Estado | BORRADOR |
| Instructor responsable | [NOMBRE] |
| Fecha de elaboración | 2026-11-24 |
| Fecha de última validación | [AAAA-MM-DD] |
| Repositorio de laboratorios | https://github.com/Netec-Mx/KAFKA_ESS |
| Rama y revisión analizada | main, [COMMIT / ETIQUETA] |
| Alcance revisado | 4 capítulos, 6 prácticas (ACT-01 a ACT-06), README principal y este documento |
| Modalidad del entorno | VIRTUALIZADO: una máquina virtual por participante, provista por Netec |

> **Instrucciones para el instructor — eliminar antes de entregar:** completar los campos entre corchetes y los marcados como `[VALIDAR]`. Los valores sin corchetes provienen del temario oficial (sección *Condiciones de implementación*). Los nombres del clúster, namespace, puertos y rutas del repositorio de código son una convención propuesta: confirmarlos contra la máquina virtual y el repositorio de código preparado y, si difieren, actualizarlos aquí y en las prácticas. No registrar secretos. Distinguir requisitos del repositorio, recomendaciones del instructor y comprobaciones realmente ejecutadas.

# Objetivo

Preparar, para cada participante, una máquina virtual con un clúster Kubernetes de un nodo en el que Strimzi 1.2.0 despliega Apache Kafka 4.2.x en modo KRaft (un único nodo que actúa como controlador y broker) y el HTTP Bridge de Strimzi, junto con las herramientas de cliente de Kafka 4.2, Java 21, el SDK de .NET 10 con Confluent.Kafka 2.16, Git, un editor de código y el repositorio con los proyectos del productor y del consumidor .NET y el script del sistema legado. El entorno está listo cuando, desde la terminal de la máquina virtual, la CLI crea y lee un topic, los proyectos .NET compilan y publican y consumen un evento, y el HTTP Bridge responde y publica en un topic.

# Requisitos de Hardware

Requisitos de la máquina virtual del participante (provista por Netec) y del equipo desde el que se accede a ella.

| Recurso | Mínimo requerido | Recomendado | Observaciones |
|---|---|---|---|
| Procesador y arquitectura (VM) | [VALIDAR] 4 vCPU x86_64 | [VALIDAR] 4 vCPU x86_64 | Kubernetes, Kafka, HTTP Bridge, hasta 5 procesos .NET simultáneos (Práctica 4) |
| Memoria RAM (VM) | [VALIDAR] 12 GB | [VALIDAR] 16 GB | El broker de Kafka y la JVM de la CLI comparten memoria con los clientes .NET |
| Espacio libre (VM) | [VALIDAR] 40 GB | [VALIDAR] 60 GB | Imágenes de contenedor de Strimzi y Kafka, SDK de .NET, paquetes NuGet y datos de los topics |
| Pantalla (equipo del participante) | 1366 × 768 | 1920 × 1080 o dos monitores | Las prácticas usan hasta cinco terminales a la vez |
| Conectividad (equipo del participante) | [VALIDAR] 10 Mbps estables | [VALIDAR] 20 Mbps | Acceso remoto a la VM durante 7 horas |

# Sistemas Operativos Compatibles

| Sistema operativo | Edición y versión | Arquitectura | Condición de soporte o validación |
|---|---|---|---|
| Linux (máquina virtual del curso) | [VALIDAR DISTRIBUCIÓN Y VERSIÓN] | x86_64 | Plataforma base del curso; todas las prácticas se ejecutan aquí con bash |
| Equipo del participante | Cualquier sistema con el cliente de acceso remoto indicado por Netec | — | Solo se usa para acceder a la VM; no se instala software del curso |

El curso no se valida en instalaciones locales del participante. Todos los comandos de las prácticas están escritos para bash en la máquina virtual.

# Software Requerido

| Componente | Edición y versión requerida | Arquitectura | Obligatorio u opcional | Fuente oficial |
|---|---|---|---|---|
| Kubernetes de un nodo | [VALIDAR DISTRIBUCIÓN Y VERSIÓN] | x86_64 | Obligatorio | [URL] |
| Strimzi (operador) | 1.2.0 | x86_64 | Obligatorio | https://strimzi.io/docs/operators/1.2.0/deploying |
| Apache Kafka (clúster) | 4.2.x en modo KRaft, versión fijada en el recurso `Kafka` | x86_64 | Obligatorio | https://kafka.apache.org/42/getting-started/quickstart/ |
| HTTP Bridge de Strimzi | Versión compatible con Strimzi 1.2.0 [VALIDAR] | x86_64 | Obligatorio | https://strimzi.io/docs/bridge/latest/ |
| Herramientas de cliente de Kafka (CLI) | 4.2.x | x86_64 | Obligatorio | https://kafka.apache.org/downloads |
| Java (para la CLI) | 21 | x86_64 | Obligatorio | [URL DE LA DISTRIBUCIÓN DE JAVA UTILIZADA] |
| SDK de .NET | 10 | x86_64 | Obligatorio | https://dotnet.microsoft.com/download/dotnet/10.0 |
| Git | [VALIDAR VERSIÓN] | x86_64 | Obligatorio | https://git-scm.com/downloads |
| Editor de código | Visual Studio Code [VALIDAR VERSIÓN] | x86_64 | Obligatorio | https://code.visualstudio.com/ |
| curl | [VALIDAR VERSIÓN] | x86_64 | Obligatorio (Práctica 6) | https://curl.se/ |

# Herramientas y Dependencias

## Paquetes, frameworks y componentes adicionales

| Dependencia | Versión o restricción | Componente que la necesita | Método de instalación o habilitación |
|---|---|---|---|
| Confluent.Kafka | 2.16.x | `Pagos.Producer` y `Pagos.Consumer` | Referencia de paquete en cada `.csproj`; se restaura con `dotnet build` |
| System.Text.Json | Incluido en .NET 10 | `Pagos.Producer` y `Pagos.Consumer` | Incluido en el SDK |
| kubectl | Compatible con la versión de Kubernetes | Comprobaciones del clúster y troubleshooting | Instalado con la distribución de Kubernetes [VALIDAR] |

## Licencias, cuentas y permisos

| Recurso o servicio | Licencia o suscripción | Cuenta o rol requerido | Permiso mínimo | Responsable de habilitación |
|---|---|---|---|---|
| Máquina virtual del curso | Provista por Netec | Usuario de la VM | Usuario con acceso a terminal, `kubectl` de lectura y ejecución de `dotnet` | Netec |
| Repositorio de código preparado | [PÚBLICO / PRIVADO] | [CUENTA, SI APLICA] | Lectura (`git clone` / `git pull`) | Netec |
| Software del curso | Apache Kafka, Strimzi, .NET y Confluent.Kafka son de código abierto | — | — | — |

Los accesos a la VM se entregan por el canal autorizado de Netec. No se registran contraseñas ni tokens en este documento.

# Instalación y Configuración

## 1. Obtener los materiales

- Repositorio de laboratorios (guías): https://github.com/Netec-Mx/KAFKA_ESS
- Repositorio de código preparado: [URL DEL REPOSITORIO CON LOS PROYECTOS .NET Y EL SCRIPT LEGADO]
- Revisión que se utilizará: [COMMIT / ETIQUETA / VERSIÓN].
- Directorio de trabajo: `/home/[USUARIO]/kafka-ess-labs` (`~/kafka-ess-labs`).
- Archivos que deben estar disponibles en el repositorio de código:

```text
~/kafka-ess-labs/
├── src/
│   ├── Pagos.Producer/      # Productor .NET: --cantidad N, --continuo, --invalidos N
│   └── Pagos.Consumer/      # Consumidor .NET: --grupo G, --reset earliest|latest, --retardo-ms N, --salida RUTA
└── legado/
    └── legado-pagos.sh      # Sistema legado: publica N pagos (idPago P-LEG-*) por HTTP en $BRIDGE_URL
```

Los archivos de las carpetas `~/act02`, `~/act05` y `~/entregables` se crean durante el curso.

## 2. Instalar o habilitar los componentes

La máquina virtual se entrega con el entorno desplegado. Los pasos siguientes documentan la preparación que Netec realiza antes del curso.

### Clúster Kafka con Strimzi

1. Instalar el operador Strimzi 1.2.0 en el namespace `kafka`, siguiendo la guía oficial de despliegue.
2. Crear un recurso `KafkaNodePool` con un único nodo con los roles `controller` y `broker`.
3. Crear el recurso `Kafka` `kafka-curso` con `version: 4.2.x` [VALIDAR PARCHE], un listener interno y un listener externo de tipo `nodeport` sin TLS ni autenticación.
4. Crear el recurso `KafkaBridge` `bridge-curso` expuesto mediante un servicio `NodePort`.

**Plataforma y terminal:** Linux, bash.

```bash
kubectl get kafka,kafkanodepool,kafkabridge -n kafka
kubectl get svc -n kafka
```

**Resultado esperado:** el recurso `kafka-curso` con `READY True`, el `KafkaBridge` listo y los servicios `kafka-curso-kafka-external-bootstrap` y del HTTP Bridge de tipo `NodePort`.

### Herramientas de cliente

1. Instalar Java 21.
2. Descomprimir la distribución binaria de Kafka 4.2.x en `/opt/kafka`.
3. Instalar el SDK de .NET 10, Git, Visual Studio Code y curl.
4. Clonar el repositorio de código en `~/kafka-ess-labs` y ejecutar `dotnet build` en ambos proyectos para precargar los paquetes NuGet.

```bash
java -version
/opt/kafka/bin/kafka-topics.sh --version
dotnet --version
git --version
```

**Resultado esperado:** Java 21, Kafka 4.2.x, .NET 10.x y la versión de Git instalada.

## 3. Preparar archivos de configuración

| Archivo | Ruta completa o relativa a la raíz definida | Configuración necesaria | Origen |
|---|---|---|---|
| `.kafka-curso.env` | `~/.kafka-curso.env` | Variables `KAFKA_HOME`, `PATH`, `BOOTSTRAP` y `BRIDGE_URL` | Crear (Netec) |
| `.bashrc` | `~/.bashrc` | Línea `source ~/.kafka-curso.env` | Crear (Netec) |

# Variables de Entorno

| Variable | Propósito | Valor o formato esperado | Alcance | Método de configuración |
|---|---|---|---|---|
| `KAFKA_HOME` | Ubicación de la CLI de Kafka | `/opt/kafka` | Usuario | `~/.kafka-curso.env` |
| `PATH` | Ejecutar `kafka-*.sh` sin ruta completa | Incluye `$KAFKA_HOME/bin` | Usuario | `~/.kafka-curso.env` |
| `BOOTSTRAP` | Dirección del listener externo, usada por la CLI y los proyectos .NET | `IP_VM:PUERTO_NODEPORT` | Usuario | `~/.kafka-curso.env` |
| `BRIDGE_URL` | URL del HTTP Bridge, usada en la Práctica 6 | `http://IP_VM:PUERTO_NODEPORT` | Usuario | `~/.kafka-curso.env` |

**Configuración — Linux, bash:**

```bash
IP_VM=$(hostname -I | awk '{print $1}')
PUERTO_KAFKA=$(kubectl get svc kafka-curso-kafka-external-bootstrap -n kafka -o jsonpath='{.spec.ports[0].nodePort}')
PUERTO_BRIDGE=$(kubectl get svc bridge-curso-bridge-service -n kafka -o jsonpath='{.spec.ports[0].nodePort}')
cat > ~/.kafka-curso.env <<EOF
export KAFKA_HOME=/opt/kafka
export PATH=\$KAFKA_HOME/bin:\$PATH
export BOOTSTRAP=$IP_VM:$PUERTO_KAFKA
export BRIDGE_URL=http://$IP_VM:$PUERTO_BRIDGE
EOF
grep -q kafka-curso.env ~/.bashrc || echo 'source ~/.kafka-curso.env' >> ~/.bashrc
source ~/.kafka-curso.env
```

**Verificación sin exponer secretos:**

```bash
echo "BOOTSTRAP=$BOOTSTRAP  BRIDGE_URL=$BRIDGE_URL"
which kafka-topics.sh
```

# Configuración Base para los Laboratorios

## Parámetros comunes

| Parámetro | Valor o convención | Observaciones |
|---|---|---|
| Raíz de trabajo | `~/kafka-ess-labs` | Repositorio de código preparado |
| Namespace de Kubernetes | `kafka` | [VALIDAR] |
| Nombre del clúster Kafka | `kafka-curso` | [VALIDAR] |
| Factor de replicación | 1 | Un único broker; la replicación se trata de forma conceptual |
| Seguridad del listener | Sin TLS ni autenticación | Solo con fines didácticos; TLS y autenticación se explican en el Capítulo 3 |
| Sufijo de nombres | `[INICIALES]` del participante | Grupos de consumidores y archivos de entregables |
| Entregables | `~/entregables/ACTNN_Bitacora_[INICIALES].md` | Una bitácora por práctica |

## Estructura de carpetas

```bash
mkdir -p ~/entregables
```

Las prácticas crean `~/act02` y `~/act05`. La carpeta `~/entregables` se conserva durante todo el curso para la evaluación.

## Recursos compartidos

| Recurso | Nombre o identificador | Estructura o configuración base | Responsable de creación |
|---|---|---|---|
| Topic | `pagos-registro` | 1 partición | Participante (Práctica 1) |
| Topic | `pagos-modelo` | 3 particiones | Participante (Práctica 2) |
| Topic | `pagos` | 3 particiones; clave = número de cuenta; valor = pago JSON | Participante (Práctica 3); se reutiliza hasta la Práctica 6 |
| Topic | `pagos-errores` | 1 partición; encabezados `error` y `origen` | Participante (Práctica 5) |
| Contrato del pago | JSON | `idPago` (texto), `cuenta` (texto), `monto` (decimal), `moneda` (texto) | Repositorio de código |

Cada participante tiene su propio clúster en su VM, por lo que no hay recursos compartidos entre participantes. La creación automática de topics debe estar deshabilitada o no utilizarse: las prácticas crean los topics de forma explícita [VALIDAR].

## Correcciones de coherencia

| Inconsistencia o dependencia omitida | Configuración unificada | Ajuste necesario en las copias de trabajo |
|---|---|---|
| El particionador predeterminado de Confluent.Kafka (`consistent_random`) difiere del de la CLI y del HTTP Bridge (murmur2) | `Partitioner = Partitioner.Murmur2Random` en el productor .NET | Se configura en la Práctica 3 |
| Los argumentos y bloques `TODO` de los proyectos .NET citados en las prácticas 3 a 6 | Los indicados en *Obtener los materiales* | [VALIDAR contra el repositorio de código] |
| La Práctica 6 usa el formato embebido `text` del HTTP Bridge | `Content-Type: application/vnd.kafka.text.v2+json` | [VALIDAR que la versión desplegada del HTTP Bridge lo admite] |

# Servicios Externos Requeridos

| Servicio | URL, endpoint o región | Uso | Acceso requerido |
|---|---|---|---|
| NuGet | https://api.nuget.org/v3/index.json | Restaurar Confluent.Kafka si no está en caché | Saliente HTTPS desde la VM |
| GitHub | https://github.com | Guías de laboratorio y repositorio de código | Saliente HTTPS desde la VM y el equipo del participante |

## Configuración de red

| Origen | Destino | Protocolo y puerto | Sentido | Condición |
|---|---|---|---|---|
| Terminal de la VM | Listener externo de Kafka | TCP, NodePort del bootstrap y del broker | Local | Obligatorio |
| Terminal de la VM | HTTP Bridge | HTTP, NodePort del servicio del bridge | Local | Obligatorio |
| Equipo del participante | VM del curso | [PROTOCOLO Y PUERTO DE ACCESO REMOTO] | Saliente | Obligatorio; informar restricciones corporativas con anticipación |
| VM | api.nuget.org, github.com | HTTPS 443 | Saliente | Recomendado |

# Creación de Entornos / Máquinas Virtuales / Emuladores

| Elemento | Tipo o plataforma | Imagen y versión | Recursos asignados | Red o región | Persistencia |
|---|---|---|---|---|---|
| VM del participante | VM | [IMAGEN Y VERSIÓN] | Ver *Requisitos de Hardware* | [RED] | Persistente durante el curso |
| Clúster Kubernetes | Clúster de un nodo dentro de la VM | [DISTRIBUCIÓN Y VERSIÓN] | — | Local a la VM | Persistente |
| Clúster Kafka | Recurso `Kafka` de Strimzi en KRaft | Kafka 4.2.x | 1 nodo controlador y broker | Namespace `kafka` | Almacenamiento persistente [VALIDAR] |

1. Aprovisionar una VM por participante a partir de la imagen preparada.
2. Comprobar que el clúster Kafka y el HTTP Bridge están listos después del arranque.
3. Generar `~/.kafka-curso.env` con la IP de la VM y los NodePort (ver *Variables de Entorno*).
4. Ejecutar las validaciones de la sección siguiente.

# Validación del Entorno

| ID | Componente o capacidad | Procedimiento | Resultado esperado | Evidencia mínima |
|---|---|---|---|---|
| V-01 | Clúster Kafka | `kubectl get kafka -n kafka` | `kafka-curso` con `READY True` y versión 4.2.x | Salida del comando |
| V-02 | CLI y conectividad | Crear, describir y eliminar el topic `validacion` | Creación y descripción sin errores | Salida de los comandos |
| V-03 | Publicación y lectura | Publicar y leer un evento en `validacion` | El evento se lee con el consumidor de consola | Salida del consumidor |
| V-04 | Proyectos .NET | `dotnet build` en ambos proyectos | `Build succeeded` | Salida del comando |
| V-05 | Cliente .NET | Publicar 1 pago y leerlo con el consumidor .NET | El consumidor procesa el pago | Salida del consumidor |
| V-06 | HTTP Bridge | `GET $BRIDGE_URL/healthy` y `POST` de un registro a `validacion` | 200/204 y respuesta con partición y offset | Salida de `curl` |

**Criterio de entorno listo:** V-01 a V-06 aprobadas en la VM de cada participante.

# Comandos de Verificación

## CLI de Kafka y conectividad

**Plataforma y terminal:** Linux, bash.  
**Directorio de ejecución:** `~`.  
**Prerrequisito:** `source ~/.kafka-curso.env`.

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic validacion --partitions 1 --replication-factor 1
echo "ok" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic validacion
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic validacion --from-beginning --max-messages 1
```

**Resultado esperado:**

```text
Created topic validacion.
ok
Processed a total of 1 messages
```

**Interpretación:** acredita conectividad con el listener externo, creación de topics y producción y consumo con la CLI.

## Proyectos .NET

**Directorio de ejecución:** `~/kafka-ess-labs`.

```bash
dotnet --version
dotnet build src/Pagos.Producer && dotnet build src/Pagos.Consumer
```

**Resultado esperado:**

```text
10.0.x
Build succeeded.
Build succeeded.
```

## HTTP Bridge

```bash
curl -s -o /dev/null -w "%{http_code}\n" $BRIDGE_URL/healthy
curl -s -X POST $BRIDGE_URL/topics/validacion \
  -H 'Content-Type: application/vnd.kafka.text.v2+json' \
  -d '{"records":[{"key":"k1","value":"ok"}]}'
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic validacion
```

**Resultado esperado:**

```text
200
{"offsets":[{"partition":0,"offset":1}]}
```

**Interpretación:** acredita que el HTTP Bridge está disponible y publica en el clúster con el formato que usa la Práctica 6.

# Troubleshooting

| Síntoma | Causa probable | Comprobación | Corrección | Validación posterior |
|---|---|---|---|---|
| `Timed out waiting for a node assignment` en la CLI | `BOOTSTRAP` vacío o incorrecto, o el clúster no está listo | `echo $BOOTSTRAP`; `kubectl get kafka -n kafka` | `source ~/.kafka-curso.env`; esperar `READY True` | V-02 |
| `kafka-topics.sh: command not found` | `PATH` sin `$KAFKA_HOME/bin` | `echo $PATH` | `source ~/.kafka-curso.env` | V-02 |
| `dotnet build` falla al restaurar Confluent.Kafka | Sin salida a NuGet y sin caché local | `curl -I https://api.nuget.org/v3/index.json` | Habilitar salida HTTPS o precargar la caché de NuGet en la imagen | V-04 |
| El cliente .NET se conecta pero no publica | El listener anuncia una dirección no alcanzable | `kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP` | Ajustar la dirección anunciada del listener `nodeport` en el recurso `Kafka` | V-05 |
| `curl` al HTTP Bridge devuelve `Connection refused` | Pod del bridge no listo o NodePort distinto | `kubectl get kafkabridge,svc -n kafka` | Regenerar `~/.kafka-curso.env` | V-06 |

# Checklist Final

- [ ] La VM cumple los requisitos de hardware validados.
- [ ] Strimzi 1.2.0 y Kafka 4.2.x en KRaft están desplegados y listos.
- [ ] El HTTP Bridge está expuesto por NodePort.
- [ ] Java 21, la CLI de Kafka 4.2, .NET 10, Git, Visual Studio Code y curl están instalados.
- [ ] El repositorio de código está clonado en `~/kafka-ess-labs` y ambos proyectos compilan.
- [ ] `~/.kafka-curso.env` define `KAFKA_HOME`, `PATH`, `BOOTSTRAP` y `BRIDGE_URL`.
- [ ] V-01 a V-06 completadas con los resultados esperados.
- [ ] El topic `validacion` fue eliminado después de la validación.
- [ ] Evidencias y fecha de validación registradas.
- [ ] Instrucciones de llenado, marcas `[VALIDAR]` y campos pendientes eliminados.

# Resumen

Cada participante trabaja en su propia VM con Apache Kafka 4.2 en modo KRaft desplegado por Strimzi 1.2.0, un listener `nodeport` sin seguridad y el HTTP Bridge expuesto por NodePort. Desde la terminal usa la CLI de Kafka 4.2 y los proyectos .NET 10 con Confluent.Kafka 2.16 del repositorio preparado, con las variables `BOOTSTRAP` y `BRIDGE_URL` definidas en `~/.kafka-curso.env`.

## Registro de validación

| Fecha | Responsable | Plataforma o entorno probado | Resultado | Limitaciones o acciones pendientes |
|---|---|---|---|---|
| [AAAA-MM-DD] | [NOMBRE] | [VM / IMAGEN] | [APTO / NO APTO] | [DETALLE O NINGUNA] |

## Referencias oficiales

| Documento o recurso | URL | Fecha de consulta |
|---|---|---|
| Apache Kafka 4.2. Quick Start | https://kafka.apache.org/42/getting-started/quickstart/ | [AAAA-MM-DD] |
| Apache Kafka 4.2. Basic Kafka Operations | https://kafka.apache.org/42/operations/basic-kafka-operations/ | [AAAA-MM-DD] |
| Strimzi 1.2.0. Deploying | https://strimzi.io/docs/operators/1.2.0/deploying | [AAAA-MM-DD] |
| Strimzi. Versiones del operador | https://github.com/strimzi/strimzi-kafka-operator/releases | [AAAA-MM-DD] |
| Strimzi. HTTP Bridge | https://strimzi.io/docs/bridge/latest/ | [AAAA-MM-DD] |
| Confluent. Cliente .NET 2.16 | https://docs.confluent.io/kafka-clients/dotnet/2.16/overview.html | [AAAA-MM-DD] |
| Confluent.Kafka en NuGet | https://www.nuget.org/packages/Confluent.Kafka | [AAAA-MM-DD] |
