<a href="https://netec.com">
  <img src="https://static.wixstatic.com/media/96edd2_934dddb3230b4b16b1d1c56c51143738~mv2.png" alt="Logo de Netec" width="300"/>
</a>

# Apache Kafka para desarrolladores .NET

## Plataforma de laboratorios

Te damos la bienvenida a la **plataforma de laboratorios** del curso **Apache Kafka para desarrolladores .NET**. Aquí podrás explorar diferentes tecnologías a través de prácticas guiadas. ¡Desarrolla tus habilidades y lleva tus conocimientos al siguiente nivel!

Todas las prácticas siguen un mismo caso ficticio, una entidad financiera que registra pagos, y se realizan en la máquina virtual del curso con Apache Kafka 4.2 en modo KRaft desplegado con Strimzi 1.2.0, .NET 10 y Confluent.Kafka 2.16. Antes de comenzar, revisa la [Guía de configuración del entorno](Setup_Guide.md).

## Lista de laboratorios

Cada uno de estos laboratorios está diseñado para ofrecerte una experiencia práctica. Haz clic en los enlaces para comenzar.

## Capítulo 1. El paradigma de Kafka

### [Práctica 1. Recorrer el registro de eventos con la CLI](Capitulo01/Practica01/README.md)

  - **Descripción**: Crear un topic de pagos, publicar eventos y leerlos con dos grupos de consumidores para comprobar la retención, la reanudación y la relectura, y contrastar Kafka con una cola tradicional.
  - ⏱️ **Duración estimada**: 45 min.

## Capítulo 2. Topics, particiones, claves y offsets

### [Práctica 2. Comprobar el efecto de claves, particiones y grupos de consumidores](Capitulo02/Practica02/README.md)

  - **Descripción**: Predecir y comprobar la partición y el orden por cuenta, observar el particionador sticky, repartir particiones entre consumidores, medir lag, reposicionar un grupo y calcular el throughput pico.
  - ⏱️ **Duración estimada**: 67 min.

## Capítulo 3. Clientes .NET con C#

### [Práctica 3. Conectar un productor C#/.NET con un consumidor C#/.NET](Capitulo03/Practica03/README.md)

  - **Descripción**: Configurar un productor y un consumidor .NET con Confluent.Kafka, publicar pagos JSON con la cuenta como clave y analizar acks, idempotencia y el particionador murmur2_random.
  - ⏱️ **Duración estimada**: 50 min.

### [Práctica 4. Escalar el consumo y estimar el número de particiones](Capitulo03/Practica04/README.md)

  - **Descripción**: Escalar el consumo con varias instancias del consumidor .NET, observar las reasignaciones, comparar auto.offset.reset y calcular el mínimo inicial de particiones.
  - ⏱️ **Duración estimada**: 44 min.

## Capítulo 4. Errores, reproceso y sistemas sin cliente Kafka

### [Práctica 5. Provocar fallas en el consumidor sin perder pagos](Capitulo04/Practica05/README.md)

  - **Descripción**: Interrumpir el consumidor a mitad de un lote, comparar la confirmación manual y la automática, hacer idempotente el procesamiento y enviar los eventos inválidos a un topic de errores.
  - ⏱️ **Duración estimada**: 63 min.

### [Práctica 6. Publicar desde un sistema legado por HTTP](Capitulo04/Practica06/README.md)

  - **Descripción**: Publicar pagos de un sistema sin cliente Kafka mediante el HTTP Bridge de Strimzi y verificar que el consumidor .NET los procesa.
  - ⏱️ **Duración estimada**: 25 min.

---

## 📬 **Contacto y más información**

Si tienes alguna pregunta o necesitas más detalles, no dudes en [contactarnos](mailto:soporte@netec.com). También puedes encontrar más recursos en nuestra [página](https://netec.com).

---

¡Gracias por visitar nuestra plataforma! No olvides revisar todos los laboratorios y comenzar tu viaje de aprendizaje hoy mismo.
