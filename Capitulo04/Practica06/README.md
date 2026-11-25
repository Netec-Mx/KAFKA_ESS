<!--
Pendiente de validación por el instructor: confirmar contra el repositorio de código preparado
la ruta y los argumentos del script legado (~/kafka-ess-labs/legado/legado-pagos.sh) y que la versión
del HTTP Bridge desplegada acepta el formato embebido text. Eliminar este comentario al validar.
-->

# Práctica 6. Publicar desde un sistema legado por HTTP

**Capítulo 4. Errores, reproceso y sistemas sin cliente Kafka**

⏱️ **Duración:** 25 min · 🛠️ **Herramienta:** HTTP Bridge de Strimzi, `curl` y CLI de Kafka 4.2 · 📄 **Entregable:** Bitácora ACT-06

## Descripción

Un sistema de pagos legado de la entidad financiera ficticia no puede incorporar un cliente Kafka; solo puede hacer llamadas HTTP. En esta práctica el participante publica sus pagos en el topic `pagos` a través del HTTP Bridge de Strimzi, con la cuenta como clave en formato de texto, comprueba con la CLI que llegan con su clave y a la partición esperada, y verifica que el consumidor .NET de la Práctica 5 los procesa junto con los del productor .NET.

## Objetivos de aprendizaje

Al finalizar la práctica, el participante será capaz de:

- Comprobar la disponibilidad del HTTP Bridge y publicar un evento con clave mediante una llamada HTTP.
- Ejecutar el script del sistema legado contra el HTTP Bridge.
- Verificar con la CLI y con el consumidor .NET el recorrido completo de un pago publicado por HTTP.

<img src="../../images/p06-objetivo.svg" alt="Diagrama de la Práctica 6" width="720"/>

## Antes de empezar

| Elemento | Valor |
|---|---|
| Dónde trabajar | Terminal de la máquina virtual del curso |
| Variable del HTTP Bridge | `$BRIDGE_URL` (definida en el [Setup Guide](../../Setup_Guide.md)) |
| Script del sistema legado | `~/kafka-ess-labs/legado/legado-pagos.sh` |
| Topic de la práctica | `pagos` |
| Grupo de consumidores | `pagos-act05-[INICIALES]` (el de la Práctica 5) |
| Nombre del entregable | `ACT06_Bitacora_[INICIALES].md` en `~/entregables/` |

> Sustituya `[INICIALES]` por sus iniciales en minúsculas. Todos los datos de la práctica son ficticios.

---

## Tarea 1. Comprobar el HTTP Bridge (5 min)

**Objetivo:** Obtener la respuesta del HTTP Bridge que confirma que está disponible y que ve el topic `pagos`.

**Por qué:** El HTTP Bridge es un servicio intermedio: traduce llamadas HTTP a operaciones de productor y consumidor de Kafka. Antes de publicar hay que confirmar que responde.

**Paso 1.** Crear la bitácora y comprobar la variable:

```bash
printf '# ACT-06. Bitácora\n\n## Predicción\nEl pago CTA-1001 publicado por HTTP caerá en la partición...\n\n' \
  > ~/entregables/ACT06_Bitacora_[INICIALES].md
echo $BRIDGE_URL
```

Completar la predicción con la partición observada para `CTA-1001` en la Práctica 3.

**Paso 2.** Consultar el estado del HTTP Bridge y la lista de topics:

```bash
curl -s -o /dev/null -w "%{http_code}\n" $BRIDGE_URL/healthy
curl -s $BRIDGE_URL/topics
```

- **Qué verás:** el código `200` o `204` y una lista JSON que incluye `"pagos"` y `"pagos-errores"`.
- **Si no ocurre así:** si `curl` no conecta, revisar la sección *Troubleshooting* del [Setup Guide](../../Setup_Guide.md).

**Al terminar esta tarea debes tener:**

- ✔ El HTTP Bridge disponible.

---

## Tarea 2. Publicar un pago por HTTP con clave de texto (7 min)

**Objetivo:** Obtener la respuesta del HTTP Bridge con la partición y el offset del pago `P-HTTP-01` de la cuenta `CTA-1001`.

**Por qué:** En el formato `text`, la clave y el valor se envían como texto tal cual. La clave `CTA-1001` llega igual que la que usa el productor .NET, por lo que el pago cae en la misma partición.

**Paso 1.** Publicar el pago:

```bash
curl -s -X POST $BRIDGE_URL/topics/pagos \
  -H 'Content-Type: application/vnd.kafka.text.v2+json' \
  -d '{"records":[{"key":"CTA-1001","value":"{\"idPago\":\"P-HTTP-01\",\"cuenta\":\"CTA-1001\",\"monto\":250.00,\"moneda\":\"MXN\"}"}]}'
```

- **Qué verás:** una respuesta como `{"offsets":[{"partition":N,"offset":M}]}`.
- **Importante:** el valor es un texto que contiene el JSON del pago; por eso las comillas internas van escapadas con `\`.

**Paso 2.** Comparar la partición de la respuesta con la de `CTA-1001` en la Práctica 3 y anotar en la bitácora, bajo `## Evidencia 1. Llamada HTTP`, la llamada y la respuesta.

**Al terminar esta tarea debes tener:**

- ✔ El pago `P-HTTP-01` publicado, con su partición y offset.

---

## Tarea 3. Ejecutar el sistema legado (5 min)

**Objetivo:** Obtener la salida del script del sistema legado con 5 pagos publicados por HTTP.

**Por qué:** El script simula al sistema legado real: registra pagos y los informa mediante las mismas llamadas HTTP de la Tarea 2.

**Paso 1.** Revisar la llamada que hace el script:

```bash
grep -n "curl" ~/kafka-ess-labs/legado/legado-pagos.sh
```

- **Qué verás:** una llamada `POST` a `$BRIDGE_URL/topics/pagos` con el mismo `Content-Type` de la Tarea 2.

**Paso 2.** Ejecutar el script para 5 pagos:

```bash
bash ~/kafka-ess-labs/legado/legado-pagos.sh 5 | tee ~/entregables/act06-legado.txt
```

- **Qué verás:** cinco respuestas con su partición y offset.

**Al terminar esta tarea debes tener:**

- ✔ Cinco pagos del sistema legado publicados.

---

## Tarea 4. Verificar el recorrido con la CLI y el consumidor .NET (8 min)

**Objetivo:** Obtener la salida de la CLI con los pagos HTTP y sus claves, y la del consumidor .NET procesándolos junto con pagos del productor .NET.

**Por qué:** El objetivo de negocio es que los pagos del sistema legado se procesen igual que los demás, sin cambios en el consumidor.

**Paso 1.** Leer los últimos pagos del topic con su clave:

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic pagos \
  --group revision-http-[INICIALES] --from-beginning --timeout-ms 10000 \
  --property print.key=true --property print.partition=true 2>/dev/null | grep -E "P-HTTP|LEG"
```

- **Qué verás:** el pago `P-HTTP-01` y los pagos del sistema legado, cada uno con su clave de cuenta como columna separada.

**Paso 2.** En **T1**, publicar 5 pagos con el productor .NET:

```bash
cd ~/kafka-ess-labs && dotnet run --project src/Pagos.Producer -- --cantidad 5
```

**Paso 3.** En **T2**, ejecutar el consumidor .NET con los ajustes de la Práctica 5 y detenerlo con **Ctrl+C** cuando deje de procesar:

```bash
dotnet run --project src/Pagos.Consumer -- --grupo pagos-act05-[INICIALES] \
  --salida ~/act05/resultado-idem.csv | tee ~/entregables/act06-consumidor.txt
```

- **Qué verás:** los 6 pagos publicados por HTTP y los 5 del productor .NET procesados, sin líneas `INVALIDO`.

**Paso 4.** Comprobar el lag del grupo:

```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group pagos-act05-[INICIALES]
```

- **Qué verás:** `LAG 0`.

**Paso 5.** Copiar las salidas a la bitácora bajo `## Evidencia 2. Recorrido` y agregar una explicación de tres a cinco líneas del recorrido: sistema legado → HTTP Bridge → topic `pagos` → consumidor .NET. Mencionar en una línea cuándo convendría Kafka Connect en lugar del HTTP Bridge.

**Al terminar esta tarea debes tener:**

- ✔ Los pagos HTTP visibles en la CLI con su clave.
- ✔ El consumidor .NET procesando pagos HTTP y .NET, con lag cero.

---

## Validación y pruebas

| Caso | Acción | Resultado esperado |
|---|---|---|
| Caso 1 | `GET $BRIDGE_URL/healthy` | Código 200 o 204 |
| Caso 2 | `POST` de `CTA-1001` en formato `text` | Respuesta con partición y offset; misma partición que en la Práctica 3 |
| Caso 3 | Leer el topic con `print.key=true` | Los pagos HTTP muestran la cuenta como clave |
| Caso adversarial | Enviar el pago con `Content-Type: application/json` | El HTTP Bridge rechaza la llamada con un error de tipo de contenido |

## Resultado esperado

- Los pagos publicados por HTTP llegan al topic con su clave.
- El consumidor .NET los procesa junto con los del productor .NET.

**Entregable:** Llamadas HTTP utilizadas, salidas de la CLI y del consumidor y una explicación breve del recorrido (`ACT06_Bitacora_[INICIALES].md`).

<!-- Agregar captura del resultado final: ../../images/p06-resultado.png -->

## Solución de problemas

**El HTTP Bridge responde 415 o 422**
- *Síntoma:* la llamada `POST` devuelve un error de formato.
- *Causa:* el `Content-Type` no coincide con el cuerpo, o el valor no es un texto con las comillas escapadas.
- *Solución:* copiar de nuevo el comando de la Tarea 2 sin modificar el encabezado ni el cuerpo.

**La clave aparece entre comillas en la CLI**
- *Síntoma:* la clave se muestra como `"CTA-1001"`.
- *Causa:* se usó el formato `application/vnd.kafka.json.v2+json`, que serializa la clave como JSON.
- *Solución:* usar `application/vnd.kafka.text.v2+json`. Con la clave entre comillas la partición puede no coincidir con la del productor .NET.

**`curl` no conecta con el HTTP Bridge**
- *Síntoma:* `Connection refused` o tiempo de espera agotado.
- *Causa:* la variable `$BRIDGE_URL` no está definida o el pod del HTTP Bridge no está listo.
- *Solución:* ejecutar `source ~/.kafka-curso.env` y `kubectl get kafkabridge -n kafka`.

## Limpieza

- Detener el productor y el consumidor. Conservar la carpeta `~/entregables/` con las bitácoras ACT-01 a ACT-06 para la evaluación.

## Recursos opcionales

- [Strimzi. HTTP Bridge](https://strimzi.io/docs/bridge/latest/)
- [Apache Kafka 4.2. Kafka Connect](https://kafka.apache.org/42/kafka-connect/overview/)

<sub>Complejidad: Intermedia · Nivel de Bloom: Aplicar · Resultado de aprendizaje que verifica: R5</sub>

---

[⬅ Anterior: Práctica 5](../../Capitulo04/Practica05/README.md) | [Inicio](../../README.md)
