# Decisiones de diseño y limitaciones

[← Volver al README](../README.md)

## Decisiones de diseño

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| Volume de Unity Catalog para la llegada de archivos | Montar un bucket o un contenedor con credenciales | El acceso lo gobierna el catálogo y los notebooks no llevan secretos |
| Marcar clientes inválidos como `999999` y conservar la venta | Descartar la orden | La venta ocurrió; perderla distorsiona el revenue. El miembro «desconocido» la mantiene visible |
| Enviar a cuarentena las órdenes cuyo producto no existe en el maestro | Conservarlas sin precio, o descartarlas sin dejar rastro | Sin producto no hay precio ni revenue que calcular |
| Un solo precio por producto y mes, el más alto | Dejar ambos | Con dos precios, el cruce con las órdenes duplicaría ventas |
| `MERGE` con llave de cuatro columnas y condición de cambio | Agregar filas (`append`) o reemplazar todo | Reintentar no duplica, y solo se reescribe lo que cambió |
| Tabla de hechos sin particionar, con `OPTIMIZE ZORDER` | Particionar por fecha | Con este volumen, particionar crea un archivo diminuto por cada fecha, más de 150 |
| Dimensiones reconstruidas completas en cada ejecución | `MERGE` también en dimensiones | Son pequeñas y no se necesita su historial en este caso |
| Calendario generado para todo el año | Solo fechas con ventas | Los lotes futuros siempre encuentran su fecha |
| Verificación con un cálculo independiente | Confiar en que el notebook corrió sin error | Que corra no prueba que el número sea correcto |

### Lo que no salió a la primera

- **Tareas «en ejecución» durante horas sin escribir nada.** La primera hipótesis fue falta de capacidad en la región. La causa real: el cluster se creó con 0 workers y sin el modo de nodo único, así que arrancaba sin nada que ejecutara el trabajo. Se encontró leyendo la configuración del cluster, no reintentando.
- **Riesgo de ventas duplicadas.** Había 24 combinaciones producto-mes con dos precios. Se detectó revisando los datos antes de hacer el cruce.
- **La prueba de idempotencia no se podía repetir.** El notebook borraba la tabla intermedia al terminar. Apareció al ejecutar la demostración completa dos veces.

## Limitaciones y siguientes pasos

- **Los datos son sintéticos.** Están diseñados para ejercitar cada regla de limpieza; no tienen el volumen ni las sorpresas de un sistema real. Por ejemplo, ningún cliente con identificador numérico falta en el maestro: la validación lo detectaría como venta sin dimensión, pero el pipeline no tiene una regla para ese caso. El precio más alto como precio vigente también es una regla del caso, no una verdad general.
- **Auto Loader corre por lotes** (`availableNow`), no como proceso continuo, y los archivos se quedan en `landing/` sin archivarse. Si se borran las tablas sin borrar los *checkpoints*, no vuelve a cargar nada: por eso el reset borra ambos.
- **Si la misma orden llega dos veces en un mismo archivo con cantidades distintas**, gana la de mayor cantidad: entre archivos gana la leída al final, pero dentro de un archivo no hay un orden de llegada. En clientes y productos, al quitar duplicados no se elige qué fila sobrevive.
- **Los 12 productos con identificador inválido comparten `product_id = 999999`.** Hoy no tienen precio ni órdenes; si llegara una orden con ese identificador, el cruce por `product_id` la multiplicaría por 12.
- **Silver y las dimensiones se reconstruyen completas** y no guardan historial de cambios (SCD tipo 2). Solo las órdenes y la tabla de hechos son incrementales.
- **La validación compara contra cifras fijas** porque los datos son fijos. Con datos reales harían falta reglas relativas (rangos, variaciones entre cargas), no igualdades.
- **El despliegue desde GitHub se ejecutó una vez, en `prod`:** el flujo [`deploy.yml`](../.github/workflows/deploy.yml) desplegó el bundle con un service principal y corrió el pipeline desde cero ([corrida registrada](https://github.com/manuel-arguelles-ai/fmcg-medallion-lakehouse-azure/actions/runs/44535633142)). El entorno `dev` escribe en el catálogo `fmcg_dev`, que no se creó en este proyecto: `dev` se probó apuntando a `fmcg`. Sin los secretos del service principal, el flujo se omite.
- **Las reglas de calidad están declaradas en el notebook** (un diccionario de reglas y restricciones `CHECK` en las tablas), no en un marco como Lakeflow Declarative Pipelines, que además llevaría el historial de cumplimiento. Solo las órdenes tienen cuarentena.
- **La lista de precios solo cubre de enero a junio de 2026.** Una orden posterior quedaría con precio y revenue nulos; la validación los cuenta, pero no hay una regla para ese caso.
- **El job es una cadena lineal**, aunque las cuatro cargas de Bronze son independientes y podrían correr en paralelo.
- **Las reglas «confirmadas por negocio»** (las cuatro ciudades y el precio más alto) son supuestos del caso: los datos son sintéticos.
- **Otras:** llaves naturales sin llaves sustitutas, importes en `DOUBLE` y no en `DECIMAL`, `ZORDER` aplicado sin medir su efecto (con menos de 5.000 filas no se espera diferencia), Change Data Feed activado sin consumidor y calendario fijo en 2026.

Siguientes pasos: validación con reglas relativas entre cargas y que no dependa del historial de las tablas, expectativas de calidad con Lakeflow Declarative Pipelines, alertas ante fallos, crear y probar el catálogo de `dev` y SCD tipo 2 en clientes.
