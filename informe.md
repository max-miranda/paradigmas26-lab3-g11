# Laboratorio 3 Paradigmas de la programacion; Procesamiento distribuido Apache Spark
## Jerez Sofia; Oneto Yamila; Miranda Maximo; Diaz Valentin

## Ejercicio 1

Sobre abstracciones de Spark **(pregunta b):**

En el pipeline que se encuentra en ``Main.scala``, se utiliza ``flatMap`` para guardar los posts de cada feed descargado en una constante de tipo ``RDD[Post]``. Se usa ``flatMap`` porque cada suscripcion puede generar una cantidad variable de posts, o incluso ninguno si falla la descarga o si todos los posts son filtrados. El trabajo que se realiza dentro de este ``flatMap`` sera paralelizado por Spark.

De manera muy similar, ``flatMap`` tambien es utilizado para detectar entidades en los posts. En este caso, cada post puede contener cero, una o varias entidades, por lo que nuevamente tiene sentido usar ``flatMap``. Esta etapa tambien es paralelizable porque la deteccion de entidades de un post no depende de la deteccion realizada en otros posts.

La funcion ``map`` se utiliza para transformar cada entidad detectada en un par de la forma ``((tipo, nombre), 1)``. Esto se hace para preparar los datos para el conteo, porque luego ``reduceByKey`` puede agrupar todos los pares que tengan la misma clave y sumar sus valores.

Se utiliza ``reduceByKey`` para agrupar las entidades que se detectaron previamente y obtener la cantidad total de apariciones de cada una. En nuestro caso, la clave es ``(entityType, text)``, por lo que se cuentan por separado las entidades segun su tipo y su nombre.

Hay algunos pasos del pipeline que no encajan directamente en ``map``, ``flatMap`` o ``reduceByKey`` porque no son transformaciones distribuidas sobre los datos, sino acciones, configuraciones o tareas que corresponden al driver. Por ejemplo, la lectura de argumentos, la carga inicial del archivo de suscripciones, la creacion de la ``SparkSession`` y la carga del diccionario ocurren en el driver antes de distribuir el trabajo. Tambien ``count()`` y ``collect()`` no son transformaciones sino acciones, porque fuerzan la ejecucion del pipeline y devuelven informacion al driver. Por ultimo, ``cache()`` tampoco transforma los datos, sino que le indica a Spark que conserve el RDD calculado para no recomputarlo en acciones posteriores.

Sobre barrera de sincronizacion **(pregunta c):**

En nuestro proyecto, todo lo que implica la descarga de los feeds (descarga, parseo de los posts y filtrado) se hace de forma paralela. Una vez realizada la descarga, 
nos encontramos con una barrera de sincronización, ```downloadResultsRDD.count()```, el cual debe esperar a que todos los workers terminen su ejecucion para poder obtener los datos.
Luego, la deteccion de entidades tambien es paralela e independiente, siguiendo con el manejo de entidades nos encontramos con 2 barreras de sincronizacion, ```reduceByKey``` y 
```sortBy```, en ambos casos sus ejecuciones son paralelas pero obligatoriamente deben esperar que todos los workers hayan terminado su trabajo anterior para poder comenzar. 
La ultima barrera que se presenta es ```pairs.collect()```, cuyo comportamiento es analogo al de ```downloadResultsRDD.count()```.

Sobre extension **(pregunta d):**

Para llevar a cabo una extension (funcion que le pasamos a cada transformacion de dato) en ``Spark``:

* no debemos asumir que un worker especifico hara una tarea en particular (no podemos conocer el comportamiento que tomara el driver de antemano)
* no debemos permitir que los workers trabajen en un valor que genere condicion de carrera(ej sobre un contador)
* el trabajo que lleve a cabo el worker no debe tener efectos secundarios, porque estos trabajan en paralelo sin coordinacion

## Ejercicio 2

Al trabajar con drivers y workers se tiene como objetivo que un error o excepcion de un nodo no afecte a los demas. Por esta razon, se busca que el
manejo de errores sea de forma controlada y de ser posible no se propague. Si dejaramos que la excepcion de un worker se propague, esta podria provocar 
la terminacion del programa, incluso si todos los demas nodos estaban trabajando de forma correcta. Es decir, que por un minimo error en solo una de 
las tantas ejecuciones, se aborta el trabajo, se descartan todos los otros workers y se desperdicia su computo ya realizado.    


## Ejercicio 3

Conceptualmente ``reduceByKey(f)`` combina todo los elementos en un unico resultado aplicando f acumulativamente, se necesita que f sea asociativa y conmutativa para que el orden de combinacion no importe ni afecte al resultado final.

Se dice que ``reduceByKey`` es una barrera de sincronizacion porque para ejecutarse debidamente espera a que todos los workers implicados terminen sus tareas previas y no pueden hacer mas tareas hasta que ``reduceByKey`` termine de computar. 

En el codigo se puede ver que ``reduceByKey`` espera cada par que proceso cada worker y dependiendo del entidad, ``reduceByKey`` lo mandara al worker encargado para esa entidad (este mecanismo se llama shuffle) y se logra la suma final.

Previo a usar el ``reduceByKey`` necesito trabajar con el diccionario, este diccionario es cargado solamente en el driver y distribuido a los workers cuando es necesario, si no tuviera broadcast, el driver le daria una copia del dato a cada worker, como uso ``.broadcast()`` el driver hace que el dato sea accesible unicamente por lectura, asi que los workers consultaran el valor cuando sea necesario, broadcast existe para hacer todo el flujo mas eficiente.

## Ejercicio 5

Sobre ``cache()``

En nuestro proyecto usamos ``cache()`` sobre ``downloadResultsRDD`` porque ese RDD representa el resultado de una etapa costosa porque incluye descargar los feeds, parsearlos y filtrar los posts validos. Si no llamaramos a ``cache()``, Spark podria recomputar toda la cadena de transformaciones que produce ese RDD cada vez que una accion necesite sus datos.

Esto es importante porque Spark trabaja de forma lazy: las transformaciones no guardan automaticamente sus resultados. Entonces, si primero hacemos una accion como ``downloadResultsRDD.count()`` para materializar la descarga y despues usamos ``downloadResultsRDD`` para detectar entidades, sin ``cache()`` Spark podria volver a ejecutar la descarga de feeds desde el principio.

En ese caso, la descarga podria ejecutarse mas de una vez. En nuestro flujo concreto, se ejecutaria al menos una vez para ``downloadResultsRDD.count()`` y podria volver a ejecutarse cuando se fuerza la accion final ``pairs.collect()``. Por eso cacheamos el RDD de posts, porque asi la descarga, el parseo y el filtrado se hagan una sola vez y los siguientes pasos reutilicen el resultado ya materializado.

Sobre ``collect()`` entre pasos del pipeline

Seria incorrecto llamar a ``collect()`` entre los pasos de deteccion y conteo de entidades, por ejemplo despues del ``flatMap`` que obtiene las entidades y antes del ``map`` que arma los pares ``((tipo, nombre), 1)``. ``collect()`` trae todos los datos desde los workers al driver, por lo que se perderia la distribucion del trabajo.

Si hicieramos eso, el driver recibiria todas las entidades como una coleccion local y los pasos siguientes ya no se ejecutarian distribuidos sobre un ``RDD``. Es decir, dejariamos de aprovechar Spark justo antes de una etapa importante del problema, que es el conteo global de entidades. Ademas, si hubiera muchos posts o muchas entidades, traer todo al driver podria generar problemas de memoria.

Por eso en nuestro codigo ``collect()`` se usa al final, cuando ya se hizo el ``reduceByKey`` y el resultado ya esta agregado. En ese punto el volumen de datos es mucho menor que el conjunto original de posts y tiene sentido traerlo al driver para formatearlo e imprimirlo.

Sobre lazy evaluation de ``cache()``

``cache()`` tambien es lazy. Esto significa que llamar a ``downloadResultsRDD.cache()`` no descarga feeds ni guarda datos inmediatamente en memoria. Solamente le indica a Spark que, cuando ese RDD sea calculado por primera vez, conviene conservar sus particiones para reutilizarlas.

El RDD se almacena realmente en memoria cuando se ejecuta una accion que obliga a computarlo. En nuestro caso, eso ocurre con ``downloadResultsRDD.count()``. Esa accion fuerza a Spark a recorrer las suscripciones, descargar los feeds, parsear los posts, filtrarlos y materializar ``downloadResultsRDD``. A partir de ahi, las siguientes operaciones que dependan de ese RDD pueden reutilizar los datos cacheados en vez de recomputarlos.

Finalmente, cuando ya no necesitamos mas ese RDD, llamamos a ``downloadResultsRDD.unpersist()`` para liberar la memoria usada por Spark.
