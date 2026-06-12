

## Ejercicio 1

Sobre extension **(pregunta d):**

Para llevar a cabo una extension (funcion que le pasamos a cada transformacion de dato) en ``Spark``:

* no debemos asumir que un worker especifico hara una tarea en particular (no podemos conocer el comportamiento que tomara el driver de antemano)
* no debemos permitir que los workers trabajen en un valor que genere condicion de carrera(ej sobre un contador)
* el trabajo que lleve a cabo el worker no debe tener efectos secundarios, porque estos trabajan en paralelo sin coordinacion

## Ejercicio 3

Conceptualmente ``reduceByKey(f)`` combina todo los elementos en un unico resultado aplicando f acumulativamente, se necesita que f sea asociativa y conmutativa para que el orden de combinacion no importe ni afecte al resultado final.

Se dice que ``reduceByKey`` es una barrera de sincronizacion porque para ejecutarse debidamente espera a que todos los workers implicados terminen sus tareas previas y no pueden hacer mas tareas hasta que ``reduceByKey`` termine de computar. 

En el codigo se puede ver que ``reduceByKey`` espera cada par que proceso cada worker y dependiendo del entidad, ``reduceByKey`` lo mandara al worker encargado para esa entidad (este mecanismo se llama shuffle) y se logra la suma final.

Previo a usar el ``reduceByKey`` necesito trabajar con el diccionario, este diccionario es cargado solamente en el driver y distribuido a los workers cuando es necesario, si no tuviera broadcast, el driver le daria una copia del dato a cada worker, como uso ``.broadcast()`` el driver hace que el dato sea accesible unicamente por lectura, asi que los workers consultaran el valor cuando sea necesario, broadcast existe para hacer todo el flujo mas eficiente.