# PFO 3 - Sistema Distribuido Cliente-Servidor

## Autor
Micaela Lujan Orellano
Tecnicatura en Software
Programación sobre Redes - 2026

## Descripción

Sistema distribuido implementado en Python que utiliza arquitectura cliente-servidor con sockets TCP. El sistema incluye:

- **Servidor principal** que recibe tareas por socket TCP
- **Pool de workers** con hilos para procesamiento concurrente
- **Cliente** para enviar tareas y recibir resultados
- **Cola de tareas interna** para gestión eficiente de carga
- **Distribución round-robin** entre workers

## Opcionales

No se implemento los siguientes puntos de la practica:
- Cola de mensajes (RabbitMQ) para comunicación entre servidores. 
- Almacenamiento distribuido (PostgreSQL, S3). 

## Arquitectura
El sistema implementa una arquitectura distribuida con los siguientes componentes:

```
Cliente → Socket TCP → Servidor → Cola de Tareas → Workers (con Thread Pool)
```
Se encuentra el diagrama completo la carpeta diagrama, el archivo: Arquitectura.jpg

## Estructura del Proyecto

```
PFO3/
├── servidor/
│   └── servidor.py          # Servidor principal con workers
├── cliente/
│   └── cliente.py           # Cliente para enviar tareas y recibir resultados
├── diagrama/
│   └── arquitectura.jpg         # Diagrama visual de arquitectura
├── .gitignore               # Archivos ignorados por Git
├── requirements.txt         # Dependencias opcionales
└── README.md                # Este archivo
```

## Instalación

### Requisitos Previos

- Python 3.8 o superior

### Pasos de Instalación

1. **Clonar el repositorio**
```bash
git clone [<url-del-repositorio>](https://github.com/Micaela2016/PR-PF03.git)
cd PFO3
```

2. **Instalar dependencias (opcional)**
```bash
pip install -r requirements.txt
```

## Uso

### Paso 1: Iniciar el Servidor

Abre una terminal y ejecuta:

```bash
python servidor/servidor.py
```

**Opciones disponibles:**
```bash
python servidor/servidor.py --host 0.0.0.0 --port 5000 --workers 3
```

**Parámetros:**
- `--host`: Host del servidor (default: 0.0.0.0)
- `--port`: Puerto del servidor (default: 5000)
- `--workers`: Número de workers (default: 3)

**Salida esperada:**
```
2026-06-05 17:00:00 - __main__ - INFO - Worker 0 inicializado con 4 hilos
2026-06-05 17:00:00 - __main__ - INFO - Worker 1 inicializado con 4 hilos
2026-06-05 17:00:00 - __main__ - INFO - Worker 2 inicializado con 4 hilos
2026-06-05 17:00:00 - __main__ - INFO - Servidor inicializado con 3 workers
2026-06-05 17:00:00 - __main__ - INFO - Servidor escuchando en 0.0.0.0:5000
```

### Paso 2: Usar el Cliente

Abre **otra terminal** y ejecuta:

#### Opción A: Modo Interactivo

```bash
python cliente/cliente.py --mode interactive
```

Te permite enviar tareas personalizadas o de ejemplo.

#### Opción B: Modo Demo

```bash
python cliente/cliente.py --mode demo
```

Ejecuta automáticamente 5 tareas de ejemplo.

**Parámetros del cliente:**
```bash
python cliente/cliente.py --host localhost --port 5000 --timeout 30 --mode interactive
```

- `--host`: Host del servidor (default: localhost)
- `--port`: Puerto del servidor (default: 5000)
- `--timeout`: Timeout en segundos (default: 30)
- `--mode`: Modo de ejecución (interactive/demo)

## Formato de Tareas

Las tareas se envían en formato JSON:

```json
{
  "id": "tarea_1",
  "data": {
    "mensaje": "Hola",
    "numero": 42,
    "cualquier_dato": "valor"
  }
}
```

**Respuesta del servidor:**

```json
{
  "task_id": "tarea_1",
  "worker_id": 0,
  "status": "completed",
  "data": {
    "mensaje": "Hola",
    "numero": 42,
    "cualquier_dato": "valor"
  },
  "processed_at": "2026-06-05T17:00:01.123456"
}
```

## Ejemplos de Uso

### Ejemplo 1: Modo Demo

**Terminal 1 - Servidor:**
```bash
python servidor/servidor.py
```

**Terminal 2 - Cliente:**
```bash
python cliente/cliente.py --mode demo
```

### Ejemplo 2: Modo Interactivo

**Terminal 1 - Servidor:**
```bash
python servidor/servidor.py --workers 5
```

**Terminal 2 - Cliente:**
```bash
python cliente/cliente.py --mode interactive
```

Luego selecciona opción 2 (tarea de ejemplo) o 1 (tarea personalizada).

### Ejemplo 3: Desde Código Python

```python
from cliente.cliente import DistributedClient

# Crear cliente
client = DistributedClient(host='localhost', port=5000)

# Enviar tarea
task = {
    'id': 'mi_tarea_1',
    'data': {
        'accion': 'procesar',
        'valores': [1, 2, 3, 4, 5]
    }
}

result = client.send_task(task)
print(result)
```
