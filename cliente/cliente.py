"""
PFO 3 - Cliente - Sistema Distribuido Cliente-Servidor
Envía tareas al servidor y recibe resultados mediante sockets
"""

import socket
import json
import logging
import time
from typing import Dict, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DistributedClient:
    
    def __init__(self, host: str = 'localhost', port: int = 5000, timeout: int = 30):
        self.host = host
        self.port = port
        self.timeout = timeout
        logger.info(f"Cliente inicializado para conectar a {host}:{port}")
    
    def send_task(self, task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Envía una tarea genérica al servidor y espera el resultado
        
        Args:
            task: Diccionario con la tarea a enviar (debe incluir 'id' y 'data')
            
        Returns:
            Resultado de la tarea o None si hay error
        """
        client_socket = None
        try:
            # Crear socket y conectar al servidor
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(self.timeout)
            
            logger.info(f"Conectando a {self.host}:{self.port}...")
            client_socket.connect((self.host, self.port))
            logger.info("Conexión establecida")
            
            # Enviar tarea
            task_json = json.dumps(task).encode('utf-8')
            client_socket.sendall(task_json)
            logger.info(f"Tarea enviada: {task.get('id', 'unknown')}")
            
            # Recibir respuesta
            response_data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                response_data += chunk
                if len(chunk) < 4096:
                    break
            
            if not response_data:
                logger.error("No se recibió respuesta del servidor")
                return None
            
            # Decodificar respuesta
            response = json.loads(response_data.decode('utf-8'))
            logger.info(f"Respuesta recibida: {response.get('status', 'unknown')}")
            
            return response
            
        except socket.timeout:
            logger.error(f"Timeout esperando respuesta del servidor ({self.timeout}s)")
            return None
        except ConnectionRefusedError:
            logger.error(f"No se pudo conectar al servidor en {self.host}:{self.port}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando respuesta JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Error enviando tarea: {e}")
            return None
        finally:
            if client_socket:
                try:
                    client_socket.close()
                except:
                    pass


def print_result(result: Optional[Dict[str, Any]]):
    """Imprime el resultado de forma legible"""
    if result is None:
        print(" Error: No se recibio respuesta del servidor\n")
        return
    
    print("\n" + "="*60)
    print("RESULTADO DE LA TAREA")
    print("="*60)
    
    if result.get('status') == 'error':
        print(f"Error: {result.get('error', 'Unknown error')}")
    else:
        print(f"Estado: {result.get('status', 'unknown')}")
        print(f"Task ID: {result.get('task_id', 'unknown')}")
        print(f"Worker ID: {result.get('worker_id', 'unknown')}")
        print(f"Procesado en: {result.get('processed_at', 'unknown')}")
        print(f"\nDatos procesados:")
        print(json.dumps(result.get('data', {}), indent=2, ensure_ascii=False))
    
    print("="*60 + "\n")


def interactive_mode(client: DistributedClient):
    """Modo interactivo para enviar tareas genericas"""
    print("\n" + "="*60)
    print("CLIENTE DISTRIBUIDO - MODO INTERACTIVO")
    print("="*60)
    print("\nEnvia tareas genericas al servidor")
    print("Formato: {'id': 'tarea_1', 'data': {'clave': 'valor'}}")
    print("="*60 + "\n")
    
    task_counter = 1
    
    while True:
        try:
            print(f"\n--- Tarea #{task_counter} ---")
            print("Opciones:")
            print("1. Enviar tarea con datos personalizados")
            print("2. Enviar tarea de ejemplo")
            print("3. Salir")
            
            choice = input("\nSelecciona una opción (1-3): ").strip()
            
            if choice == '1':
                print("\nIngresa los datos de la tarea (formato JSON):")
                print("Ejemplo: {\"mensaje\": \"Hola\", \"numero\": 42}")
                data_json = input("Datos: ").strip()
                data = json.loads(data_json)
                
                task = {
                    'id': f'task_{task_counter}',
                    'data': data
                }
                
                result = client.send_task(task)
                print_result(result)
                task_counter += 1
                
            elif choice == '2':
                task = {
                    'id': f'task_{task_counter}',
                    'data': {
                        'mensaje': 'Tarea de ejemplo',
                        'timestamp': time.time()
                    }
                }
                
                result = client.send_task(task)
                print_result(result)
                task_counter += 1
                
            elif choice == '3':
                print("\nHasta luego!")
                break
                
            else:
                print("[X] Opcion invalida. Intenta de nuevo.\n")
                
        except KeyboardInterrupt:
            print("\n\nHasta luego!")
            break
        except json.JSONDecodeError:
            print("\n[X] Error: Formato JSON invalido\n")
        except Exception as e:
            print(f"\n[X] Error: {e}\n")


def demo_mode(client: DistributedClient):
    """Modo demo que ejecuta varias tareas de ejemplo"""
    print("\n" + "="*60)
    print("MODO DEMO - EJECUTANDO TAREAS DE EJEMPLO")
    print("="*60 + "\n")
    
    # Ejemplo 1: Tarea simple
    print("Ejemplo 1: Tarea simple")
    task1 = {
        'id': 'demo_task_1',
        'data': {'mensaje': 'Primera tarea', 'prioridad': 'alta'}
    }
    result = client.send_task(task1)
    print_result(result)
    time.sleep(1)
    
    # Ejemplo 2: Tarea con numeros
    print("Ejemplo 2: Tarea con datos numericos")
    task2 = {
        'id': 'demo_task_2',
        'data': {'valores': [10, 20, 30], 'operacion': 'procesar'}
    }
    result = client.send_task(task2)
    print_result(result)
    time.sleep(1)
    
    # Ejemplo 3: Tarea con texto
    print("Ejemplo 3: Tarea con texto")
    task3 = {
        'id': 'demo_task_3',
        'data': {'texto': 'Sistema distribuido cliente-servidor', 'tipo': 'analisis'}
    }
    result = client.send_task(task3)
    print_result(result)
    time.sleep(1)
    
    # Ejemplo 4: Tarea con timestamp
    print("Ejemplo 4: Tarea con timestamp")
    task4 = {
        'id': 'demo_task_4',
        'data': {'timestamp': time.time(), 'evento': 'registro'}
    }
    result = client.send_task(task4)
    print_result(result)
    time.sleep(1)
    
    # Ejemplo 5: Tarea compleja
    print("Ejemplo 5: Tarea con estructura compleja")
    task5 = {
        'id': 'demo_task_5',
        'data': {
            'usuario': 'cliente_1',
            'accion': 'consulta',
            'parametros': {'filtro': 'activos', 'limite': 100}
        }
    }
    result = client.send_task(task5)
    print_result(result)
    
    print("\n Demo completado!\n")


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Cliente Distribuido')
    parser.add_argument('--host', default='localhost', help='Host del servidor')
    parser.add_argument('--port', type=int, default=5000, help='Puerto del servidor')
    parser.add_argument('--timeout', type=int, default=30, help='Timeout en segundos')
    parser.add_argument('--mode', choices=['interactive', 'demo'], default='interactive',
                        help='Modo de ejecución')
    
    args = parser.parse_args()
    
    client = DistributedClient(host=args.host, port=args.port, timeout=args.timeout)
    
    if args.mode == 'demo':
        demo_mode(client)
    else:
        interactive_mode(client)


if __name__ == '__main__':
    main()