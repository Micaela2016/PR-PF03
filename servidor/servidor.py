"""
Servidor Principal - Sistema Distribuido Cliente-Servidor
Recibe tareas por socket y las distribuye a workers mediante un pool de hilos
"""

import socket
import threading
import json
import logging
import queue
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaskWorker:
    """Worker que procesa tareas usando un pool de hilos"""
    
    def __init__(self, worker_id: int, num_threads: int = 4):
        self.worker_id = worker_id
        self.executor = ThreadPoolExecutor(max_workers=num_threads)
        self.tasks_processed = 0
        self.is_running = True
        logger.info(f"Worker {worker_id} inicializado con {num_threads} hilos")
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Procesa una tarea individual de forma genérica"""
        try:
            task_id = task.get('id', 'unknown')
            task_data = task.get('data', {})
            
            logger.info(f"Worker {self.worker_id} procesando tarea {task_id}")
            
            # Simula procesamiento de la tarea
            time.sleep(0.5)
            
            self.tasks_processed += 1
            
            return {
                'task_id': task_id,
                'worker_id': self.worker_id,
                'status': 'completed',
                'data': task_data,
                'processed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error procesando tarea en Worker {self.worker_id}: {e}")
            return {
                'task_id': task.get('id', 'unknown'),
                'worker_id': self.worker_id,
                'status': 'error',
                'error': str(e)
            }
    
    def submit_task(self, task: Dict[str, Any]):
        """Envía una tarea al pool de hilos del worker"""
        return self.executor.submit(self.process_task, task)
    
    def shutdown(self):
        """Detiene el worker"""
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info(f"Worker {self.worker_id} detenido. Tareas procesadas: {self.tasks_processed}")


class DistributedServer:
    """Servidor principal que distribuye tareas a workers"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 5000, num_workers: int = 3):
        self.host = host
        self.port = port
        self.num_workers = num_workers
        self.workers = []
        self.task_queue = queue.Queue()
        self.results = {}
        self.is_running = False
        self.server_socket = None
        self.current_worker_index = 0
        self.lock = threading.Lock()
        
        for i in range(num_workers):
            worker = TaskWorker(worker_id=i, num_threads=4)
            self.workers.append(worker)
        
        logger.info(f"Servidor inicializado con {num_workers} workers")
    
    def start(self):
        """Inicia el servidor"""
        self.is_running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        logger.info(f"Servidor escuchando en {self.host}:{self.port}")
        
        task_processor_thread = threading.Thread(target=self._process_task_queue, daemon=True)
        task_processor_thread.start()
        
        try:
            while self.is_running:
                try:
                    client_socket, address = self.server_socket.accept()
                    logger.info(f"Nueva conexión desde {address}")
                    
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                    
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.is_running:
                        logger.error(f"Error aceptando conexión: {e}")
        
        except KeyboardInterrupt:
            logger.info("Servidor interrumpido por el usuario")
        finally:
            self.stop()
    
    def _handle_client(self, client_socket: socket.socket, address: tuple):
        """Maneja la comunicación con un cliente"""
        try:
            data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(chunk) < 4096:
                    break
            
            if not data:
                logger.warning(f"No se recibieron datos de {address}")
                return
            
            task = json.loads(data.decode('utf-8'))
            task_id = task.get('id', f"task_{time.time()}")
            task['id'] = task_id
            
            logger.info(f"Tarea recibida de {address}: {task_id}")
            
            self.task_queue.put((task, client_socket, address))
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando JSON de {address}: {e}")
            self._send_error(client_socket, "Invalid JSON format")
        except Exception as e:
            logger.error(f"Error manejando cliente {address}: {e}")
            self._send_error(client_socket, str(e))
    
    def _process_task_queue(self):
        """Procesa tareas de la cola y las distribuye a workers"""
        while self.is_running:
            try:
                task, client_socket, address = self.task_queue.get(timeout=1)
                
                worker = self._get_next_worker()
                
                future = worker.submit_task(task)
                
                result_thread = threading.Thread(
                    target=self._send_result,
                    args=(future, client_socket, address),
                    daemon=True
                )
                result_thread.start()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error procesando cola de tareas: {e}")
    
    def _get_next_worker(self) -> TaskWorker:
        """Devuelve el siguiente worker disponible"""
        with self.lock:
            worker = self.workers[self.current_worker_index]
            self.current_worker_index = (self.current_worker_index + 1) % self.num_workers
            return worker
    
    def _send_result(self, future, client_socket: socket.socket, address: tuple):
        """Envía el resultado al cliente"""
        try:
            result = future.result(timeout=30)
            
            response = json.dumps(result).encode('utf-8')
            client_socket.sendall(response)
            
            logger.info(f"Resultado enviado a {address}")
            
        except Exception as e:
            logger.error(f"Error enviando resultado a {address}: {e}")
            self._send_error(client_socket, str(e))
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def _send_error(self, client_socket: socket.socket, error_message: str):
        """Envía un mensaje de error al cliente"""
        try:
            error_response = json.dumps({
                'status': 'error',
                'error': error_message
            }).encode('utf-8')
            client_socket.sendall(error_response)
        except:
            pass
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def stop(self):
        """Detiene el servidor"""
        logger.info("Deteniendo servidor...")
        self.is_running = False
        
        for worker in self.workers:
            worker.shutdown()
        
        if self.server_socket:
            self.server_socket.close()
        
        logger.info("Servidor detenido")
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del servidor"""
        return {
            'workers': len(self.workers),
            'tasks_in_queue': self.task_queue.qsize(),
            'worker_stats': [
                {
                    'worker_id': w.worker_id,
                    'tasks_processed': w.tasks_processed
                }
                for w in self.workers
            ]
        }


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Servidor Distribuido')
    parser.add_argument('--host', default='0.0.0.0', help='Host del servidor')
    parser.add_argument('--port', type=int, default=5000, help='Puerto del servidor')
    parser.add_argument('--workers', type=int, default=3, help='Número de workers')
    
    args = parser.parse_args()
    
    server = DistributedServer(host=args.host, port=args.port, num_workers=args.workers)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Servidor interrumpido")
    finally:
        server.stop()


if __name__ == '__main__':
    main()
